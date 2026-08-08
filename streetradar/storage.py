"""Cloudflare R2 uploads.

Replaces the copy of this logic that each provider carried — 125 of the ~220
lines were byte-identical between `ja` and `naver`, and the two had already
drifted apart in small ways.

Progress goes through the logger rather than raw `sys.stdout.write("\\r...")`.
Carriage-return progress bars are unreadable in a cron log or a container's
captured output, which is where this actually runs.
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

from .config import R2Config, env_int, r2_config
from .logging import get_logger

__all__ = ["R2Storage", "UploadResult", "StorageError", "file_md5"]

logger = get_logger(__name__)


class StorageError(RuntimeError):
    """Raised when an upload cannot be completed or verified."""


@dataclass(frozen=True)
class UploadResult:
    key: str
    bucket: str
    size_bytes: int
    md5: str
    duration_seconds: float
    verified: bool

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)

    @property
    def average_mbps(self) -> float:
        return self.size_mb / self.duration_seconds if self.duration_seconds > 0 else 0.0


def file_md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """MD5 of a file, read in chunks so multi-GB PMTiles do not land in memory."""
    digest = hashlib.md5()  # noqa: S324 — content fingerprint, not a security control
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _ProgressLogger:
    """boto3 upload callback that logs at most once per interval."""

    def __init__(self, total_bytes: int, label: str, interval: float = 15.0) -> None:
        self._total = float(total_bytes)
        self._label = label
        self._interval = interval
        self._seen = 0
        self._started = time.monotonic()
        self._last_log = 0.0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount: int) -> None:
        with self._lock:
            self._seen += bytes_amount
            now = time.monotonic()
            done = self._seen >= self._total

            if not done and now - self._last_log < self._interval:
                return
            self._last_log = now

            elapsed = now - self._started
            pct = (self._seen / self._total * 100) if self._total else 100.0
            speed = (self._seen / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
            eta = (
                (self._total - self._seen) / (self._seen / elapsed) if self._seen and elapsed else 0
            )

            logger.info(
                "%s: %.0f%% (%.1f/%.1f MB) at %.1f MB/s, eta %.0fs",
                self._label,
                pct,
                self._seen / (1024 * 1024),
                self._total / (1024 * 1024),
                speed,
                eta,
            )


class R2Storage:
    """Thin wrapper over the S3 API as exposed by Cloudflare R2."""

    def __init__(self, config: R2Config | None = None) -> None:
        self.config = config or r2_config()
        self._client = boto3.client(
            "s3",
            endpoint_url=self.config.endpoint,
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            region_name="auto",
        )

    def upload(
        self,
        path: Path,
        key: str,
        *,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
        part_size_mb: int | None = None,
        verify: bool = True,
    ) -> UploadResult:
        """Upload a file and, by default, confirm its size server-side.

        Raises StorageError if the file is missing or verification fails.
        """
        if not path.is_file():
            raise StorageError(f"File not found: {path}")

        size = path.stat().st_size
        # R2 rejects multipart chunks below 5 MB.
        part_size = max(part_size_mb or env_int("PART_SIZE_MB", 64), 5) * 1024 * 1024

        logger.info("hashing %s (%.1f MB)", path.name, size / (1024 * 1024))
        digest = file_md5(path)

        extra_args = {
            "ContentType": content_type,
            "Metadata": {
                "original-filename": path.name,
                "file-hash": digest,
                "upload-time": datetime.now(UTC).isoformat(),
                "file-size": str(size),
                **(metadata or {}),
            },
        }

        logger.info("uploading to s3://%s/%s", self.config.bucket, key)
        started = time.monotonic()
        try:
            self._client.upload_file(
                Filename=str(path),
                Bucket=self.config.bucket,
                Key=key,
                Config=TransferConfig(
                    multipart_threshold=part_size,
                    multipart_chunksize=part_size,
                    max_concurrency=10,
                    use_threads=True,
                ),
                Callback=_ProgressLogger(size, path.name),
                ExtraArgs=extra_args,
            )
        except Exception as exc:  # boto3 raises a wide range of client errors
            raise StorageError(f"Upload of {path.name} failed: {exc}") from exc

        duration = time.monotonic() - started
        verified = self.verify(key, size) if verify else False

        if verify and not verified:
            raise StorageError(f"Uploaded {key} but the object does not report {size} bytes back")

        logger.info(
            "uploaded %s in %.1fs (%.1f MB/s) -> %s/%s",
            key,
            duration,
            (size / (1024 * 1024)) / duration if duration else 0.0,
            self.config.public_base_url,
            key,
        )

        return UploadResult(
            key=key,
            bucket=self.config.bucket,
            size_bytes=size,
            md5=digest,
            duration_seconds=duration,
            verified=verified,
        )

    def verify(self, key: str, expected_size: int) -> bool:
        """Check that the stored object reports the size we uploaded."""
        try:
            head = self._client.head_object(Bucket=self.config.bucket, Key=key)
        except Exception as exc:
            logger.warning("could not HEAD %s: %s", key, exc)
            return False
        return bool(head.get("ContentLength") == expected_size)
