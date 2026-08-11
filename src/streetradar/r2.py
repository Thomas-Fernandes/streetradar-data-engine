"""Cloudflare R2 object storage: the one place that talks to the bucket.

Everything the engine publishes — the versioned PMTiles archives, the
`latest.json` pointer, the `status.json` dashboard document, the packed raw
`.tar.zst` — lands in one R2 bucket, and every call that puts or reads an object
there goes through this module. It is deliberately small: the object *keys* and
the publish rules (versioning, the never-overwrite-a-live-object gate, retention)
are not here — they belong to the publish stage that composes these primitives.
This module only knows how to move bytes to and from a key.

## Not the fetch path, and not gated by it

`streetradar.fetch` is the single outbound path for *provider* traffic — the
place that carries `robots.txt`, per-host budgets, retries and scraping metrics
(design invariant 1). R2 is our own infrastructure, not a provider, so it is a
separate outbound path by design: talking to the bucket is not scraping, carries
no politeness budget, and is **not** gated by `ENGINE_ALLOW_OUTBOUND`. Reads back
are tiny and rare — the small JSON pointer, a size check after an upload — so the
Class B request budget that ADR 0005 watches is never spent here in bulk.

## Injectable client, so it is testable offline

The boto3 S3 client is created lazily and can be supplied instead, so the whole
surface is exercised against an in-memory fake with no credentials and no
network. Configuration is read from `ENGINE_R2_*` (the engine's env convention,
as with `ENGINE_ALLOW_OUTBOUND` and `ENGINE_APPLE_TOKEN`) and every missing
variable is reported at once, because fixing a fresh deploy one variable per run
is exactly the papercut the previous engine had.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlparse

from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

__all__ = ["R2Client", "R2Config", "R2Error", "S3Like", "StoredObject", "r2_config"]

#: Multipart part size. R2 rejects chunks below 5 MB; 64 MB keeps the number of
#: Class A requests per multi-GB upload small, which is the only R2 line item
#: ADR 0005 flags as able to surprise.
_MULTIPART_PART_SIZE = 64 * 1024 * 1024

#: Env variables carry the engine's own prefix, so R2 configuration is a visible,
#: namespaced decision rather than ambient AWS credentials picked up by accident.
_PREFIX = "ENGINE_R2_"


class R2Error(RuntimeError):
    """An R2 operation could not be completed or verified."""


@dataclass(frozen=True)
class R2Config:
    """Everything needed to talk to the bucket."""

    access_key_id: str
    secret_access_key: str
    account_id: str
    bucket: str
    endpoint: str
    public_domain: str

    @property
    def public_base_url(self) -> str:
        return f"https://{self.public_domain}"


@dataclass(frozen=True)
class StoredObject:
    """A stored object as the bucket reports it: its key, byte size and ETag."""

    key: str
    size: int
    etag: str


class S3Like(Protocol):
    """The slice of the boto3 S3 client this module uses.

    Typing against this, rather than boto3's untyped client, is what lets a fake
    stand in for the bucket in tests. The signatures are boto3's, so the real
    client satisfies the protocol structurally.
    """

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def delete_object(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def upload_file(self, **kwargs: Any) -> None: ...


def _env(name: str) -> str | None:
    """Read an environment variable, treating blank as absent."""
    value = os.getenv(name)
    return value if value and value.strip() else None


def r2_config() -> R2Config:
    """Build the R2 configuration from the environment, reporting all gaps at once.

    The account id may be omitted when a full custom endpoint is given, since it
    is the first label of the endpoint host; conversely the endpoint defaults to
    the account's `r2.cloudflarestorage.com` host when only the id is set.
    """
    access_key_id = _env(_PREFIX + "ACCESS_KEY_ID")
    secret_access_key = _env(_PREFIX + "SECRET_ACCESS_KEY")
    account_id = _env(_PREFIX + "ACCOUNT_ID")
    bucket = _env(_PREFIX + "BUCKET")
    endpoint = _env(_PREFIX + "ENDPOINT")

    if not account_id and endpoint:
        host = urlparse(endpoint).hostname
        if host:
            account_id = host.split(".")[0]

    missing = [
        _PREFIX + suffix
        for suffix, value in (
            ("ACCESS_KEY_ID", access_key_id),
            ("SECRET_ACCESS_KEY", secret_access_key),
            ("ACCOUNT_ID", account_id),
            ("BUCKET", bucket),
        )
        if not value
    ]
    if missing:
        raise R2Error("missing required environment variables: " + ", ".join(missing))
    assert access_key_id and secret_access_key and account_id and bucket  # narrows for the checker

    return R2Config(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        account_id=account_id,
        bucket=bucket,
        endpoint=endpoint or f"https://{account_id}.r2.cloudflarestorage.com",
        public_domain=_env(_PREFIX + "PUBLIC_DOMAIN") or "tiles.streetradar.app",
    )


def file_md5(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """MD5 of a file, read in chunks so a multi-GB archive never lands in memory."""
    digest = hashlib.md5()  # a content fingerprint for the upload check, not a security control
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _make_client(config: R2Config) -> S3Like:
    import boto3

    client: S3Like = boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        region_name="auto",
    )
    return client


def _transfer_config() -> Any:
    from boto3.s3.transfer import TransferConfig

    return TransferConfig(
        multipart_threshold=_MULTIPART_PART_SIZE,
        multipart_chunksize=_MULTIPART_PART_SIZE,
        max_concurrency=10,
        use_threads=True,
    )


def _clean_etag(raw: object) -> str:
    """S3 wraps the ETag in quotes; strip them for a bare hex string."""
    return str(raw).strip('"') if raw else ""


def _is_not_found(exc: ClientError) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = response.get("Error", {}).get("Code")
    if code in {"404", "NoSuchKey", "NotFound"}:
        return True
    return bool(response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404)


class R2Client:
    """Move bytes to and from keys in the bucket. Nothing about what a key means.

    Synchronous on purpose: uploads happen at shard boundaries, not in the async
    fetch hot path, so a caller inside an event loop offloads with
    `asyncio.to_thread`. Keeping the client plain is what makes it testable
    against an injected fake.
    """

    def __init__(self, config: R2Config | None = None, *, client: S3Like | None = None) -> None:
        self.config = config or r2_config()
        self._s3: S3Like = client if client is not None else _make_client(self.config)

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        metadata: Mapping[str, str] | None = None,
    ) -> StoredObject:
        """Store an in-memory body — the small JSON pointer and status documents."""
        try:
            response = self._s3.put_object(
                Bucket=self.config.bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
                Metadata=dict(metadata or {}),
            )
        except ClientError as exc:
            raise R2Error(f"writing {key} failed: {exc}") from exc
        return StoredObject(key=key, size=len(data), etag=_clean_etag(response.get("ETag")))

    def upload(
        self,
        path: Path,
        key: str,
        *,
        content_type: str = "application/octet-stream",
        metadata: Mapping[str, str] | None = None,
        verify: bool = True,
    ) -> StoredObject:
        """Upload a file, multipart above the part size, and confirm its size back.

        The size check is a HEAD after the fact: a truncated upload that reports
        the wrong length must not be mistaken for a published archive. Raises
        `R2Error` if the file is missing or the stored size does not match.
        """
        if not path.is_file():
            raise R2Error(f"nothing to upload at {path}")

        size = path.stat().st_size
        digest = file_md5(path)
        extra_args = {
            "ContentType": content_type,
            "Metadata": {"md5": digest, "size": str(size), **dict(metadata or {})},
        }
        try:
            self._s3.upload_file(
                Filename=str(path),
                Bucket=self.config.bucket,
                Key=key,
                ExtraArgs=extra_args,
                Config=_transfer_config(),
            )
        except (ClientError, OSError) as exc:
            raise R2Error(f"upload of {path.name} to {key} failed: {exc}") from exc

        if verify:
            stored = self.head(key)
            if stored is None or stored.size != size:
                reported = "absent" if stored is None else f"{stored.size} bytes"
                raise R2Error(f"uploaded {key} but it reports {reported}, not {size} bytes")

        return StoredObject(key=key, size=size, etag=digest)

    def get_bytes(self, key: str, *, missing_ok: bool = False) -> bytes | None:
        """Read a key's body. Returns None for an absent key when `missing_ok`.

        `missing_ok` mirrors the fetch layer's convention, so reading a
        `latest.json` that does not exist yet on a first run is a None, not a
        control-flow exception.
        """
        try:
            response = self._s3.get_object(Bucket=self.config.bucket, Key=key)
        except ClientError as exc:
            if _is_not_found(exc):
                if missing_ok:
                    return None
                raise R2Error(f"no object at {key}") from exc
            raise R2Error(f"reading {key} failed: {exc}") from exc
        data = response["Body"].read()
        return data if isinstance(data, bytes) else bytes(data)

    def head(self, key: str) -> StoredObject | None:
        """The object's size and ETag, or None if it is not there."""
        try:
            response = self._s3.head_object(Bucket=self.config.bucket, Key=key)
        except ClientError as exc:
            if _is_not_found(exc):
                return None
            raise R2Error(f"HEAD {key} failed: {exc}") from exc
        return StoredObject(
            key=key,
            size=int(response.get("ContentLength", 0)),
            etag=_clean_etag(response.get("ETag")),
        )

    def exists(self, key: str) -> bool:
        """Whether a live object sits at this key — the never-overwrite guard's test."""
        return self.head(key) is not None

    def delete(self, key: str) -> None:
        """Remove a key. Used by retention to prune superseded versions."""
        try:
            self._s3.delete_object(Bucket=self.config.bucket, Key=key)
        except ClientError as exc:
            raise R2Error(f"deleting {key} failed: {exc}") from exc

    def list_objects(self, prefix: str) -> list[StoredObject]:
        """Every object under a prefix, following pagination to the end.

        Retention lists a provider's published versions to decide which to prune,
        so a truncated first page would silently keep the old versions alive.
        """
        objects: list[StoredObject] = []
        token: str | None = None
        while True:
            params: dict[str, Any] = {"Bucket": self.config.bucket, "Prefix": prefix}
            if token:
                params["ContinuationToken"] = token
            try:
                response = self._s3.list_objects_v2(**params)
            except ClientError as exc:
                raise R2Error(f"listing {prefix} failed: {exc}") from exc
            for item in response.get("Contents") or ():
                objects.append(
                    StoredObject(
                        key=item["Key"],
                        size=int(item.get("Size", 0)),
                        etag=_clean_etag(item.get("ETag")),
                    )
                )
            if not response.get("IsTruncated"):
                return objects
            token = response.get("NextContinuationToken")
            if not token:
                return objects
