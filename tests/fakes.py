"""Shared test doubles. Not a test module — imported by the ones that need it.

The in-memory bucket lives here because more than one test module drives the R2
client against it: the client's own tests, the publish path, and the run loop.
Keeping one faithful fake — one that raises the same `ClientError` a missing key
produces on the wire — is what keeps those tests exercising real code paths
rather than three subtly different mocks.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from streetradar.r2 import R2Client, R2Config

CONFIG = R2Config(
    access_key_id="k",
    secret_access_key="s",
    account_id="acct",
    bucket="streetradar",
    endpoint="https://acct.r2.cloudflarestorage.com",
    public_domain="tiles.streetradar.app",
)


def not_found(operation: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        operation,
    )


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3:
    """A dict-backed bucket that pages small, so the list loop is exercised."""

    def __init__(self, *, page_size: int = 2) -> None:
        self.store: dict[str, dict[str, Any]] = {}
        self.page_size = page_size
        self._tokens: dict[str, int] = {}
        self._next = 0

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        data = kwargs["Body"]
        etag = f'"{hashlib.md5(data).hexdigest()}"'
        self.store[kwargs["Key"]] = {
            "data": data,
            "ContentType": kwargs.get("ContentType"),
            "Metadata": dict(kwargs.get("Metadata") or {}),
            "ETag": etag,
        }
        return {"ETag": etag}

    def upload_file(self, **kwargs: Any) -> None:
        data = Path(kwargs["Filename"]).read_bytes()
        extra = kwargs.get("ExtraArgs") or {}
        self.store[kwargs["Key"]] = {
            "data": data,
            "ContentType": extra.get("ContentType"),
            "Metadata": dict(extra.get("Metadata") or {}),
            "ETag": f'"{hashlib.md5(data).hexdigest()}"',
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        record = self.store.get(kwargs["Key"])
        if record is None:
            raise not_found("GetObject")
        return {"Body": _Body(record["data"]), "ContentLength": len(record["data"])}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        record = self.store.get(kwargs["Key"])
        if record is None:
            raise not_found("HeadObject")
        return {"ContentLength": len(record["data"]), "ETag": record["ETag"]}

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        self.store.pop(kwargs["Key"], None)
        return {}

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        keys = sorted(k for k in self.store if k.startswith(kwargs.get("Prefix", "")))
        start = self._tokens.get(kwargs.get("ContinuationToken", ""), 0)
        page = keys[start : start + self.page_size]
        contents = [
            {"Key": k, "Size": len(self.store[k]["data"]), "ETag": self.store[k]["ETag"]}
            for k in page
        ]
        truncated = start + self.page_size < len(keys)
        result: dict[str, Any] = {"Contents": contents, "IsTruncated": truncated}
        if truncated:
            self._next += 1
            token = f"t{self._next}"
            self._tokens[token] = start + self.page_size
            result["NextContinuationToken"] = token
        return result


def make_client() -> tuple[R2Client, FakeS3]:
    """A client wired to a fresh in-memory bucket, and the bucket to inspect."""
    fake = FakeS3()
    return R2Client(CONFIG, client=fake), fake
