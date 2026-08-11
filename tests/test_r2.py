"""The R2 client, exercised against an in-memory bucket.

No network and no credentials: a fake S3 client backed by a dict stands in for
the bucket, raising the same `ClientError` a missing key produces on the wire, so
the not-found paths (`missing_ok`, `head`, `exists`) are real rather than mocked
away. The one thing this cannot cover is that boto3's real client satisfies
`S3Like` — that is the type checker's job, and `_make_client` is annotated to it.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from streetradar.r2 import R2Client, R2Config, R2Error, StoredObject, r2_config

CONFIG = R2Config(
    access_key_id="k",
    secret_access_key="s",
    account_id="acct",
    bucket="streetradar",
    endpoint="https://acct.r2.cloudflarestorage.com",
    public_domain="tiles.streetradar.app",
)


def _not_found(operation: str) -> ClientError:
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
        etag = hashlib.md5(data).hexdigest()  # noqa: F841 — kept for parity with a real ETag
        self.store[kwargs["Key"]] = {
            "data": data,
            "ContentType": kwargs.get("ContentType"),
            "Metadata": dict(kwargs.get("Metadata") or {}),
            "ETag": f'"{etag}"',
        }
        return {"ETag": f'"{etag}"'}

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
            raise _not_found("GetObject")
        return {"Body": _Body(record["data"]), "ContentLength": len(record["data"])}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        record = self.store.get(kwargs["Key"])
        if record is None:
            raise _not_found("HeadObject")
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


def client() -> tuple[R2Client, FakeS3]:
    fake = FakeS3()
    return R2Client(CONFIG, client=fake), fake


class TestConfig:
    def test_it_reads_the_engine_prefixed_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for suffix, value in (
            ("ACCESS_KEY_ID", "k"),
            ("SECRET_ACCESS_KEY", "s"),
            ("ACCOUNT_ID", "acct"),
            ("BUCKET", "b"),
        ):
            monkeypatch.setenv("ENGINE_R2_" + suffix, value)
        monkeypatch.delenv("ENGINE_R2_ENDPOINT", raising=False)
        config = r2_config()
        assert config.endpoint == "https://acct.r2.cloudflarestorage.com"
        assert config.public_base_url == "https://tiles.streetradar.app"

    def test_the_account_id_is_recovered_from_a_custom_endpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENGINE_R2_ACCESS_KEY_ID", "k")
        monkeypatch.setenv("ENGINE_R2_SECRET_ACCESS_KEY", "s")
        monkeypatch.setenv("ENGINE_R2_BUCKET", "b")
        monkeypatch.setenv("ENGINE_R2_ENDPOINT", "https://myacct.r2.cloudflarestorage.com")
        monkeypatch.delenv("ENGINE_R2_ACCOUNT_ID", raising=False)
        assert r2_config().account_id == "myacct"

    def test_every_missing_variable_is_named_at_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for suffix in ("ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "ACCOUNT_ID", "BUCKET", "ENDPOINT"):
            monkeypatch.delenv("ENGINE_R2_" + suffix, raising=False)
        with pytest.raises(R2Error) as caught:
            r2_config()
        message = str(caught.value)
        assert "ACCESS_KEY_ID" in message and "SECRET_ACCESS_KEY" in message
        assert "ACCOUNT_ID" in message and "BUCKET" in message

    def test_a_blank_variable_counts_as_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENGINE_R2_ACCESS_KEY_ID", "   ")
        monkeypatch.setenv("ENGINE_R2_SECRET_ACCESS_KEY", "s")
        monkeypatch.setenv("ENGINE_R2_ACCOUNT_ID", "acct")
        monkeypatch.setenv("ENGINE_R2_BUCKET", "b")
        with pytest.raises(R2Error, match="ACCESS_KEY_ID"):
            r2_config()


class TestBytes:
    def test_put_then_get_round_trips(self) -> None:
        r2, _ = client()
        stored = r2.put_bytes("latest.json", b'{"v":1}', content_type="application/json")
        assert isinstance(stored, StoredObject)
        assert stored.size == len(b'{"v":1}')
        assert r2.get_bytes("latest.json") == b'{"v":1}'

    def test_content_type_and_metadata_reach_the_bucket(self) -> None:
        r2, fake = client()
        r2.put_bytes("status.json", b"{}", content_type="application/json", metadata={"run": "42"})
        assert fake.store["status.json"]["ContentType"] == "application/json"
        assert fake.store["status.json"]["Metadata"]["run"] == "42"

    def test_a_missing_key_is_none_when_allowed(self) -> None:
        r2, _ = client()
        assert r2.get_bytes("absent.json", missing_ok=True) is None

    def test_a_missing_key_raises_when_not_allowed(self) -> None:
        r2, _ = client()
        with pytest.raises(R2Error, match="no object at absent.json"):
            r2.get_bytes("absent.json")


class TestUpload:
    def test_a_file_uploads_and_verifies_its_size(self, tmp_path: Path) -> None:
        blob = tmp_path / "ja.pmtiles"
        blob.write_bytes(b"x" * 4096)
        r2, fake = client()
        stored = r2.upload(blob, "tiles/ja/v1.pmtiles", content_type="application/octet-stream")
        assert stored.size == 4096
        assert stored.etag == hashlib.md5(b"x" * 4096).hexdigest()
        assert fake.store["tiles/ja/v1.pmtiles"]["Metadata"]["md5"] == stored.etag

    def test_a_missing_file_is_refused(self, tmp_path: Path) -> None:
        r2, _ = client()
        with pytest.raises(R2Error, match="nothing to upload"):
            r2.upload(tmp_path / "nope.pmtiles", "tiles/ja/v1.pmtiles")

    def test_a_size_mismatch_after_upload_is_caught(self, tmp_path: Path) -> None:
        blob = tmp_path / "ja.pmtiles"
        blob.write_bytes(b"x" * 4096)

        class LyingS3(FakeS3):
            def head_object(self, **kwargs: Any) -> dict[str, Any]:
                head = super().head_object(**kwargs)
                return {**head, "ContentLength": head["ContentLength"] - 1}

        r2 = R2Client(CONFIG, client=LyingS3())
        with pytest.raises(R2Error, match="not 4096 bytes"):
            r2.upload(blob, "tiles/ja/v1.pmtiles")

    def test_verification_can_be_skipped(self, tmp_path: Path) -> None:
        blob = tmp_path / "ja.pmtiles"
        blob.write_bytes(b"x" * 10)

        class LyingS3(FakeS3):
            def head_object(self, **kwargs: Any) -> dict[str, Any]:
                raise AssertionError("head must not be called when verify is off")

        stored = R2Client(CONFIG, client=LyingS3()).upload(blob, "k", verify=False)
        assert stored.size == 10


class TestHeadAndDelete:
    def test_head_reports_a_present_object(self) -> None:
        r2, _ = client()
        r2.put_bytes("a", b"12345")
        head = r2.head("a")
        assert head is not None and head.size == 5
        assert r2.exists("a")

    def test_head_is_none_for_an_absent_object(self) -> None:
        r2, _ = client()
        assert r2.head("gone") is None
        assert not r2.exists("gone")

    def test_delete_removes_the_object(self) -> None:
        r2, _ = client()
        r2.put_bytes("doomed", b"x")
        r2.delete("doomed")
        assert not r2.exists("doomed")


class TestList:
    def test_it_returns_every_object_under_a_prefix_across_pages(self) -> None:
        r2, _ = client()  # FakeS3 pages two at a time
        for i in range(5):
            r2.put_bytes(f"tiles/ja/v{i}.pmtiles", b"x" * (i + 1))
        r2.put_bytes("tiles/naver/v0.pmtiles", b"y")
        found = r2.list_objects("tiles/ja/")
        assert sorted(o.key for o in found) == [f"tiles/ja/v{i}.pmtiles" for i in range(5)]
        assert {o.size for o in found} == {1, 2, 3, 4, 5}


def test_unexpected_client_errors_are_not_swallowed_as_not_found() -> None:
    class BrokenS3(FakeS3):
        def head_object(self, **kwargs: Any) -> dict[str, Any]:
            raise ClientError(
                {"Error": {"Code": "AccessDenied"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
                "HeadObject",
            )

    r2 = R2Client(CONFIG, client=BrokenS3())
    with pytest.raises(R2Error, match="HEAD"):
        r2.head("whatever")
