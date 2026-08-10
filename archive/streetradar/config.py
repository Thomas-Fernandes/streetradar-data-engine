"""Environment loading and typed configuration.

Configuration is read once, validated eagerly, and returned as a frozen
dataclass. Nothing here calls `sys.exit`: a library that kills the process
cannot be tested or reused, so misconfiguration raises `ConfigError` and the
caller decides what to do about it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

__all__ = ["ConfigError", "R2Config", "load_env", "r2_config", "env_str", "env_int"]


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or unusable."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def load_env() -> Path | None:
    """Load the nearest `.env` file, walking up from the repo root.

    Cached: repeated calls from different modules are free, and a second call
    will not clobber variables that were set programmatically in between.

    Returns the file that was loaded, or None when falling back to whatever
    `python-dotenv` finds on its own.
    """
    for candidate in (_repo_root() / ".env", Path.cwd() / ".env"):
        if candidate.is_file():
            load_dotenv(dotenv_path=candidate)
            return candidate

    load_dotenv()
    return None


def env_str(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    """Read a string variable, treating blank as absent."""
    value = os.getenv(name)
    if value is not None and value.strip() == "":
        value = None
    if value is None:
        value = default
    if required and value is None:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def env_int(name: str, default: int) -> int:
    """Read an integer variable, falling back on anything unparseable."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class R2Config:
    """Everything needed to talk to the Cloudflare R2 bucket."""

    access_key_id: str
    secret_access_key: str
    account_id: str
    bucket: str
    endpoint: str
    public_domain: str

    @property
    def public_base_url(self) -> str:
        return f"https://{self.public_domain}"


def r2_config() -> R2Config:
    """Build the R2 configuration from the environment.

    Reports *every* missing variable at once. The previous per-provider copies
    failed on the first one, so fixing a fresh setup took several runs.
    """
    load_env()

    access_key_id = env_str("R2_ACCESS_KEY_ID")
    secret_access_key = env_str("R2_SECRET_ACCESS_KEY")
    account_id = env_str("R2_ACCOUNT_ID")
    bucket = env_str("R2_BUCKET")
    endpoint = env_str("R2_ENDPOINT")

    # The account id can be recovered from a custom endpoint, which is how the
    # older scripts were sometimes configured.
    if not account_id and endpoint:
        host = urlparse(endpoint).hostname
        if host:
            account_id = host.split(".")[0]

    missing = [
        name
        for name, value in (
            ("R2_ACCESS_KEY_ID", access_key_id),
            ("R2_SECRET_ACCESS_KEY", secret_access_key),
            ("R2_ACCOUNT_ID", account_id),
            ("R2_BUCKET", bucket),
        )
        if not value
    ]
    if missing:
        raise ConfigError("Missing required environment variables: " + ", ".join(missing))

    assert access_key_id and secret_access_key and account_id and bucket  # for type narrowing

    return R2Config(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        account_id=account_id,
        bucket=bucket,
        endpoint=endpoint or f"https://{account_id}.r2.cloudflarestorage.com",
        # The old default pointed at streetradar.com. The bucket is served from
        # tiles.streetradar.app.
        public_domain=env_str("R2_PUBLIC_DOMAIN", "tiles.streetradar.app") or "",
    )
