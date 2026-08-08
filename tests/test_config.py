import pytest

from streetradar.config import ConfigError, env_int, env_str, r2_config

R2_VARS = (
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_ACCOUNT_ID",
    "R2_BUCKET",
    "R2_ENDPOINT",
    "R2_PUBLIC_DOMAIN",
)


@pytest.fixture
def clean_env(monkeypatch):
    """Start from an environment with no R2 configuration at all."""
    for name in R2_VARS:
        monkeypatch.delenv(name, raising=False)
    # load_env() is cached; pretend it already ran so tests never read a real .env
    from streetradar import config

    config.load_env.cache_clear()
    monkeypatch.setattr(config, "load_env", lambda: None)
    return monkeypatch


def test_env_str_treats_blank_as_absent(monkeypatch):
    monkeypatch.setenv("SR_TEST", "   ")
    assert env_str("SR_TEST", "fallback") == "fallback"


def test_env_str_required_raises_with_the_variable_name(monkeypatch):
    monkeypatch.delenv("SR_MISSING", raising=False)
    with pytest.raises(ConfigError, match="SR_MISSING"):
        env_str("SR_MISSING", required=True)


def test_env_int_falls_back_when_unset(monkeypatch):
    monkeypatch.delenv("SR_N", raising=False)
    assert env_int("SR_N", 7) == 7


def test_env_int_rejects_garbage(monkeypatch):
    monkeypatch.setenv("SR_N", "not-a-number")
    with pytest.raises(ConfigError, match="SR_N"):
        env_int("SR_N", 7)


def test_r2_config_reports_every_missing_variable_at_once(clean_env):
    """The old per-provider code failed on the first missing name, so setting
    up a fresh environment took one run per variable."""
    clean_env.setenv("R2_ACCESS_KEY_ID", "key")

    with pytest.raises(ConfigError) as exc:
        r2_config()

    message = str(exc.value)
    assert "R2_SECRET_ACCESS_KEY" in message
    assert "R2_ACCOUNT_ID" in message
    assert "R2_BUCKET" in message
    assert "R2_ACCESS_KEY_ID" not in message


def test_r2_config_derives_account_id_from_endpoint(clean_env):
    clean_env.setenv("R2_ACCESS_KEY_ID", "key")
    clean_env.setenv("R2_SECRET_ACCESS_KEY", "secret")
    clean_env.setenv("R2_BUCKET", "bucket")
    clean_env.setenv("R2_ENDPOINT", "https://abc123.r2.cloudflarestorage.com")

    config = r2_config()

    assert config.account_id == "abc123"


def test_r2_config_builds_default_endpoint(clean_env):
    for name, value in (
        ("R2_ACCESS_KEY_ID", "key"),
        ("R2_SECRET_ACCESS_KEY", "secret"),
        ("R2_ACCOUNT_ID", "acct"),
        ("R2_BUCKET", "bucket"),
    ):
        clean_env.setenv(name, value)

    config = r2_config()

    assert config.endpoint == "https://acct.r2.cloudflarestorage.com"


def test_public_domain_defaults_to_the_app_tld(clean_env):
    """The old scripts defaulted to streetradar.com, which is not the domain
    the bucket is served from."""
    for name, value in (
        ("R2_ACCESS_KEY_ID", "key"),
        ("R2_SECRET_ACCESS_KEY", "secret"),
        ("R2_ACCOUNT_ID", "acct"),
        ("R2_BUCKET", "bucket"),
    ):
        clean_env.setenv(name, value)

    config = r2_config()

    assert config.public_domain == "tiles.streetradar.app"
    assert config.public_base_url == "https://tiles.streetradar.app"
