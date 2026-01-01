from pathlib import Path

import pytest

from ashare.core.config import CONFIG_FILE_ENV, ProxyConfig, get_section, load_config


def _write_config(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def test_load_config_returns_empty_when_missing(monkeypatch, tmp_path):
    load_config.cache_clear()
    missing = tmp_path / "missing.yaml"
    monkeypatch.setenv(CONFIG_FILE_ENV, str(missing))
    assert load_config() == {}
    load_config.cache_clear()


def test_load_config_from_env_path(monkeypatch, tmp_path):
    path = _write_config(tmp_path, "database:\n  host: 127.0.0.1\n")
    monkeypatch.setenv(CONFIG_FILE_ENV, str(path))
    load_config.cache_clear()
    data = load_config()
    assert data["database"]["host"] == "127.0.0.1"
    load_config.cache_clear()


def test_get_section_raises_on_invalid_type(monkeypatch, tmp_path):
    path = _write_config(tmp_path, "proxy: bad\n")
    monkeypatch.setenv(CONFIG_FILE_ENV, str(path))
    load_config.cache_clear()
    with pytest.raises(ValueError):
        get_section("proxy")
    load_config.cache_clear()


def test_proxyconfig_from_env_and_config(monkeypatch, tmp_path):
    path = _write_config(
        tmp_path,
        "proxy:\n  http: http://cfg\n  https: http://cfgs\n",
    )
    monkeypatch.setenv(CONFIG_FILE_ENV, str(path))
    monkeypatch.setenv("ASHARE_HTTP_PROXY", "http://env")
    monkeypatch.delenv("ASHARE_HTTPS_PROXY", raising=False)
    load_config.cache_clear()
    proxy = ProxyConfig.from_env()
    assert proxy.http == "http://env"
    assert proxy.https == "http://cfgs"
    load_config.cache_clear()
