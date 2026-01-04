"测试 config 模块的功能."
import os
import pytest
from pathlib import Path
from ashare.core.config import ProxyConfig, load_config, CONFIG_FILE_ENV, get_section

@pytest.fixture(autouse=True)
def clean_proxy_env(monkeypatch):
    """每次测试前清理代理相关的环境变量."""
    # 清理常用的代理变量
    for key in ['ASHARE_HTTP_PROXY', 'ASHARE_HTTPS_PROXY', 
                'HTTP_PROXY', 'HTTPS_PROXY', 
                'http_proxy', 'https_proxy']:
        monkeypatch.delenv(key, raising=False)
    
    # 关键修复：设置一个不存在的配置文件路径，而不是删除变量。
    # 这样 load_config 会尝试读取该文件，失败后返回 {}，而不是回退读取项目根目录的 config.yaml
    monkeypatch.setenv(CONFIG_FILE_ENV, "non_existent_dummy_config.yaml")
    
    # 清除配置加载缓存
    load_config.cache_clear()

class TestProxyConfig:
    """测试 ProxyConfig 类."""
    
    def test_initialization_default(self):
        """测试默认初始化."""
        config = ProxyConfig()
        assert config.http is None
        assert config.https is None
    
    def test_initialization_with_values(self):
        """测试使用自定义值初始化."""
        config = ProxyConfig(http="http://p:80", https="https://p:80")
        assert config.http == "http://p:80"
        assert config.https == "https://p:80"
    
    def test_from_env_empty(self):
        """测试从空环境变量加载配置."""
        # 由于 clean_proxy_env 设置了假的 config 路径，这里应该读不到任何配置
        config = ProxyConfig.from_env()
        assert config.http is None
        assert config.https is None
    
    def test_from_env_ashare_vars(self, monkeypatch):
        """测试从ASHARE_*环境变量加载配置."""
        monkeypatch.setenv('ASHARE_HTTP_PROXY', 'http://ashare:80')
        monkeypatch.setenv('ASHARE_HTTPS_PROXY', 'https://ashare:80')
        config = ProxyConfig.from_env()
        assert config.http == 'http://ashare:80'
        assert config.https == 'https://ashare:80'
    
    def test_from_env_standard_vars(self, monkeypatch):
        """测试从标准环境变量加载配置."""
        monkeypatch.setenv('HTTP_PROXY', 'http://std:80')
        monkeypatch.setenv('HTTPS_PROXY', 'https://std:80')
        config = ProxyConfig.from_env()
        assert config.http == 'http://std:80'
        assert config.https == 'https://std:80'
    
    def test_from_env_precedence(self, monkeypatch):
        """测试ASHARE_* 优先级高于标准变量."""
        monkeypatch.setenv('ASHARE_HTTP_PROXY', 'http://ashare:80')
        monkeypatch.setenv('HTTP_PROXY', 'http://std:80')
        config = ProxyConfig.from_env()
        assert config.http == 'http://ashare:80'
    
    def test_as_requests_proxies(self):
        """测试生成requests代理格式."""
        config = ProxyConfig(http="http://p:1", https="https://p:2")
        proxies = config.as_requests_proxies()
        assert proxies == {"http": "http://p:1", "https": "https://p:2"}
    
    def test_apply_to_environment(self, monkeypatch):
        """测试应用配置到环境变量."""
        # 确保初始环境干净
        assert os.environ.get("HTTP_PROXY") is None
        
        config = ProxyConfig(http="http://new:80", https="https://new:80")
        config.apply_to_environment()
        
        # 验证环境变量被修改 (monkeypatch 会在测试后自动恢复)
        assert os.environ["HTTP_PROXY"] == "http://new:80"
        assert os.environ["HTTPS_PROXY"] == "https://new:80"
    
    def test_apply_to_environment_partial(self, monkeypatch):
        """测试部分应用配置."""
        # 预设 HTTP_PROXY
        monkeypatch.setenv('HTTP_PROXY', 'http://old:80')
        
        # 只应用 HTTPS 配置
        config = ProxyConfig(https="https://new:80")
        config.apply_to_environment()
        
        # HTTP_PROXY 应该保持不变
        assert os.environ["HTTP_PROXY"] == "http://old:80"
        # HTTPS_PROXY 应该被设置
        assert os.environ["HTTPS_PROXY"] == "https://new:80"


def _write_config(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def test_load_config_returns_empty_when_missing(monkeypatch, tmp_path):
    load_config.cache_clear()
    missing = tmp_path / "missing_absolutely.yaml"
    monkeypatch.setenv(CONFIG_FILE_ENV, str(missing))
    result = load_config()
    # Explicitly check type and length to avoid weird hashing issues during equality check
    assert isinstance(result, dict)
    assert len(result) == 0
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
    # 确保 config.yaml 有值，且 env 也有值，验证优先级和组合
    path = _write_config(
        tmp_path,
        "proxy:\n  http: http://cfg\n  https: http://cfgs\n",
    )
    monkeypatch.setenv(CONFIG_FILE_ENV, str(path))
    
    # 设置环境变量覆盖 http
    monkeypatch.setenv("ASHARE_HTTP_PROXY", "http://env")
    # 确保不设置 HTTPS 环境变量，让其从文件读取
    monkeypatch.delenv("ASHARE_HTTPS_PROXY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    
    load_config.cache_clear()
    proxy = ProxyConfig.from_env()
    
    assert proxy.http == "http://env"
    assert proxy.https == "http://cfgs"
    load_config.cache_clear()
