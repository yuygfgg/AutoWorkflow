from typing import Any

import types
import pytest

from autoworkflow.services.engine import Engine
from autoworkflow_plugins.qbittorrent import QBittorrentPlugin


class _DummyClient:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.logged_in = False
        self.logged_out = False

    def auth_log_in(self):
        self.logged_in = True

    def auth_log_out(self):
        self.logged_out = True


@pytest.fixture(autouse=True)
def patch_qbittorrentapi(monkeypatch):
    # Patch the module attribute used by the plugin
    fake_mod = types.SimpleNamespace(Client=_DummyClient)
    monkeypatch.setattr(
        "autoworkflow_plugins.qbittorrent.qbittorrentapi", fake_mod, raising=True
    )


def test_qbittorrent_plugin_setup_and_teardown_with_constructor_args():
    engine = Engine(config={})

    pl = QBittorrentPlugin(
        host="h",
        port=9000,
        username="u",
        password="p",
        use_https=True,
        verify_ssl=False,
        timeout=5,
    )
    pl.setup(engine, engine.config)

    # Provider and mapping should be available
    provider = pl.get_context_provider()
    assert isinstance(provider, _DummyClient)
    inj = pl.get_context_injections()
    assert inj["qbittorrent"] is provider

    # Teardown logs out and clears client
    pl.teardown()
    assert provider.logged_out is True
    assert pl.get_context_provider() is None


def test_qbittorrent_plugin_setup_with_engine_config_override():
    # Engine config should override constructor defaults/values
    engine = Engine(
        config={
            "qbittorrent": {
                "host": "h2",
                "port": 8081,
                "username": "u2",
                "password": "p2",
                "use_https": False,
                "verify_ssl": True,
                "timeout": 7,
            }
        }
    )

    pl = QBittorrentPlugin(name="customname", host="ignored", port=1111)
    pl.setup(engine, engine.config)
    provider = pl.get_context_provider()
    assert isinstance(provider, _DummyClient)
    # ensure values come from engine.config, not constructor
    assert provider.kwargs["host"] == "h2"
    assert provider.kwargs["port"] == 8081


def test_qbittorrent_plugin_uses_constructor_defaults_when_missing_config():
    eng = Engine(config={})
    pl = QBittorrentPlugin()
    pl.setup(eng, eng.config)
    provider = pl.get_context_provider()
    assert isinstance(provider, _DummyClient)
    # ensure teardown path runs without errors
    pl.teardown()
    

def test_qbittorrent_plugin_supports_fallback_keys_with_class_name():
    engine = Engine(
        config={
            "QBittorrentPlugin": {
                "host": "fbhost",
                "port": 8090,
                "username": "fu",
                "password": "fp",
                "use_https": True,
                "verify_ssl": False,
                "timeout": 9,
            }
        }
    )

    pl = QBittorrentPlugin()  # default name is "qbittorrent"
    pl.setup(engine, engine.config)
    provider = pl.get_context_provider()
    assert isinstance(provider, _DummyClient)
    assert provider.kwargs["host"] == "fbhost"
    assert provider.kwargs["port"] == 8090