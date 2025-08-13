from __future__ import annotations

import pytest

from autoworkflow.services.engine import Engine
from autoworkflow_plugins import CloudrevePlugin
from autoworkflow.core import BaseContext


class _DummyCloudreve:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logged_in = False
        self.login_args: tuple[str, str] | None = None

    def login(self, username: str, password: str):
        self.logged_in = True
        self.login_args = (username, password)


@pytest.fixture(autouse=True)
def patch_cloudreve_sdk(monkeypatch):
    monkeypatch.setattr(
        "autoworkflow_plugins.cloudreve.CloudreveClient", _DummyCloudreve, raising=True
    )


def test_cloudreve_plugin_setup_and_injections_with_constructor_args():
    eng = Engine(config={})
    pl = CloudrevePlugin(base_url="http://127.0.0.1:5212", username="u", password="p")
    pl.setup(eng, eng.config)

    provider = pl.get_context_provider()
    assert isinstance(provider, _DummyCloudreve)
    assert provider.logged_in is True
    assert provider.base_url == "http://127.0.0.1:5212"
    assert provider.login_args == ("u", "p")

    inj = pl.get_context_injections()
    assert inj[pl.name] is provider


def test_cloudreve_plugin_setup_with_engine_config_override():
    eng = Engine(
        config={
            "cloudreve": {
                "base_url": "http://example:5212",
                "username": "eu",
                "password": "ep",
            }
        }
    )

    # constructor args should be overridden by engine config
    pl = CloudrevePlugin(
        base_url="http://ignored", username="ignored", password="ignored"
    )
    pl.setup(eng, eng.config)
    provider = pl.get_context_provider()
    assert isinstance(provider, _DummyCloudreve)
    assert provider.base_url == "http://example:5212"
    assert provider.login_args == ("eu", "ep")


class _Ctx(BaseContext):
    cloudreve: _DummyCloudreve | None


def test_engine_injects_cloudreve_into_context():
    eng = Engine(config={"cloudreve": {"username": "u2", "password": "p2"}})
    pl = CloudrevePlugin()
    eng.register_plugin(pl)
    eng.setup_plugins()

    ctx = _Ctx()
    eng.inject_plugins_into_context(ctx)
    assert getattr(ctx, "cloudreve", None) is not None
    assert isinstance(ctx.cloudreve, _DummyCloudreve)
