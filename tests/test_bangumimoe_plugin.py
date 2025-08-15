from __future__ import annotations

from typing import Any

import hashlib
import types
import pytest

from autoworkflow.services.engine import Engine
from autoworkflow_plugins import BangumiMoePlugin


class _DummyResponse:
    def __init__(
        self, status_code: int = 200, json_data: Any | None = None, text: str = ""
    ):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data


class _DummySession:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def post(
        self, url: str, headers=None, json=None, files=None, timeout=None, verify=None
    ):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "files": files,
                "timeout": timeout,
                "verify": verify,
            }
        )
        if url.endswith("/api/user/signin"):
            return _DummyResponse(200, {"ok": True})
        if url.endswith("/api/v2/torrent/upload"):
            return _DummyResponse(200, {"torrent_id": "tid-1", "file_id": "fid-1"})
        if url.endswith("/api/v2/torrent/add"):
            return _DummyResponse(200, {"ok": True, "id": "added-1"})
        return _DummyResponse(404, text="not found")

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def patch_requests(monkeypatch):
    fake_requests = types.SimpleNamespace(Session=_DummySession)
    monkeypatch.setattr(
        "autoworkflow_plugins.bangumiMoe.requests", fake_requests, raising=True
    )


def test_bangumimoe_plugin_setup_and_teardown_with_constructor_args():
    eng = Engine(config={})
    username = "user1"
    password = "pass1"

    pl = BangumiMoePlugin(username=username, password=password)
    pl.setup(eng, eng.config)

    provider = pl.get_context_provider()
    assert provider is not None
    inj = pl.get_context_injections()
    assert inj[pl.name] is provider

    # ensure login used md5 hashed password
    sess: _DummySession = provider.session  # type: ignore[attr-defined]
    assert len(sess.calls) >= 1
    login_call = sess.calls[0]
    assert login_call["url"].endswith("/api/user/signin")
    assert login_call["json"]["username"] == username
    assert login_call["json"]["password"] == hashlib.md5(password.encode()).hexdigest()

    # teardown closes session and clears client
    pl.teardown()
    assert sess.closed is True
    assert pl.get_context_provider() is None


def test_bangumimoe_plugin_setup_with_engine_config_override():
    eng = Engine(
        config={
            "bangumi_moe": {
                "base_url": "https://example.org",
                "username": "u2",
                "password": "p2",
                "verify_ssl": False,
                "timeout": 7,
            }
        }
    )

    # constructor args should be overridden by engine config
    pl = BangumiMoePlugin(
        username="ignored", password="ignored", base_url="https://ignored"
    )
    pl.setup(eng, eng.config)
    provider = pl.get_context_provider()
    assert provider is not None
    assert provider.base_url == "https://example.org"
    assert provider.username == "u2"
    assert provider.password == "p2"


def test_bangumimoe_client_publish_flow(tmp_path):
    eng = Engine(config={"bangumi_moe": {"username": "u3", "password": "p3"}})
    pl = BangumiMoePlugin()
    pl.setup(eng, eng.config)
    client = pl.get_context_provider()
    assert client is not None
    torrent_path = tmp_path / "dummy.torrent"
    torrent_path.write_bytes(b"dummy")
    res = client.publish(str(torrent_path), title="T", introduction="I")
    assert res.get("ok") is True


def test_bangumimoe_plugin_supports_fallback_keys_with_class_name():
    eng = Engine(
        config={
            "BangumiMoePlugin": {
                "base_url": "https://fallback.example",
                "username": "fu",
                "password": "fp",
                "verify_ssl": True,
                "timeout": 12,
            }
        }
    )

    pl = BangumiMoePlugin()  # default name
    pl.setup(eng, eng.config)

    provider = pl.get_context_provider()
    assert provider is not None
    assert provider.base_url == "https://fallback.example"
    assert provider.username == "fu"
    assert provider.password == "fp"
