"""
QBittorrent plugin for AutoWorkflow.

Provides an authenticated qBittorrent WebUI client and injects it into
the workflow context during Engine runs.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING, Protocol, TypedDict, cast

from autoworkflow.services.plugins import Plugin

if TYPE_CHECKING:
    from autoworkflow.services.engine import Engine

import qbittorrentapi
from qbittorrentapi import Client as QBittorrentClient


class QBittorrentConfig(TypedDict, total=False):
    host: str
    port: int
    username: str
    password: str
    use_https: bool
    verify_ssl: bool
    timeout: int


class HasQBittorrent(Protocol):
    """
    Context typing helper for IDE completion.
    """

    qbittorrent: QBittorrentClient | None


class QBittorrentPlugin(Plugin[QBittorrentClient]):
    """
    Plugin that integrates qBittorrent's WebUI via qbittorrent-api.

    Creates and authenticates a client in setup() and injects it into the
    workflow context under common attribute names ("qbittorrent").
    """

    def __init__(
        self,
        name: str = "qbittorrent",
        *,
        host: str = "localhost",
        port: int = 8080,
        username: str = "admin",
        password: str = "adminadmin",
        use_https: bool = False,
        verify_ssl: bool = True,
        timeout: int = 30,
    ):
        self._name = name
        self._client: QBittorrentClient | None = None
        # Optional instance-level config to enable IDE hints on constructor
        self._config_override: QBittorrentConfig = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "use_https": use_https,
            "verify_ssl": verify_ssl,
            "timeout": timeout,
        }

    @property
    def name(self) -> str:
        return self._name

    def setup(self, engine: "Engine", config: Dict[str, Any]):  # type: ignore[name-defined]
        """
        Initialize and authenticate the qBittorrent client.

        Reads configuration from Engine.config under the key "qbittorrent"
        """
        # Merge order: instance override (constructor) <- engine config
        cfg_from_engine = (
            _extract_qbittorrent_config(
                config, fallback_keys=[self._name, "QBittorrentPlugin"]
            )
            or {}
        )
        cfg = cast(
            QBittorrentConfig,
            {
                **self._config_override,
                **(cfg_from_engine if isinstance(cfg_from_engine, dict) else {}),
            },
        )

        host: str = str(cfg.get("host", "localhost")).strip()
        port: int = int(cfg.get("port", 8080))
        username: str = str(cfg.get("username", "admin"))
        password: str = str(cfg.get("password", "adminadmin"))
        use_https: bool = bool(cfg.get("use_https", False))
        verify_ssl: bool = bool(cfg.get("verify_ssl", True))
        timeout: int = int(cfg.get("timeout", 30))

        requests_args: Dict[str, Any] = {"timeout": timeout, "verify": verify_ssl}
        client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=verify_ssl,
            REQUESTS_ARGS=requests_args,
            FORCE_SCHEME_FROM_HOST=use_https,
        )

        client.auth_log_in()
        self._client = client

    def get_context_provider(self) -> QBittorrentClient | None:
        """Return the configured qBittorrent client instance."""
        return self._client

    def get_context_injections(self) -> Dict[str, Any]:
        """Provide preferred attribute names for context injection."""
        return {"qbittorrent": self._client}

    def teardown(self):
        """Logout and cleanup the client on engine shutdown."""
        client = self._client
        self._client = None
        try:
            if client is not None:
                client.auth_log_out()
        except Exception:
            pass


def _extract_qbittorrent_config(
    config: Optional[Dict[str, Any]], *, fallback_keys: list[str] | None = None
) -> Dict[str, Any] | None:
    """Extract per-plugin configuration from engine config dict."""
    if not config:
        return None
    if "qbittorrent" in config and isinstance(config["qbittorrent"], dict):
        return config["qbittorrent"]
    if fallback_keys:
        for key in fallback_keys:
            val = config.get(key)
            if isinstance(val, dict):
                return val
    return None
