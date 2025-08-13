"""
Cloudreve plugin for AutoWorkflow.

Provides an authenticated Cloudreve client and injects it into the
workflow context during Engine runs.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING, Protocol, TypedDict, cast

from autoworkflow.services.plugins import Plugin

if TYPE_CHECKING:
    from autoworkflow.services.engine import Engine

from cloudreve import Cloudreve as CloudreveClient


class CloudreveConfig(TypedDict, total=False):
    base_url: str
    url: str
    username: str
    email: str
    password: str


class HasCloudreve(Protocol):
    """Context typing helper for IDE completion."""

    cloudreve: CloudreveClient | None


class CloudrevePlugin(Plugin[CloudreveClient]):
    """
    Plugin that integrates Cloudreve via the python `cloudreve` SDK.

    Creates and authenticates a client in setup() and injects it into the
    workflow context under attribute name "cloudreve" by default.
    """

    def __init__(
        self,
        name: str = "cloudreve",
        *,
        base_url: str = "http://127.0.0.1:5212",
        username: str | None = None,
        password: str | None = None,
    ):
        self._name = name
        self._client: CloudreveClient | None = None
        self._config_override: CloudreveConfig = {
            "base_url": base_url,
            "username": username or "",
            "password": password or "",
        }

    @property
    def name(self) -> str:
        return self._name

    def setup(self, engine: "Engine", config: Dict[str, Any]):
        # Merge order: instance overrides <- engine config
        cfg_from_engine = (
            _extract_cloudreve_config(
                config, fallback_keys=[self._name, "CloudrevePlugin", "cloudreve"]
            )
            or {}
        )
        merged = cast(
            CloudreveConfig,
            {
                **self._config_override,
                **(cfg_from_engine if isinstance(cfg_from_engine, dict) else {}),
            },
        )

        base_url = str(
            merged.get("base_url") or merged.get("url") or "http://127.0.0.1:5212"
        ).strip()
        username = str(merged.get("username") or merged.get("email") or "").strip()
        password = str(merged.get("password") or "").strip()

        if not username or not password:
            raise ValueError(
                "CloudrevePlugin requires 'username' (or 'email') and 'password'."
            )

        client = CloudreveClient(base_url)
        # Perform login early to fail-fast
        client.login(username, password)
        self._client = client

    def get_context_provider(self) -> CloudreveClient | None:
        return self._client

    def get_context_injections(self) -> Dict[str, Any]:
        return {self._name: self._client}

    def teardown(self):
        # Cloudreve SDK does not expose an explicit close; drop reference
        self._client = None


def _extract_cloudreve_config(
    config: Optional[Dict[str, Any]], *, fallback_keys: list[str] | None = None
) -> Dict[str, Any] | None:
    if not config:
        return None
    # Preferred key
    if "cloudreve" in config and isinstance(config["cloudreve"], dict):
        return config["cloudreve"]
    # Additional fallbacks
    if fallback_keys:
        for key in fallback_keys:
            val = config.get(key)
            if isinstance(val, dict):
                return val
    return None
