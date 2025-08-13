"""
Bangumi.moe plugin for AutoWorkflow.

Provides an authenticated Bangumi.moe client and injects it into the
workflow context during Engine runs.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING, Protocol, TypedDict, cast

import hashlib
import requests

from autoworkflow.services.plugins import Plugin

if TYPE_CHECKING:
    from autoworkflow.services.engine import Engine


class BangumiMoeConfig(TypedDict, total=False):
    base_url: str
    username: str
    password: str
    verify_ssl: bool
    timeout: int


class BangumiMoeClient:
    """Thin client for Bangumi.moe Web API.

    This client keeps a requests.Session for authenticated calls.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://bangumi.moe",
        username: str,
        password: str,
        verify_ssl: bool = True,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self._session = requests.Session()
        self._is_logged_in = False

    @property
    def session(self) -> requests.Session:
        return self._session

    def login(self) -> None:
        """Authenticate with Bangumi.moe API using MD5 password scheme."""
        login_url = f"{self.base_url}/api/user/signin"
        headers = {"x-kl-kis-ajax-request": "Ajax_Request"}
        encrypted_pwd = hashlib.md5(self.password.encode()).hexdigest()
        payload = {"username": self.username, "password": encrypted_pwd}
        resp = self._session.post(
            login_url,
            headers=headers,
            json=payload,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()
        self._is_logged_in = True

    def upload_torrent(self, file_path: str) -> Dict[str, Any]:
        """Upload a .torrent file and return the API JSON result."""
        if not self._is_logged_in:
            self.login()
        url = f"{self.base_url}/api/v2/torrent/upload"
        headers = {"x-kl-kis-ajax-request": "Ajax_Request"}
        with open(file_path, "rb") as f:
            files = {"file": (file_path.split("/")[-1], f)}
            resp = self._session.post(
                url, headers=headers, files=files, timeout=self.timeout, verify=self.verify_ssl
            )
        resp.raise_for_status()
        return resp.json()

    def add_torrent(self, templ_torrent_id: str, file_id: str, title: str, introduction: str) -> Dict[str, Any]:
        """Finalize and add an uploaded torrent with metadata."""
        if not self._is_logged_in:
            self.login()
        url = f"{self.base_url}/api/v2/torrent/add"
        headers = {"x-kl-kis-ajax-request": "Ajax_Request"}
        payload = {
            "templ_torrent_id": templ_torrent_id,
            "file_id": file_id,
            "title": title,
            "introduction": introduction,
        }
        resp = self._session.post(
            url, headers=headers, json=payload, timeout=self.timeout, verify=self.verify_ssl
        )
        resp.raise_for_status()
        return resp.json()

    def publish(self, torrent_path: str, *, title: str, introduction: str) -> Dict[str, Any]:
        """Convenience method: upload then add in a single call."""
        up = self.upload_torrent(torrent_path)
        return self.add_torrent(
            templ_torrent_id=str(up.get("torrent_id")),
            file_id=str(up.get("file_id")),
            title=title,
            introduction=introduction,
        )

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass


class HasBangumiMoe(Protocol):
    """Context typing helper for IDE completion."""

    bangumi_moe: BangumiMoeClient | None


class BangumiMoePlugin(Plugin[BangumiMoeClient]):
    """
    Plugin that integrates Bangumi.moe Web API.

    Creates and authenticates a client in setup() and injects it into the
    workflow context under attribute name "bangumi_moe" by default.
    """

    def __init__(
        self,
        name: str = "bangumi_moe",
        *,
        base_url: str = "https://bangumi.moe",
        username: str | None = None,
        password: str | None = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ):
        self._name = name
        self._client: Optional[BangumiMoeClient] = None
        self._config_override: BangumiMoeConfig = {
            "base_url": base_url,
            "username": username or "",
            "password": password or "",
            "verify_ssl": verify_ssl,
            "timeout": timeout,
        }

    @property
    def name(self) -> str:
        return self._name

    def setup(self, engine: "Engine", config: Dict[str, Any]):  # type: ignore[name-defined]
        cfg = _extract_bangumimoe_config(config, fallback_keys=[self._name, "BangumiMoePlugin"]) or {}
        merged = cast(
            BangumiMoeConfig,
            {
                **self._config_override,
                **(cfg if isinstance(cfg, dict) else {}),
            },
        )

        username = str(merged.get("username", "")).strip()
        password = str(merged.get("password", "")).strip()
        if not username or not password:
            raise ValueError("BangumiMoePlugin requires 'username' and 'password' in config or constructor.")

        base_url = str(merged.get("base_url", "https://bangumi.moe")).strip()
        verify_ssl = bool(merged.get("verify_ssl", True))
        timeout = int(merged.get("timeout", 30))

        client = BangumiMoeClient(
            base_url=base_url,
            username=username,
            password=password,
            verify_ssl=verify_ssl,
            timeout=timeout,
        )
        # Authenticate now so users get early failure instead of runtime
        client.login()
        self._client = client

    def get_context_provider(self) -> BangumiMoeClient | None:
        return self._client

    def get_context_injections(self) -> Dict[str, Any]:
        return {self._name: self._client}

    def teardown(self):
        client = self._client
        self._client = None
        try:
            if client is not None:
                client.close()
        except Exception:
            pass


def _extract_bangumimoe_config(
    config: Optional[Dict[str, Any]], *, fallback_keys: list[str] | None = None
) -> Dict[str, Any] | None:
    if not config:
        return None
    if "bangumi_moe" in config and isinstance(config["bangumi_moe"], dict):
        return config["bangumi_moe"]
    if fallback_keys:
        for key in fallback_keys:
            val = config.get(key)
            if isinstance(val, dict):
                return val
    return None
