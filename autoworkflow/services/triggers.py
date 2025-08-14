"""
Built-in Triggers for the Engine.
"""

from __future__ import annotations

from typing import Protocol, Callable, Dict, Any, Optional
import os
import time
import re
import logging

logger = logging.getLogger(__name__)


class BaseTrigger(Protocol):
    """The protocol that all trigger implementations must follow."""

    def attach(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Attach a payload callback to be invoked when events occur."""

    def tick(self, now: Optional[float] = None) -> None:
        """Perform a single polling step. Implementations should be fast."""


class ScheduledTrigger:
    """Triggers a workflow based on a cron-like schedule."""

    def __init__(self, cron: str):
        self.cron = cron
        self._callback: Optional[Callable[[Dict[str, Any]], None]] = None
        self._interval_seconds: Optional[int] = None
        self._next_fire_at: Optional[float] = None

    def _parse_interval_seconds(self) -> int | None:
        """Parses a minimal cron-like string into seconds interval.

        Supported formats:
        - "every <N>s" | "every <N>m" | "every <N>h"
        - "*/N * * * *" (every N minutes)
        - "* * * * *" (every minute)
        """
        s = self.cron.strip().lower()

        m = re.match(r"^every\s+(\d+)\s*([smh])$", s)
        if m:
            value = int(m.group(1))
            unit = m.group(2)
            if unit == "s":
                return max(1, value)
            if unit == "m":
                return max(1, value) * 60
            if unit == "h":
                return max(1, value) * 3600

        m2 = re.match(r"^\*/(\d+)\s+\*\s+\*\s+\*\s+\*$", s)
        if m2:
            return max(1, int(m2.group(1))) * 60

        if s == "* * * * *":
            return 60

        return None

    def attach(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        logger.info("ScheduledTrigger attached with schedule: '%s'", self.cron)
        self._callback = callback
        self._interval_seconds = self._parse_interval_seconds()
        if self._interval_seconds is None:
            raise ValueError(
                "Unsupported cron format. Use 'every <N>s|m|h' or '*/N * * * *' or '* * * * *'."
            )
        now = time.time()
        self._next_fire_at = now + float(self._interval_seconds)

    def tick(self, now: Optional[float] = None) -> None:
        if self._callback is None or self._interval_seconds is None:
            return
        if now is None:
            now = time.time()
        if self._next_fire_at is None:
            self._next_fire_at = now + float(self._interval_seconds)
            return
        # Fire as many times as intervals passed to avoid drift
        while self._next_fire_at <= now:
            try:
                self._callback({})
            except Exception:
                logger.exception("ScheduledTrigger callback error")
            self._next_fire_at += float(self._interval_seconds)

        # No explicit stop lifecycle; engine controls polling cadence


class DirectoryWatchTrigger:
    """
    A polling directory watcher trigger for new files.
    """

    def __init__(
        self,
        watch_dir: str,
        *,
        suffix: str,
        interval_seconds: float = 2.0,
        stable_seconds: float = 1.0,
        payload_key: str = "file_path",
    ):
        self.watch_dir = os.path.abspath(watch_dir)
        self.suffix = suffix
        self.interval_seconds = max(0.5, float(interval_seconds))
        self.stable_seconds = max(0.5, float(stable_seconds))
        self.payload_key = payload_key
        self._seen: set[str] = set()
        self._callback: Optional[Callable[[Dict[str, Any]], None]] = None

    def attach(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        logger.info(
            "DirectoryWatchTrigger attached for '%s' with '*%s'",
            self.watch_dir,
            self.suffix,
        )
        self._callback = callback

    def tick(self, now: Optional[float] = None) -> None:
        if self._callback is None:
            return
        if now is None:
            now = time.time()
        try:
            with os.scandir(self.watch_dir) as it:
                for entry in it:
                    if not entry.is_file():
                        continue
                    if not entry.name.lower().endswith(self.suffix):
                        continue
                    full_path = os.path.join(self.watch_dir, entry.name)
                    if full_path in self._seen:
                        continue
                    try:
                        stat = entry.stat()
                    except FileNotFoundError:
                        continue
                    # Ensure file is stable not being written
                    if now - stat.st_mtime < self.stable_seconds:
                        continue
                    self._seen.add(full_path)
                    payload = {self.payload_key: full_path}
                    try:
                        logger.info("DirectoryWatchTrigger payload: %s", payload)
                        self._callback(payload)
                    except Exception:
                        logger.exception(
                            "DirectoryWatchTrigger callback error for '%s'",
                            full_path,
                        )
        except FileNotFoundError:
            logger.warning(
                "DirectoryWatchTrigger watch directory '%s' not found",
                self.watch_dir,
            )
        except Exception as e:
            logger.exception("DirectoryWatchTrigger scan error: %s", e)

