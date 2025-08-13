import os
import time
from typing import Dict, Any

from autoworkflow.services.triggers import ScheduledTrigger, DirectoryWatchTrigger


def test_scheduled_trigger_parse_intervals():
    st = ScheduledTrigger("every 2s")
    assert st._parse_interval_seconds() == 2

    stm = ScheduledTrigger("every 2m")
    assert stm._parse_interval_seconds() == 120

    sth = ScheduledTrigger("every 2h")
    assert sth._parse_interval_seconds() == 7200

    st5 = ScheduledTrigger("*/5 * * * *")
    assert st5._parse_interval_seconds() == 300

    st1 = ScheduledTrigger("* * * * *")
    assert st1._parse_interval_seconds() == 60

    st_bad = ScheduledTrigger("invalid cron expression")
    assert st_bad._parse_interval_seconds() is None


def test_scheduled_trigger_start_stop_and_callback_exception(monkeypatch):
    calls: Dict[str, int] = {"cnt": 0}

    st = ScheduledTrigger("every 1s")

    # Speed up interval to keep test fast
    monkeypatch.setattr(st, "_parse_interval_seconds", lambda: 0.01)

    def cb(_payload: Dict[str, Any]):
        calls["cnt"] += 1
        # Raise once to exercise exception handling path in the runner
        if calls["cnt"] == 1:
            raise RuntimeError("boom")

    st.attach(cb)
    deadline = time.time() + 0.03
    while time.time() < deadline:
        st.tick()
        time.sleep(0.002)
    # no stop lifecycle in cooperative mode

    assert calls["cnt"] >= 1


def test_directory_watch_trigger_detects_new_file_once(tmp_path):
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    target = watch_dir / "a.txt"
    target.write_text("hello")

    # Make the file older than stable_seconds
    old = time.time() - 2
    os.utime(target, (old, old))

    seen = []

    def cb(payload: Dict[str, Any]):
        seen.append(payload["file_path"])  # default key

    trg = DirectoryWatchTrigger(
        str(watch_dir), suffix=".txt", interval_seconds=0.05, stable_seconds=0.05
    )
    trg.attach(cb)
    deadline = time.time() + 0.2
    while time.time() < deadline and len(seen) < 1:
        trg.tick()
        time.sleep(0.01)
    # no stop lifecycle in cooperative mode

    assert len(seen) == 1
    assert seen[0] == str(target)


def test_directory_watch_trigger_custom_payload_key(tmp_path):
    watch_dir = tmp_path / "watch2"
    watch_dir.mkdir()
    target = watch_dir / "b.dat"
    target.write_text("world")

    old = time.time() - 2
    os.utime(target, (old, old))

    captured = []

    def cb(payload: Dict[str, Any]):
        captured.append(payload["path"])  # custom key

    trg = DirectoryWatchTrigger(
        str(watch_dir),
        suffix=".dat",
        interval_seconds=0.05,
        stable_seconds=0.05,
        payload_key="path",
    )
    trg.attach(cb)
    deadline = time.time() + 0.2
    while time.time() < deadline and len(captured) < 1:
        trg.tick()
        time.sleep(0.01)
    # no stop lifecycle in cooperative mode

    assert captured == [str(target)]


def test_directory_watch_trigger_missing_dir():
    missing = os.path.join("/tmp", f"aw-missing-{int(time.time()*1000)}")
    # Ensure it doesn't exist
    if os.path.exists(missing):
        # Very unlikely, but guard anyway
        if os.path.isdir(missing):
            for name in os.listdir(missing):
                os.remove(os.path.join(missing, name))
            os.rmdir(missing)
        else:
            os.remove(missing)

    trg = DirectoryWatchTrigger(
        missing, suffix=".x", interval_seconds=0.05, stable_seconds=0.05
    )

    # Should not raise even if directory is missing; it logs and continues
    trg.attach(lambda _p: None)
    deadline = time.time() + 0.12
    while time.time() < deadline:
        trg.tick()
        time.sleep(0.01)
    # no stop lifecycle in cooperative mode
