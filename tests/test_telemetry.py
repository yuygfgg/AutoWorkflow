from __future__ import annotations

from typing import Dict
import threading
import time

from autoworkflow.services.telemetry import InMemoryEventSink, NodeEvent


def test_telemetry_run_lifecycle_and_status_mapping_and_getters():
    sink = InMemoryEventSink()

    run_id = "rid-telemetry"
    sink.on_run_started(run_id, "WF")

    # push several node events to exercise status mapping
    sink.on_node_event(NodeEvent(run_id, "n1", "scheduled", time.time()))
    sink.on_node_event(NodeEvent(run_id, "n1", "started", time.time()))
    sink.on_node_event(
        NodeEvent(run_id, "n2", "finished", time.time(), {"status": "skipped"})
    )
    sink.on_node_event(NodeEvent(run_id, "n3", "failed", time.time()))
    sink.on_node_event(NodeEvent(run_id, "n1", "finished", time.time()))

    sink.on_run_finished(run_id, success=False)

    # list_runs and get_run should reflect stored info
    runs = sink.list_runs()
    assert run_id in runs
    detail = sink.get_run(run_id)
    assert detail is not None and detail.get("success") is False
    status_map: Dict[str, str] = detail.get("node_status", {})  # type: ignore[assignment]
    assert status_map.get("n1") == "done"
    assert status_map.get("n2") == "skipped"
    assert status_map.get("n3") == "failed"

    # get_events exposes a list; get_events_since filters using seq
    events_all = sink.get_events()
    assert events_all, "expected some events recorded"
    last_seq = 0
    # wait_for_new_events with timeout should not hang and should return latest seq
    latest = sink.wait_for_new_events(last_seq, timeout=0.01)
    assert latest >= last_seq
    filtered, latest2 = sink.get_events_since(latest)
    assert filtered == [] and latest2 >= latest


def test_telemetry_wait_blocks_until_notified():
    sink = InMemoryEventSink()
    run_id = "rid-wait"
    sink.on_run_started(run_id, "WF2")

    # Capture current seq
    current_seq = sink.get_events_since(0)[1]
    woke = threading.Event()

    def waiter():
        # Should block until we add a new event, then return new seq
        latest = sink.wait_for_new_events(current_seq, timeout=2.0)
        assert latest > current_seq
        woke.set()

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.05)
    sink.on_node_event(NodeEvent(run_id, "n", "scheduled", time.time()))
    assert woke.wait(timeout=1.0)
    t.join(timeout=1.0)
