from __future__ import annotations

import time
from typing import Any

from autoworkflow.core import Workflow
from autoworkflow.services.telemetry import InMemoryEventSink, NodeEvent


def _count_events(events: list[NodeEvent], node_id: str, kind: str) -> int:
    return sum(1 for e in events if e.node_id == node_id and e.event == kind)


def test_concurrent_executor_emits_events_and_skips_non_selected_branch():
    wf = Workflow(name="Evt-Conc-Case-Skip")

    @wf.node
    def on():
        return 1

    @wf.node
    def unused():
        # would be skipped due to constant decision
        return "UNUSED"

    decision = wf.case(on=on, branches={1: 42, 2: unused})

    @wf.node
    def final(v: Any = wf.dep(decision)):
        return v

    sink = InMemoryEventSink()
    run_id = "rid-evt-1"
    # Simulate a proper run lifecycle
    sink.on_run_started(run_id, wf.name)
    out = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    assert out["final"] == 42
    sink.on_run_finished(run_id, True)

    events = sink.get_events()
    # final should be scheduled/started/finished
    assert _count_events(events, "final", "scheduled") >= 1
    assert _count_events(events, "final", "started") >= 1
    assert _count_events(events, "final", "finished") >= 1

    # 'unused' branch should be marked skipped (finished with status=skipped)
    skipped = [
        e
        for e in events
        if e.node_id == "unused"
        and e.event == "finished"
        and (e.data or {}).get("status") == "skipped"
    ]
    assert skipped, "Expected non-selected branch to be skipped"


def test_concurrent_executor_emits_events_for_normal_node_and_persists():
    wf = Workflow(name="Evt-Conc-Normal")

    @wf.node
    def a():
        time.sleep(0.01)
        return 7

    sink = InMemoryEventSink()
    run_id = "rid-evt-2"
    sink.on_run_started(run_id, wf.name)
    out = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    assert out["a"] == 7
    sink.on_run_finished(run_id, True)

    events = sink.get_events()
    assert _count_events(events, "a", "scheduled") >= 1
    assert _count_events(events, "a", "started") >= 1
    assert _count_events(events, "a", "finished") >= 1
