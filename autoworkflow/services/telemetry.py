"""
Lightweight telemetry/event sink for workflow runs and node statuses.

This module defines an in-memory event sink that can be used by a Web UI or
other observers to track the live status of workflow runs and their nodes.
"""

from __future__ import annotations

from typing import Protocol, Dict, Any, Optional, Tuple, List, Literal
from dataclasses import dataclass, asdict
import threading
import time


@dataclass
class NodeEvent:
    """Represents a node-level event during a workflow run."""

    run_id: str
    node_id: str
    event: Literal[
        "scheduled", "started", "finished", "failed", "run_started", "run_finished"
    ]
    timestamp: float
    data: Optional[Dict[str, Any]] = None


class NodeEventSink(Protocol):
    """Protocol for receiving run/node lifecycle events."""

    def on_run_started(self, run_id: str, workflow_name: str) -> None: ...

    def on_node_event(self, evt: NodeEvent) -> None: ...

    def on_run_finished(self, run_id: str, success: bool) -> None: ...


class InMemoryEventSink:
    """Thread-safe in-memory sink to store run and node status for UI polling."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # run_id -> {workflow_name, started_at, finished_at, success, node_status: {node_id: str}}
        self._runs: Dict[str, Dict[str, Any]] = {}
        # keep a list of recent events (bounded) as (seq, NodeEvent)
        self._events: List[Tuple[int, NodeEvent]] = []
        self._max_events: int = 1000
        self._seq: int = 0
        self._cv = threading.Condition(self._lock)

    # Public API used by UI server
    def list_runs(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            # return shallow copy for safety
            return {k: dict(v) for k, v in self._runs.items()}

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            run = self._runs.get(run_id)
            return dict(run) if run is not None else None

    def get_events(self) -> list[NodeEvent]:
        with self._lock:
            return [ev for _seq, ev in self._events]

    def get_events_since(self, since_seq: int) -> Tuple[List[Dict[str, Any]], int]:
        """Return serialized events with seq greater than since_seq, and the latest seq."""
        with self._lock:
            out: List[Dict[str, Any]] = [
                asdict(ev) for s, ev in self._events if s > since_seq
            ]
            return out, self._seq

    def wait_for_new_events(self, current_seq: int, timeout: float = 15.0) -> int:
        """Block until new events are available or timeout. Return latest seq (maybe unchanged)."""
        end = time.time() + timeout
        with self._cv:
            while self._seq <= current_seq:
                remaining = end - time.time()
                if remaining <= 0:
                    break
                self._cv.wait(timeout=remaining)
            return self._seq

    def on_run_started(self, run_id: str, workflow_name: str) -> None:
        now = time.time()
        with self._lock:
            self._runs[run_id] = {
                "workflow_name": workflow_name,
                "started_at": now,
                "finished_at": None,
                "success": None,
                "node_status": {},  # node_id -> status
            }
            # also record as stream event
            self._seq += 1
            self._events.append(
                (
                    self._seq,
                    NodeEvent(
                        run_id, "", "run_started", now, {"workflow_name": workflow_name}
                    ),
                )
            )
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events :]
            with self._cv:
                self._cv.notify_all()

    def on_node_event(self, evt: NodeEvent) -> None:
        with self._lock:
            run = self._runs.get(evt.run_id)
            if run is not None:
                status_map: Dict[str, str] = run.setdefault("node_status", {})
                # map event types to a coarse status
                if evt.event == "scheduled":
                    status_map[evt.node_id] = "queued"
                elif evt.event == "started":
                    status_map[evt.node_id] = "running"
                elif evt.event == "finished":
                    if evt.data and evt.data.get("status") == "skipped":
                        status_map[evt.node_id] = "skipped"
                    else:
                        status_map[evt.node_id] = "done"
                elif evt.event == "failed":
                    status_map[evt.node_id] = "failed"
            self._seq += 1
            self._events.append((self._seq, evt))
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events :]
            with self._cv:
                self._cv.notify_all()

    def on_run_finished(self, run_id: str, success: bool) -> None:
        now = time.time()
        with self._lock:
            run = self._runs.get(run_id)
            if run is not None:
                run["finished_at"] = now
                run["success"] = bool(success)
            # stream event
            self._seq += 1
            self._events.append(
                (
                    self._seq,
                    NodeEvent(
                        run_id, "", "run_finished", now, {"success": bool(success)}
                    ),
                )
            )
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events :]
            with self._cv:
                self._cv.notify_all()
