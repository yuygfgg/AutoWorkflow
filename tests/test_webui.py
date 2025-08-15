from __future__ import annotations

import json
import socket
import threading
import time
from contextlib import closing
from typing import Any, Dict

import httpx
import uvicorn
from fastapi.testclient import TestClient

from autoworkflow.core import Workflow
from autoworkflow.services.engine import Engine
from autoworkflow.services.telemetry import NodeEvent


def build_simple_workflow() -> Workflow:
    wf = Workflow(name="UI-Test")

    @wf.node
    def hello() -> str:
        return "world"

    return wf


def test_webui_endpoints_workflows_and_runs():
    wf = build_simple_workflow()
    eng = Engine()
    eng.register_workflow(wf)
    eng.enable_web_ui(host="127.0.0.1", port=0)

    # Build the FastAPI app without starting a real server
    ui = eng._web_ui
    assert ui is not None
    app = ui._build_app(eng)

    client = TestClient(app)

    # Index should serve HTML
    r = client.get("/")
    assert r.status_code == 200
    assert "AutoWorkflow UI" in r.text

    # Workflows endpoint should list our workflow graph
    r = client.get("/workflows")
    assert r.status_code == 200
    data: Dict[str, Any] = r.json()
    assert "UI-Test" in data
    assert "hello" in data["UI-Test"]["nodes"]

    # Before any run, runs should be empty
    r = client.get("/runs")
    assert r.status_code == 200
    assert isinstance(r.json(), dict)

    # Simulate a workflow run with event sink wiring
    assert eng._event_sink is not None
    run_id = "rid-ui-test"
    eng._event_sink.on_run_started(run_id, wf.name)
    wf.run(run_id=run_id, event_sink=eng._event_sink)

    # Now the runs endpoint should show the run and node status
    r = client.get("/runs")
    assert r.status_code == 200
    runs_state: Dict[str, Any] = r.json()
    assert run_id in runs_state
    node_status = runs_state[run_id].get("node_status", {})
    assert node_status.get("hello") in {"done", "running", "queued", "failed"}


def test_webui_run_detail_and_runs_endpoint_updates():
    wf = build_simple_workflow()
    eng = Engine()
    eng.register_workflow(wf)
    eng.enable_web_ui(host="127.0.0.1", port=0)

    ui = eng._web_ui
    assert ui is not None
    app = ui._build_app(eng)
    client = TestClient(app)

    run_id = "rid-detail"
    # Before any run, detail should be empty dict
    r = client.get(f"/runs/{run_id}")
    assert r.status_code == 200
    assert r.json() == {}

    # Trigger a run and then verify detail endpoint reflects it
    assert eng._event_sink is not None
    eng._event_sink.on_run_started(run_id, wf.name)
    wf.run(run_id=run_id, event_sink=eng._event_sink)

    r = client.get(f"/runs/{run_id}")
    assert r.status_code == 200
    detail: Dict[str, Any] = r.json()
    assert detail.get("workflow_name") == wf.name
    assert isinstance(detail.get("node_status", {}), dict)


def test_webui_events_without_sink_streams_minimal_event():
    # Build a UI without setting a sink to exercise empty SSE path
    import autoworkflow.services.webui as webui_mod

    ui = webui_mod.WebUIServer()

    class DummyEng:
        _workflows: Dict[str, Any] = {}

    app = ui._build_app(DummyEng())
    client = TestClient(app)

    import json

    with client.stream("GET", "/events", timeout=1.0) as resp:
        assert resp.status_code == 200
        # Read a few lines at most to avoid indefinite waits
        for _ in range(20):
            line = next(resp.iter_lines(), None)
            if not line:
                continue
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            if line.startswith("data: "):
                payload = json.loads(line[len("data: ") :])
                assert payload["type"] in {"events", "snapshot"}
                break


def test_webui_start_starts_background_thread_and_is_idempotent_while_alive():
    import threading
    import autoworkflow.services.webui as webui_mod

    started = threading.Event()
    stop = threading.Event()
    calls: list[tuple[str, int]] = []

    def fake_run(app, host: str, port: int, log_level: str):
        calls.append((host, port))
        started.set()
        stop.wait(timeout=0.2)

    original_run = webui_mod.uvicorn.run
    try:
        webui_mod.uvicorn.run = fake_run
        ui = webui_mod.WebUIServer(host="127.0.0.1", port=0)
        eng = Engine()
        ui.start(eng)
        assert started.wait(timeout=1.0)
        assert ui._thread is not None and ui._thread.is_alive()
        # Second start while thread is alive should no-op
        ui.start(eng)
        stop.set()
        ui._thread.join(timeout=1.0)
        assert len(calls) >= 1
    finally:
        webui_mod.uvicorn.run = original_run


def test_webui_events_with_sink_streams_snapshot_and_updates(monkeypatch):
    def find_free_port():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    port = find_free_port()
    host = "127.0.0.1"

    wf = build_simple_workflow()
    eng = Engine()
    eng.register_workflow(wf)
    eng.enable_web_ui(host=host, port=port)

    ui = eng._web_ui
    assert ui is not None and eng._event_sink is not None

    server_started = threading.Event()
    original_startup = uvicorn.Server.startup

    async def new_startup(self, *args, **kwargs):
        await original_startup(self, *args, **kwargs)
        server_started.set()

    monkeypatch.setattr(uvicorn.Server, "startup", new_startup)

    ui.start(eng)
    assert server_started.wait(timeout=5), "Server did not start in time"

    run_id = "rid-sse-test"
    try:
        with httpx.stream("GET", f"http://{host}:{port}/events", timeout=5) as resp:
            assert resp.status_code == 200

            lines_iterator = resp.iter_lines()

            # Check for initial snapshot
            snapshot_received = False
            for line in lines_iterator:
                if line.startswith("data: "):
                    data = json.loads(line[len("data: ") :])
                    if data.get("type") == "snapshot":
                        snapshot_received = True
                        break
            assert snapshot_received, "Did not receive snapshot"

            # Trigger a new event and check if it's received
            event_to_send = NodeEvent(run_id, "hello", "scheduled", time.time())
            eng._event_sink.on_node_event(event_to_send)

            event_received = False
            for line in lines_iterator:
                if line.startswith("data: "):
                    data = json.loads(line[len("data: ") :])
                    if data.get("type") == "events":
                        events = data.get("events", [])
                        if (
                            events
                            and events[0]["run_id"] == run_id
                            and events[0]["node_id"] == "hello"
                        ):
                            event_received = True
                            break
            assert event_received, "Did not receive new event"
    finally:
        # The server thread is a daemon, so it will be cleaned up on exit.
        pass


def test_webui_workflows_endpoint_handles_complex_graph():
    wf = Workflow(name="Complex-UI")

    @wf.node
    def source():
        return "a"

    @wf.node
    def branch_a():
        return "A"

    @wf.node
    def branch_b():
        return "B"

    case_node = wf.case(
        on=source,
        branches={"a": branch_a, "b": branch_b, "none": "skip"},
    )

    @wf.node
    def final(x=wf.dep(case_node)):
        return x

    eng = Engine()
    eng.register_workflow(wf)
    eng.enable_web_ui(host="127.0.0.1", port=0)

    ui = eng._web_ui
    assert ui is not None
    app = ui._build_app(eng)
    client = TestClient(app)

    r = client.get("/workflows")
    assert r.status_code == 200
    data = r.json()

    assert "Complex-UI" in data
    wf_data = data["Complex-UI"]

    # Check for case node metadata
    case_meta = next(
        (n for n in wf_data["nodes_meta"] if n["id"] == case_node.node_id), None
    )
    assert case_meta is not None
    assert case_meta["kind"] == "case"
    assert case_meta["on"] == source.node_id
    assert len(case_meta["branches"]) == 3
    branch_targets = {b["condition"]: b["target_node"] for b in case_meta["branches"]}
    assert branch_targets["a"] == branch_a.node_id
    assert branch_targets["b"] == branch_b.node_id
    assert branch_targets["none"] is None  # for "skip"

    # Check for labeled dependencies
    deps_labeled = wf_data["dependencies_labeled"]
    assert final.node_id in deps_labeled
    final_deps = deps_labeled[final.node_id]
    assert len(final_deps) == 1
    assert final_deps[0]["from"] == case_node.node_id
    assert final_deps[0]["arg"] == "x"
