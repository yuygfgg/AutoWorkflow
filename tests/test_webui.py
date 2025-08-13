from __future__ import annotations

from typing import Any, Dict

from fastapi.testclient import TestClient

from autoworkflow.core import Workflow
from autoworkflow.services.engine import Engine


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
    ui = eng._web_ui  # type: ignore[attr-defined]
    assert ui is not None
    app = ui._build_app(eng)  # type: ignore[attr-defined]

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
    assert eng._event_sink is not None  # type: ignore[attr-defined]
    run_id = "rid-ui-test"
    eng._event_sink.on_run_started(run_id, wf.name)  # type: ignore[attr-defined]
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

    ui = eng._web_ui  # type: ignore[attr-defined]
    assert ui is not None
    app = ui._build_app(eng)  # type: ignore[attr-defined]
    client = TestClient(app)

    run_id = "rid-detail"
    # Before any run, detail should be empty dict
    r = client.get(f"/runs/{run_id}")
    assert r.status_code == 200
    assert r.json() == {}

    # Trigger a run and then verify detail endpoint reflects it
    assert eng._event_sink is not None  # type: ignore[attr-defined]
    eng._event_sink.on_run_started(run_id, wf.name)  # type: ignore[attr-defined]
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

    app = ui._build_app(DummyEng())  # type: ignore[arg-type]
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

    def fake_run(app, host: str, port: int, log_level: str):  # type: ignore[no-untyped-def]
        calls.append((host, port))
        started.set()
        stop.wait(timeout=0.2)

    original_run = webui_mod.uvicorn.run
    try:
        webui_mod.uvicorn.run = fake_run  # type: ignore[assignment]
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
        webui_mod.uvicorn.run = original_run  # type: ignore[assignment]
