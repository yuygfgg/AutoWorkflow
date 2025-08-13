from __future__ import annotations

from typing import Any, Dict

from fastapi.testclient import TestClient

from autoworkflow.core import Workflow
from autoworkflow.services.engine import Engine


def test_enable_web_ui_wires_sink_and_workflows_endpoint_lists_dependencies():
    eng = Engine()

    wf = Workflow(name="W1")

    @wf.node
    def a():
        return 1

    @wf.node
    def b(x: int = wf.dep(a)):
        return x + 1

    eng.register_workflow(wf)
    eng.enable_web_ui(host="127.0.0.1", port=0)

    ui = eng._web_ui
    assert ui is not None and eng._event_sink is not None
    app = ui._build_app(eng)

    client = TestClient(app)
    r = client.get("/workflows")
    data: Dict[str, Any] = r.json()
    assert "W1" in data
    deps = data["W1"]["dependencies"]
    assert set(deps.get("b", [])) == {"a"}
