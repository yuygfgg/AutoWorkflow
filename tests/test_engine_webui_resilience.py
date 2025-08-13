from __future__ import annotations

from autoworkflow.core import Workflow
from autoworkflow.services.engine import Engine


def test_engine_webui_start_exception_is_swallowed(monkeypatch):
    # Force WebUIServer.start to raise to cover exception path in Engine.run
    import autoworkflow.services.webui as webui_mod

    def boom_start(self, engine_ref):
        raise RuntimeError("start boom")

    monkeypatch.setattr(webui_mod.WebUIServer, "start", boom_start, raising=True)

    eng = Engine()
    wf = Workflow(name="noop-webui")

    @wf.node
    def n():
        return 1

    eng.register_workflow(wf)
    eng.enable_web_ui(host="127.0.0.1", port=0)

    class KBTrigger:
        def attach(self, callback):
            # Immediately stop engine loop when engine attaches
            raise KeyboardInterrupt()

        def tick(self, now=None):
            pass

        def stop(self):
            pass

    eng.add_trigger(KBTrigger(), workflow_name=wf.name)

    # Should not raise despite WebUI start error
    eng.run()
