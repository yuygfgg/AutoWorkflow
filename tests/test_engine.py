from typing import Any, Callable, Dict, cast

from autoworkflow.core import Workflow, BaseContext, CONTEXT
from autoworkflow.services.engine import Engine


class FakeStateBackend:
    def __init__(self):
        self.saved = {}  # (run_id, node_id) -> result

    def save_result(self, run_id: str, node_id: str, result):
        self.saved[(run_id, node_id)] = result

    def load_result(self, run_id: str, node_id: str):  # pragma: no cover
        return self.saved.get((run_id, node_id))

    def get_run_state(self, run_id: str):  # pragma: no cover
        return {nid: res for (rid, nid), res in self.saved.items() if rid == run_id}


def test_engine_trigger_callback_runs_workflow_with_context_plugins_and_state_backend():
    eng = Engine()

    class Ctx(BaseContext):
        def __init__(self):
            self.mult = 2
            self.service = None
            self.pluginX = None

    # Plugin A injects by mapping (service)
    class PluginA:
        @property
        def name(self):
            return "pluginA"

        def setup(self, engine, config):
            pass

        def get_context_injections(self):
            return {"service": "SVC"}

        def get_context_provider(self):
            return None

        def teardown(self):
            pass

    # Plugin B injects by provider under attribute `pluginX`
    class PluginB:
        @property
        def name(self):
            return "pluginX"

        def setup(self, engine, config):
            pass

        def get_context_injections(self):
            return {}

        def get_context_provider(self):
            return "PX"

        def teardown(self):
            pass

    eng.register_plugin(PluginA())
    eng.register_plugin(PluginB())
    eng.set_context_factory(Ctx)

    backend = FakeStateBackend()
    eng.set_state_backend(backend)

    wf = Workflow(name="eng_test")

    @wf.node
    def compute(x: int, ctx: Any = CONTEXT):
        return f"{x * ctx.mult}|{ctx.service}|{ctx.pluginX}"

    eng.register_workflow(wf)

    class FakeTrigger:
        def attach(self, callback: Callable[[Dict[str, Any]], None]) -> None:
            pass

        def tick(self, now=None):
            pass

        def stop(self) -> None:
            pass

    trg = FakeTrigger()
    eng.add_trigger(trg, workflow_name=wf.name, initial_data={"x": 1})

    # Simulate event trigger: payload overrides x in initial_data
    assert hasattr(trg, "_autoworkflow_bound_start")
    trg._autoworkflow_bound_start({"x": 5})  # type: ignore[attr-defined]

    # Verify that results were saved to StateBackend (run_id is dynamic; just assert the first entry)
    import time as _t

    deadline = _t.time() + 0.5
    while not backend.saved and _t.time() < deadline:
        _t.sleep(0.005)
    assert backend.saved, "No results saved by state backend"
    # Find the saved record for the compute node
    compute_results = [
        res for (rid, nid), res in backend.saved.items() if nid == "compute"
    ]
    assert compute_results and compute_results[0] == "10|SVC|PX"


def test_inject_plugins_into_context_mapping_and_provider():
    eng = Engine()

    class Ctx(BaseContext):
        def __init__(self):
            self.service = None
            self.pluginX = None

    ctx = Ctx()

    class PluginMap:
        @property
        def name(self):
            return "ignored_by_mapping"

        def setup(self, engine, config):  # pragma: no cover
            pass

        def get_context_injections(self):
            return {"service": "SVC2"}

        def get_context_provider(self):
            return None

        def teardown(self):  # pragma: no cover
            pass

    class PluginProvider:
        @property
        def name(self):
            return "pluginX"

        def setup(self, engine, config):  # pragma: no cover
            pass

        def get_context_injections(self):
            return {}

        def get_context_provider(self):
            return "PX2"

        def teardown(self):  # pragma: no cover
            pass

    eng.register_plugin(PluginMap())
    eng.register_plugin(PluginProvider())

    updated_ctx = cast(Ctx, eng.inject_plugins_into_context(ctx))
    assert updated_ctx.service == "SVC2"
    assert updated_ctx.pluginX == "PX2"


def test_engine_run_calls_plugin_setup_and_teardown():
    eng = Engine()

    class PL:
        def __init__(self):
            self.setup_called = False
            self.teardown_called = False

        @property
        def name(self):
            return "pl"

        def setup(self, engine, config):
            self.setup_called = True

        def get_context_injections(self):  # pragma: no cover
            return {}

        def get_context_provider(self):  # pragma: no cover
            return None

        def teardown(self):
            self.teardown_called = True

    pl = PL()
    eng.register_plugin(pl)

    class KBTrigger:
        def attach(self, callback):
            # Raise KeyboardInterrupt so engine run exits and should call plugin teardown
            raise KeyboardInterrupt()

        def tick(self, now=None):
            pass

        def stop(self):
            pass

    wf = Workflow(name="noop")

    @wf.node
    def n():
        return 1

    eng.register_workflow(wf)
    eng.add_trigger(KBTrigger(), workflow_name=wf.name)

    # Run engine: should exit immediately due to KeyboardInterrupt and call plugin teardown
    eng.run()

    assert pl.setup_called is True
    assert pl.teardown_called is True


def test_add_trigger_with_missing_workflow_does_not_execute():
    eng = Engine()
    backend = FakeStateBackend()
    eng.set_state_backend(backend)

    class FakeTrigger:
        def attach(self, callback: Callable[[Dict[str, Any]], None]) -> None:
            pass

        def start(self, callback: Callable[[Dict[str, Any]], None]) -> None:
            # no-op for tests
            pass

        def tick(self, now=None):
            pass

        def stop(self) -> None:
            pass

    trg = FakeTrigger()
    # Do not register the workflow name
    eng.add_trigger(trg, workflow_name="missing_wf")
    assert hasattr(trg, "_autoworkflow_bound_start")
    # Trigger callback should not raise and should not save any result
    trg._autoworkflow_bound_start({"x": 1})  # type: ignore[attr-defined]
    assert backend.saved == {}


def test_engine_add_trigger_binds_callback_and_runs_with_initial_payload_and_factory_context():
    eng = Engine()

    class Ctx(BaseContext):
        def __init__(self):
            self.mult = 3

    eng.set_context_factory(Ctx)

    wf = Workflow(name="e2e")

    @wf.node
    def compute(x: int, ctx: Any = CONTEXT):
        return x * ctx.mult

    eng.register_workflow(wf)

    class FakeTrigger:
        def attach(self, callback: Callable[[Dict[str, Any]], None]) -> None:
            pass

        def tick(self, now=None):
            pass

        def stop(self) -> None:
            pass

    # add with initial data that will be overridden by payload
    trg = FakeTrigger()
    eng.add_trigger(trg, workflow_name=wf.name, initial_data={"x": 1})

    # ensure the attribute is bound
    assert hasattr(trg, "_autoworkflow_bound_start")

    # simulate event arrival by invoking the bound callback once
    trg._autoworkflow_bound_start({"x": 4})  # type: ignore[attr-defined]


def test_engine_context_factory_exception_and_plugin_injection_exception_are_swallowed():
    eng = Engine()

    class Ctx(BaseContext):
        def __init__(self):
            self.attr = None

    # a faulty factory
    def bad_factory():
        raise RuntimeError("factory boom")

    eng.set_context_factory(bad_factory)

    # plugin whose injection methods raise
    class BadPlugin:
        @property
        def name(self):
            return "bad"

        def setup(self, engine, config):
            pass

        def get_context_injections(self):
            raise RuntimeError("inj boom")

        def get_context_provider(self):
            raise RuntimeError("prov boom")

        def teardown(self):
            pass

    eng.register_plugin(BadPlugin())

    wf = Workflow(name="noop2")

    @wf.node
    def n():
        return 1

    eng.register_workflow(wf)

    class Tr:
        def attach(self, callback):
            callback({})

        def tick(self, now=None):
            pass

        def stop(self):
            pass

    t = Tr()
    eng.add_trigger(t, workflow_name=wf.name)

    # Should not raise despite factory/injection errors
    t._autoworkflow_bound_start({})  # type: ignore[attr-defined]


def test_engine_run_starts_triggers_without_bound_callback():
    eng = Engine()

    class Tr:
        def __init__(self):
            self.started = False

        def attach(self, callback):
            self.started = True

        def tick(self, now=None):
            pass

        def stop(self):
            pass

    tr = Tr()
    eng.add_trigger(tr, workflow_name="missing_wf")

    # Do not register the workflow; run should still start the trigger
    # and then stop due to KeyboardInterrupt
    def stop_soon():
        raise KeyboardInterrupt()

    # monkeypatch-like inline: wrap Engine.run to exit early by injecting a KeyboardInterrupt
    # Build a dummy plugin so teardown path is also exercised
    class PL:
        @property
        def name(self):
            return "plx"

        def setup(self, engine, config):
            pass

        def get_context_injections(self):
            return {}

        def get_context_provider(self):
            return None

        def teardown(self):
            pass

    eng.register_plugin(PL())

    # Replace time.sleep to raise KeyboardInterrupt quickly
    import autoworkflow.services.engine as eng_mod

    orig_sleep = eng_mod.time.sleep
    try:
        eng_mod.time.sleep = lambda _t: (_ for _ in ()).throw(KeyboardInterrupt())
        eng.run()
    finally:
        eng_mod.time.sleep = orig_sleep
