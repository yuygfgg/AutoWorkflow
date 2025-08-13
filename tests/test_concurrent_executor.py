import time
from typing import Any, Dict
from unittest.mock import MagicMock, call
import pytest
import inspect

from autoworkflow.core import Workflow, BaseContext, CONTEXT
from autoworkflow.exceptions import ExecutionError
from autoworkflow.services.telemetry import InMemoryEventSink
from autoworkflow.services.telemetry import NodeEvent


def test_concurrent_executor_retry_exhaustion_raises():
    wf = Workflow(name="conc Exhaust")

    @wf.node
    def a():
        return 1

    @wf.node(retries=1)
    def b(x: int = wf.dep(a)):
        raise RuntimeError("always")

    with pytest.raises(ExecutionError) as ei:
        wf.run(executor="concurrent")
    assert getattr(ei.value, "node_id", None) == "b"


def test_concurrent_executor_map_empty_list_short_circuit():
    wf = Workflow(name="conc Map Empty")

    @wf.node
    def items():
        return []

    def _double(x: int = wf.map_dep(items)):
        return x * 2

    mapped = wf.map_node(name="mapped")(_double)

    res = wf.run(executor="concurrent", return_mode="leaves")
    assert res[mapped.node_id] == []


def test_concurrent_executor_case_with_callable_to_constant():
    wf = Workflow(name="conc Case Callable Constant")

    @wf.node
    def on():
        return 7

    @wf.node
    def side():
        # not used in chosen constant branch
        return 100

    # Callable predicate returns True, branch returns constant
    decision = wf.case(
        on=on,
        branches={
            (lambda v: v % 2 == 1): 42,
            (lambda v: v % 2 == 0): side,
        },
    )

    @wf.node
    def final(v=wf.dep(decision)):
        return v

    res = wf.run(executor="concurrent", return_mode="leaves")
    assert res["final"] == 42


def test_concurrent_case_deferred_target_without_deps_emits_events_and_skips_others():
    wf = Workflow(name="Conc-Case-Deferred-NoDeps")

    @wf.node
    def on():
        return "A"

    @wf.node
    def A():
        return 1

    @wf.node
    def B():
        return 2

    decision = wf.case(on=on, branches={"A": A, "B": B})

    @wf.node
    def final(v: Any = wf.dep(decision)):
        return v

    sink = InMemoryEventSink()
    run_id = "rid-deferred-nodeps"
    sink.on_run_started(run_id, wf.name)
    out = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    sink.on_run_finished(run_id, True)

    assert out["final"] == 1

    evs = sink.get_events()
    # Selected target scheduled+started via deferred scheduling path
    assert any(e.node_id == "A" and e.event == "scheduled" for e in evs)
    assert any(e.node_id == "A" and e.event == "started" for e in evs)
    # Non-selected should be marked skipped
    skipped_B = [
        e
        for e in evs
        if e.node_id == "B"
        and e.event == "finished"
        and (e.data or {}).get("status") == "skipped"
    ]
    assert skipped_B


def test_concurrent_map_accepts_tuple_input():
    wf = Workflow(name="Conc-Map-Tuple")

    @wf.node
    def items():
        return (1, 3)

    def _double(x: int = wf.map_dep(items)):
        return x * 2

    mapped = wf.map_node(name="double_t")(_double)
    out = wf.run(executor="concurrent", return_mode="leaves")
    assert out[mapped.node_id] == [2, 6]


def test_concurrent_case_resolving_to_another_case_with_constant():
    wf = Workflow(name="conc-nested-case-const")

    @wf.node
    def chooser_outer():
        return "inner"

    @wf.node
    def chooser_inner():
        return "const"

    inner_case = wf.case(on=chooser_inner, branches={"const": "constant_value"})

    outer_case = wf.case(on=chooser_outer, branches={"inner": inner_case})

    @wf.node
    def out(v=wf.dep(outer_case)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == "constant_value"


def test_concurrent_case_resolving_to_another_case_with_dependency():
    wf = Workflow(name="conc-nested-case-dep")

    @wf.node
    def chooser_outer():
        return "inner"

    @wf.node
    def chooser_inner():
        return "b"

    @wf.node
    def branch_a():
        return "A"

    @wf.node
    def branch_b():
        return "B"

    inner_case = wf.case(on=chooser_inner, branches={"a": branch_a, "b": branch_b})

    outer_case = wf.case(on=chooser_outer, branches={"inner": inner_case})

    @wf.node
    def out(v=wf.dep(outer_case)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == "B"


def test_concurrent_finalize_downstream_cases_telemetry_exceptions():
    """
    Covers exception handling paths in `finalize_downstream_cases` for both
    state backend and event sink failures.
    """
    wf = Workflow(name="conc-finalize-downstream-exc")

    @wf.node
    def trigger():
        return "go"

    @wf.node
    def slow_node(t=wf.dep(trigger)):
        time.sleep(0.05)
        return "OK"

    # Create a chain: case2 depends on case1, which depends on slow_node
    case1 = wf.case(on=trigger, branches={"go": slow_node})
    case2 = wf.case(on=case1, branches={"OK": "done"})

    @wf.node
    def final(res=wf.dep(case2)):
        return res

    mock_state_backend = MagicMock()
    mock_state_backend.save_result.side_effect = IOError("state backend error")

    mock_event_sink = MagicMock()
    mock_event_sink.on_node_event.side_effect = IOError("event sink error")

    # The workflow should complete without crashing, as exceptions are caught.
    result = wf.run(
        executor="concurrent",
        run_id="test-finalize-exc",
        state_backend=mock_state_backend,
        event_sink=mock_event_sink,
    )

    assert result[final.node_id] == "done"
    # Verify that save_result was called for the cases, triggering the error
    assert mock_state_backend.save_result.called
    assert mock_event_sink.on_node_event.called


class FaultyStateBackend:
    def save_result(self, run_id: str, node_id: str, result):
        # Let some succeed to ensure we test various paths
        if node_id in ["a", "chooser_outer"]:
            return
        raise IOError("Disk is full")


class FaultyEventSink:
    def on_node_event(self, event: NodeEvent):
        if event.node_id in ["a", "chooser_outer"]:
            return
        raise IOError("Network error")


def test_faulty_telemetry_and_state_backend_are_handled():
    """
    Ensures that exceptions from the state backend or event sink are caught
    and do not crash the concurrent executor.
    """
    wf = Workflow(name="faulty-telemetry")

    @wf.node
    def a():
        return [1]  # to trigger map node

    def _process(item=wf.map_dep(a)):
        return item * 2

    map_node = wf.map_node(name="map_node")(_process)

    @wf.node
    def chooser_outer():
        return "inner"

    @wf.node
    def chooser_inner(items=wf.dep(map_node)):
        return "const" if sum(items) > 1 else "target"

    @wf.node
    def target_node():
        return "TARGET"

    inner_case = wf.case(
        on=chooser_inner,
        branches={
            "const": "CONSTANT",
            "target": target_node,
        },
    )

    outer_case = wf.case(on=chooser_outer, branches={"inner": inner_case})

    @wf.node
    def final(res=wf.dep(outer_case)):
        return res

    # This should run without crashing despite IOErrors from telemetry
    result = wf.run(
        executor="concurrent",
        run_id="faulty-run",
        state_backend=FaultyStateBackend(),
        event_sink=FaultyEventSink(),
    )

    # Check that the workflow still completes correctly
    assert result[final.node_id] == "CONSTANT"


def test_graph_dependencies_exception_is_handled(monkeypatch):
    """
    Covers a niche case where accessing graph.dependencies for debug logging
    might fail. The executor should handle this gracefully.
    """
    wf = Workflow(name="graph-dep-exc")

    @wf.node
    def a():
        return 1

    class BadDict(dict):
        def items(self):
            # Only fail if called from the executor's logging code
            caller_frame = inspect.currentframe().f_back  # type: ignore[union-attr]
            if "concurrent.py" in caller_frame.f_code.co_filename:  # type: ignore[union-attr]
                raise RuntimeError("mocked error")
            return super().items()

    original_build_graph = wf._build_graph

    def new_build_graph():
        graph = original_build_graph()
        graph.dependencies = BadDict(graph.dependencies)
        return graph

    monkeypatch.setattr(wf, "_build_graph", new_build_graph)

    # The exception in the debug log should be caught and not crash the run
    res = wf.run(executor="concurrent")
    assert res["a"] == 1


def test_concurrent_deferred_node_disabled_by_constant_case():
    """
    Covers the scenario where a deferred node is disabled because its `case`
    resolves to a constant.
    """
    wf = Workflow(name="conc-deferred-node-disabled")

    @wf.node
    def chooser():
        time.sleep(0.05)
        return "CONST"

    @wf.node
    def deferred_branch():
        raise AssertionError("This branch should not be executed")

    case_node = wf.case(
        on=chooser,
        branches={
            "A": deferred_branch,
            "CONST": "constant_value",
        },
    )

    @wf.node
    def final(res=wf.dep(case_node)):
        return res

    mock_event_sink = MagicMock()
    result = wf.run(
        executor="concurrent",
        run_id="test-deferred-disabled",
        event_sink=mock_event_sink,
    )

    assert result[final.node_id] == "constant_value"

    # Verify that the deferred branch was marked as skipped
    skipped_event_found = False
    for call_args in mock_event_sink.on_node_event.call_args_list:
        event = call_args[0][0]
        if (
            event.node_id == "deferred_branch"
            and event.event == "finished"
            and event.data.get("status") == "skipped"
        ):
            skipped_event_found = True
            break
    assert skipped_event_found, "Expected a 'skipped' event for deferred_branch"


def test_concurrent_deferred_nested_case_as_branch():
    """
    Covers the scheduling of a deferred branch that is itself a `case` node.
    """
    wf = Workflow(name="conc-deferred-nested-case")

    @wf.node
    def chooser():
        time.sleep(0.05)
        return "A"

    @wf.node
    def inner_chooser():
        return "X"

    @wf.node
    def branch_x():
        return "Branch X"

    # This case is a deferred branch of the outer case
    inner_case = wf.case(on=inner_chooser, branches={"X": branch_x})

    outer_case = wf.case(on=chooser, branches={"A": inner_case})

    @wf.node
    def final(res=wf.dep(outer_case)):
        return res

    result = wf.run(executor="concurrent")
    assert result[final.node_id] == "Branch X"


def test_concurrent_deferred_case_scheduling():
    """
    Covers the scheduling of a deferred `case` node that becomes ready
    after its dependencies are met.
    """
    wf = Workflow(name="conc-deferred-case")

    @wf.node
    def gate():
        time.sleep(0.05)
        return "A"

    @wf.node
    def branch_a():
        return "Branch A"

    # This case node is deferred because its `on` dependency (`gate`) is slow
    deferred_case = wf.case(on=gate, branches={"A": branch_a})

    @wf.node
    def final(res=wf.dep(deferred_case)):
        return res

    result = wf.run(executor="concurrent")
    assert result[final.node_id] == "Branch A"


def test_concurrent_deferred_case_with_context_dependency():
    """
    Covers the scheduling of a deferred `case` node that has a `CONTEXT`
    dependency.
    """
    wf = Workflow(name="conc-deferred-case-with-context")

    class Ctx(BaseContext):
        val: int = 0

    @wf.node
    def chooser():
        time.sleep(0.05)
        return "A"

    @wf.node
    def deferred_case_on(ctx: Ctx = CONTEXT):  # type: ignore[no-untyped-def]
        return "X" if ctx.val > 0 else "Y"

    @wf.node
    def branch_x():
        return "Branch X"

    @wf.node
    def branch_y():
        return "Branch Y"

    deferred_case = wf.case(
        on=deferred_case_on, branches={"X": branch_x, "Y": branch_y}
    )

    case_node = wf.case(on=chooser, branches={"A": deferred_case})

    @wf.node
    def final(res=wf.dep(case_node)):
        return res

    context = Ctx()
    context.val = 1
    result = wf.run(executor="concurrent", context=context)

    assert result[final.node_id] == "Branch X"


def test_concurrent_deferred_case_as_branch_target():
    """
    Covers the scheduling of a deferred `case` node that is also a branch
    target of another `case` node.
    """
    wf = Workflow(name="conc-deferred-case-as-target")

    @wf.node
    def chooser():
        time.sleep(0.05)
        return "A"

    @wf.node
    def inner_chooser():
        return "X"

    @wf.node
    def branch_x():
        return "Branch X"

    # This case is a deferred target of the outer case
    inner_case = wf.case(on=inner_chooser, branches={"X": branch_x})

    outer_case = wf.case(on=chooser, branches={"A": inner_case})

    @wf.node
    def final(res=wf.dep(outer_case)):
        return res

    result = wf.run(executor="concurrent")
    assert result[final.node_id] == "Branch X"


def test_concurrent_deferred_branch_scheduling():
    """
    Covers the scheduling of deferred branches of a `case` node.
    """
    wf = Workflow(name="conc-deferred-branches")

    class Ctx(BaseContext):
        val: int = 0

    @wf.node
    def chooser():
        time.sleep(0.05)
        return "B"

    @wf.node
    def branch_a(ctx: Ctx = CONTEXT):  # type: ignore[no-untyped-def]
        ctx.val = 1
        return "A"

    @wf.node
    def branch_b(ctx: Ctx = CONTEXT):  # type: ignore[no-untyped-def]
        ctx.val = 2
        return "B"

    @wf.node
    def branch_c(ctx: Ctx = CONTEXT):  # type: ignore[no-untyped-def]
        ctx.val = 3
        return "C"

    case_node = wf.case(
        on=chooser,
        branches={
            "A": branch_a,
            "B": branch_b,
            "C": branch_c,
        },
    )

    @wf.node
    def final(res=wf.dep(case_node)):
        return res

    context = Ctx()
    result = wf.run(executor="concurrent", context=context)

    assert result[final.node_id] == "B"
    assert context.val == 2


def test_node_retries_on_failure():
    wf = Workflow(name="retry-test")

    mock_func = MagicMock()
    mock_func.side_effect = [Exception("fail"), Exception("fail"), "success"]

    @wf.node(retries=2)
    def flaky_node():
        return mock_func()

    @wf.node
    def out(v=wf.dep(flaky_node)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == "success"
    assert mock_func.call_count == 3


def test_map_with_none_iterable():
    wf = Workflow(name="map-none-iterable")

    @wf.node
    def provide_none():
        return None

    def _process(item=wf.map_dep(provide_none)):
        raise AssertionError("process should not be called for None iterable")

    process_node = wf.map_node(name="process_node")(_process)

    @wf.node
    def out(v=wf.dep(process_node)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == []


def test_map_with_non_list_iterable():
    wf = Workflow(name="map-non-list-iterable")

    @wf.node
    def provide_iterable():
        return 5  # Not a list

    def _process(item=wf.map_dep(provide_iterable)):
        return item * 2

    process_node = wf.map_node(name="process_node")(_process)

    @wf.node
    def out(v=wf.dep(process_node)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == [10]


def test_case_resolving_to_another_case_with_constant():
    wf = Workflow(name="nested-case-const")

    @wf.node
    def chooser_outer():
        return "inner"

    @wf.node
    def chooser_inner():
        return "const"

    inner_case = wf.case(on=chooser_inner, branches={"const": "constant_value"})

    outer_case = wf.case(on=chooser_outer, branches={"inner": inner_case})

    @wf.node
    def out(v=wf.dep(outer_case)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == "constant_value"


def test_case_skips_unselected_branches():
    wf = Workflow(name="case-skip")
    mock_event_sink = MagicMock()

    @wf.node
    def chooser():
        return "a"

    @wf.node
    def branch_a():
        return "A"

    @wf.node
    def branch_b():
        raise AssertionError("branch_b should not be executed")

    case_node = wf.case(
        on=chooser,
        branches={
            "a": branch_a,
            "b": branch_b,
        },
    )

    @wf.node
    def out(v=wf.dep(case_node)):
        return v

    wf.run(executor="concurrent", run_id="skip-run", event_sink=mock_event_sink)

    # Verify that branch_b was marked as skipped
    skipped_event_found = False
    for call_args in mock_event_sink.on_node_event.call_args_list:
        event = call_args[0][0]
        if (
            event.node_id == "branch_b"
            and event.event == "finished"
            and event.data.get("status") == "skipped"
        ):
            skipped_event_found = True
            break
    assert skipped_event_found, "Expected a 'skipped' event for branch_b"


def test_case_resolving_to_another_case_with_dependency():
    wf = Workflow(name="nested-case-dep")

    @wf.node
    def chooser_outer():
        return "inner"

    @wf.node
    def chooser_inner():
        return "b"

    @wf.node
    def branch_a():
        return "A"

    @wf.node
    def branch_b():
        return "B"

    inner_case = wf.case(
        on=chooser_inner,
        branches={
            "a": branch_a,
            "b": branch_b,
        },
    )

    outer_case = wf.case(on=chooser_outer, branches={"inner": inner_case})

    @wf.node
    def out(v=wf.dep(outer_case)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == "B"


def test_state_backend_and_event_sink_integration():
    wf = Workflow(name="telemetry-integration")

    mock_state_backend = MagicMock()
    mock_event_sink = MagicMock()

    @wf.node
    def node_a():
        return "A"

    @wf.node
    def node_b(a=wf.dep(node_a)):
        return a + "B"

    @wf.node
    def out(b=node_b):
        return b

    wf.run(
        executor="concurrent",
        run_id="test-run",
        state_backend=mock_state_backend,
        event_sink=mock_event_sink,
    )

    # Check state backend calls
    mock_state_backend.save_result.assert_has_calls(
        [
            call("test-run", "node_a", "A"),
            call("test-run", "node_b", "AB"),
            call("test-run", "out", "AB"),
        ],
        any_order=True,
    )

    # Check event sink calls
    assert mock_event_sink.on_node_event.call_count > 0


def test_idle_loop_coverage():
    wf = Workflow(name="idle-loop")

    @wf.node
    def slow_node():
        time.sleep(0.1)
        return "slow"

    @wf.node
    def dependent_node(s=wf.dep(slow_node)):
        return s + "-fast"

    res = wf.run(executor="concurrent")
    assert res["dependent_node"] == "slow-fast"


def test_execution_error_on_max_retries():
    wf = Workflow(name="max-retries-fail")

    mock_func = MagicMock()
    mock_func.side_effect = Exception("persistent failure")

    @wf.node(retries=1)
    def always_fail():
        return mock_func()

    with pytest.raises(ExecutionError) as excinfo:
        wf.run(executor="concurrent")

    assert "failed after 2 attempt(s)" in str(excinfo.value)
    assert mock_func.call_count == 2


def test_case_with_predicate_exception_is_ignored_concurrent():
    wf = Workflow(name="case-predicate-exc-conc")

    @wf.node
    def val():
        return 3

    def bad_predicate(_):
        raise RuntimeError("boom")

    decision = wf.case(
        on=val,
        branches={
            bad_predicate: 111,  # should be ignored due to exception
            3: 222,  # matched equality
        },
    )

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == 222


def test_case_no_match_returns_none_concurrent():
    wf = Workflow(name="case-no-match-conc")

    @wf.node
    def val():
        return "c"

    decision = wf.case(
        on=val,
        branches={
            "a": 1,
            "b": 2,
        },
    )

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] is None


def test_map_with_scalar_input_concurrent():
    wf = Workflow(name="map-scalar-conc")

    @wf.node
    def provide_scalar():
        return 5

    def _process(item=wf.map_dep(provide_scalar)):
        return item * 2

    process_node = wf.map_node(name="process_node")(_process)

    @wf.node
    def out(v=wf.dep(process_node)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] == [10]


def _count(events: list[NodeEvent], node_id: str, kind: str) -> int:
    return sum(1 for e in events if e.node_id == node_id and e.event == kind)


def test_concurrent_case_deferred_target_emits_scheduled_started_and_case_finished():
    wf = Workflow(name="Conc-Case-Deferred-Events")

    @wf.node
    def on():
        # fast decision so target is scheduled after the case resolves
        return 1

    @wf.node
    def gate():
        time.sleep(0.03)
        return True

    @wf.node
    def slow_target(_g: bool = wf.dep(gate)):
        time.sleep(0.02)
        return "OK"

    # Another branch target that's not selected
    @wf.node
    def other():
        return "X"

    decision = wf.case(on=on, branches={1: slow_target, 2: other})

    @wf.node
    def final(v: Any = wf.dep(decision)):
        return v

    sink = InMemoryEventSink()
    run_id = "rid-deferred-events"
    sink.on_run_started(run_id, wf.name)
    out = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    sink.on_run_finished(run_id, True)
    assert out["final"] == "OK"

    evs = sink.get_events()
    # case meta-node should be marked scheduled and finished
    assert _count(evs, decision.node_id, "scheduled") >= 1
    assert _count(evs, decision.node_id, "finished") >= 1
    # selected target should be scheduled and started after case resolves
    assert _count(evs, slow_target.node_id, "scheduled") >= 1
    assert _count(evs, slow_target.node_id, "started") >= 1


def test_concurrent_case_deferred_target_ready_before_case():
    wf = Workflow(name="Conc-Case-Deferred-Ready")

    @wf.node
    def on():
        time.sleep(0.03)
        return 1

    @wf.node
    def t_sel():
        return "A"

    @wf.node
    def t_other():
        return "B"

    decision = wf.case(on=on, branches={1: t_sel, 2: t_other})

    @wf.node
    def final(v=wf.dep(decision)):
        return v

    sink = InMemoryEventSink()
    run_id = "rid-case-deferred-ready"
    sink.on_run_started(run_id, wf.name)
    out = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    sink.on_run_finished(run_id, True)

    assert out["final"] == "A"
    evs = sink.get_events()
    assert _count(evs, decision.node_id, "scheduled") >= 1
    assert _count(evs, decision.node_id, "finished") >= 1
    assert _count(evs, t_sel.node_id, "scheduled") >= 1
    assert _count(evs, t_sel.node_id, "started") >= 1
    assert _count(evs, t_other.node_id, "started") == 0
    assert _count(evs, t_other.node_id, "finished") >= 1


def test_concurrent_case_constant_then_late_ready_branch_targets_skipped_in_scheduler():
    wf = Workflow(name="Conc-Case-Const-Late-Targets")

    @wf.node
    def on():
        return 0  # choose constant

    @wf.node
    def gate():
        time.sleep(0.02)  # make branch targets become ready after case resolves
        return True

    @wf.node
    def t1(_g: bool = wf.dep(gate)):
        return "T1"

    @wf.node
    def t2(_g: bool = wf.dep(gate)):
        return "T2"

    decision = wf.case(on=on, branches={0: 42, 1: t1, 2: t2})

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    sink = InMemoryEventSink()
    run_id = "rid-case-const-late-targets"
    sink.on_run_started(run_id, wf.name)
    res = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    sink.on_run_finished(run_id, True)

    assert res["out"] == 42
    evs = sink.get_events()
    # t1/t2 should not start; they are skipped by scheduler after case resolved to constant
    assert _count(evs, t1.node_id, "started") == 0
    assert _count(evs, t2.node_id, "started") == 0
    # case scheduled and finished
    assert _count(evs, decision.node_id, "scheduled") >= 1
    assert _count(evs, decision.node_id, "finished") >= 1


def test_concurrent_normal_nodes_emit_scheduled_events():
    wf = Workflow(name="Conc-Normal-Scheduled")

    @wf.node
    def a():
        return 1

    @wf.node
    def b(x: int = wf.dep(a)):
        return x + 1

    sink = InMemoryEventSink()
    run_id = "rid-normal-scheduled"
    sink.on_run_started(run_id, wf.name)
    out = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    sink.on_run_finished(run_id, True)

    assert out["b"] == 2
    evs = sink.get_events()
    # both nodes should have at least one scheduled event
    assert _count(evs, "a", "scheduled") >= 1
    assert _count(evs, "b", "scheduled") >= 1


def test_concurrent_map_scalar_via_const_binding_and_events_and_state_backend():
    wf = Workflow(name="Conc-Map-Scalar-Events")

    # Create a map node without dependencies; supply const binding as scalar
    def _double_impl(x: int = 0):
        return x * 2

    mapped = wf.map_node(name="double2")(_double_impl)
    bound = wf.bind(mapped, x=7)

    @wf.node
    def take(vs: list[int] = wf.dep(bound)):
        return vs

    class _Backend:
        def __init__(self):
            self.saved: Dict[tuple[str, str], Any] = {}

        def save_result(self, run_id: str, node_id: str, result: Any) -> None:
            self.saved[(run_id, node_id)] = result

    sink = InMemoryEventSink()
    backend = _Backend()
    run_id = "rid-map-scalar"

    sink.on_run_started(run_id, wf.name)
    out = wf.run(
        executor="concurrent",
        return_mode="leaves",
        run_id=run_id,
        event_sink=sink,
        state_backend=backend,
    )
    sink.on_run_finished(run_id, True)

    assert out["take"] == [14]
    # ensure map node or bound id and final were persisted
    saved_keys = {nid for (_rid, nid) in backend.saved if _rid == run_id}
    assert take.node_id in saved_keys
    assert mapped.node_id in saved_keys or bound.node_id in saved_keys

    evs = sink.get_events()
    # bound map node should be scheduled and finished; each item runs as map_item not explicitly surfaced, but node finished event exists
    assert (
        _count(evs, mapped.node_id, "scheduled") >= 1
        or _count(evs, bound.node_id, "scheduled") >= 1
    )
    assert (
        _count(evs, mapped.node_id, "finished") >= 1
        or _count(evs, bound.node_id, "finished") >= 1
    )


def test_concurrent_map_empty_list_persists_immediately():
    wf = Workflow(name="Conc-Map-Empty-Backend")

    @wf.node
    def items():
        return []

    def _double(x: int = wf.map_dep(items)):
        return x * 2

    mapped = wf.map_node(name="mapped_empty")(_double)

    class _Backend:
        def __init__(self):
            self.saved: Dict[tuple[str, str], Any] = {}

        def save_result(self, run_id: str, node_id: str, result: Any) -> None:
            self.saved[(run_id, node_id)] = result

    backend = _Backend()
    out = wf.run(
        executor="concurrent",
        return_mode="leaves",
        run_id="rid-empty",
        state_backend=backend,
    )
    assert out[mapped.node_id] == []
    saved_keys = {nid for (rid, nid) in backend.saved.keys() if rid == "rid-empty"}
    assert mapped.node_id in saved_keys


def test_concurrent_dep_access_path_key_and_attr():
    from dataclasses import dataclass

    wf = Workflow(name="Conc-Access-Path")

    @wf.node
    def a():
        return {"inner": {"value": 5}}

    @wf.node
    def b(x: int = wf.dep(a)["inner"]["value"]):
        return x * 2

    @dataclass
    class Obj:
        y: int

    @wf.node
    def c():
        return Obj(7)

    @wf.node
    def d(v: int = wf.dep(c).y):
        return v + 1

    res = wf.run(executor="concurrent", return_mode="leaves")
    assert res["b"] == 10
    assert res["d"] == 8


def test_concurrent_case_callable_raises_and_no_match_skips_targets_and_returns_none():
    from autoworkflow.services.telemetry import InMemoryEventSink

    wf = Workflow(name="Conc-Case-NoMatch")

    @wf.node
    def on():
        return 3

    @wf.node
    def t1():
        return "T1"

    def bad_cond(v: int) -> bool:
        raise RuntimeError("boom")

    def false_cond(v: int) -> bool:
        return False

    decision = wf.case(on=on, branches={bad_cond: t1, false_cond: t1})

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    sink = InMemoryEventSink()
    run_id = "rid-case-nomatch"
    sink.on_run_started(run_id, wf.name)
    res = wf.run(
        executor="concurrent", return_mode="leaves", run_id=run_id, event_sink=sink
    )
    sink.on_run_finished(run_id, True)

    assert res["out"] is None
    evs = sink.get_events()
    # t1 should be skipped (never started)
    assert _count(evs, t1.node_id, "started") == 0
    assert _count(evs, t1.node_id, "finished") >= 1
    # case node should be scheduled and finished
    assert _count(evs, decision.node_id, "scheduled") >= 1
    assert _count(evs, decision.node_id, "finished") >= 1


def test_concurrent_retry_eventual_success():
    wf = Workflow(name="Conc-Retry-Success")

    attempts = {"n": 0}

    @wf.node
    def a():
        return 1

    @wf.node(retries=2)
    def b(x: int = wf.dep(a)):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("fail once")
        return x + 10

    res = wf.run(executor="concurrent", return_mode="leaves")
    assert res["b"] == 11
    assert attempts["n"] >= 2


def test_concurrent_map_order_preserved_with_concurrency():
    import time as _t

    wf = Workflow(name="Conc-Map-Order")

    @wf.node
    def items():
        return [3, 1, 2]

    def _slow_double(x: int = wf.map_dep(items)):
        _t.sleep(0.01 * (4 - x))  # reverse order of completion
        return x * 10

    mapped = wf.map_node(name="mapped_order")(_slow_double)

    @wf.node
    def take(vs=wf.dep(mapped)):
        return vs

    out = wf.run(executor="concurrent", return_mode="leaves")
    assert out["take"] == [30, 10, 20]


def test_concurrent_context_injection():
    from autoworkflow.core import CONTEXT, BaseContext

    class Ctx(BaseContext):
        def __init__(self):
            self.val = 4

    wf = Workflow(name="Conc-Context")

    @wf.node
    def a(ctx=CONTEXT):  # type: ignore[no-untyped-def]
        v = getattr(ctx, "val", 0)
        return 1 + v

    res = wf.run(executor="concurrent", context=Ctx(), return_mode="leaves")
    assert res["a"] == 5


def test_concurrent_case_target_path_persists_case_result():

    wf = Workflow(name="Conc-Case-Persist")

    @wf.node
    def on():
        return 1

    @wf.node
    def chosen():
        return "OK"

    @wf.node
    def other():
        return "X"

    decision = wf.case(on=on, branches={1: chosen, 2: other})

    @wf.node
    def final(v: Any = wf.dep(decision)):
        return v

    class _Backend:
        def __init__(self):
            self.saved: Dict[tuple[str, str], Any] = {}

        def save_result(self, run_id: str, node_id: str, result: Any) -> None:
            self.saved[(run_id, node_id)] = result

    backend = _Backend()
    run_id = "rid-case-persist"
    out = wf.run(
        executor="concurrent",
        return_mode="leaves",
        run_id=run_id,
        state_backend=backend,
    )
    assert out["final"] == "OK"

    saved_keys = {nid for (rid, nid) in backend.saved if rid == run_id}
    # case node and final should be persisted (selected target may or may not depending on timing)
    assert decision.node_id in saved_keys
    assert "final" in saved_keys
