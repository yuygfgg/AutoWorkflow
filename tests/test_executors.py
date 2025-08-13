import time
from typing import Any, Dict

from autoworkflow.core import Workflow


class _FakeStateBackend:
    def __init__(self):
        self.saved: Dict[tuple[str, str], Any] = {}

    def save_result(self, run_id: str, node_id: str, result: Any) -> None:
        self.saved[(run_id, node_id)] = result


def test_concurrent_executor_case_target_already_ready():
    wf = Workflow(name="conc Case Target Ready")

    # Make 'on' slow so branch targets finish first
    @wf.node
    def on():
        time.sleep(0.03)
        return "A"

    executed: Dict[str, int] = {"A": 0, "B": 0}

    @wf.node
    def branch_a():
        executed["A"] += 1
        return "A"

    @wf.node
    def branch_b():
        executed["B"] += 1
        return "B"

    decision = wf.case(on=on, branches={"A": branch_a, "B": branch_b})

    @wf.node
    def final(v: str | None = wf.dep(decision)):
        return v

    out = wf.run(executor="concurrent", return_mode="leaves")
    # Expect chosen branch value propagated
    assert out["final"] == "A"
    # Both branches may run (they are independent), but at least A must run
    assert executed["A"] >= 1


def test_concurrent_executor_case_target_pending_then_finalize():
    wf = Workflow(name="conc Case Target Pending")

    # Fast 'on' so decision occurs quickly
    @wf.node
    def on():
        return 1

    @wf.node
    def gate():
        time.sleep(0.03)
        return True

    @wf.node
    def slow_target(_g: bool = wf.dep(gate)):
        return "T"

    @wf.node
    def other():
        return "X"

    decision = wf.case(on=on, branches={1: slow_target, 2: other})

    @wf.node
    def final(v: str | None = wf.dep(decision)):
        return v

    out = wf.run(executor="concurrent", return_mode="leaves")
    assert out["final"] == "T"


def test_concurrent_executor_map_scalar_and_state_backend_saved():
    wf = Workflow(name="conc Map Scalar+State")

    @wf.node
    def single():
        return [5]

    def _double(x: int = wf.map_dep(single)):
        return x * 2

    mapped = wf.map_node(name="double")(_double)

    # Bind providing const for the same param name to exercise const_bindings path safely
    bound = wf.bind(mapped, x=999)

    @wf.node
    def take(vs: list[int] = wf.dep(bound)):
        return vs

    backend = _FakeStateBackend()
    run_id = "rid-1"
    out = wf.run(
        executor="concurrent",
        return_mode="leaves",
        state_backend=backend,
        run_id=run_id,
    )
    assert out["take"] == [10]

    # Ensure map node and final were persisted
    saved_keys = {nid for (_rid, nid) in backend.saved.keys() if _rid == run_id}
    assert mapped.node_id in saved_keys or bound.node_id in saved_keys
    assert "take" in saved_keys


def test_concurrent_executor_const_bindings_with_initial_data_precedence_and_state_backend():
    wf = Workflow(name="conc Const Bindings + State")

    @wf.node
    def add(x: int = 0, y: int = 0):
        return x + y

    bound = wf.bind(add, y=3)

    backend = _FakeStateBackend()
    out = wf.run(
        executor="concurrent",
        return_mode="leaves",
        initial_data={"x": 2},
        state_backend=backend,
        run_id="rid-2",
    )
    assert out[bound.node_id] == 5

def test_concurrent_executor_skips_node_if_result_in_initial_data():
    wf = Workflow(name="Conc-InitialData-Skip")

    @wf.node
    def a():
        return 2

    # Provide initial result for node 'a'; executor should mark it done without executing
    res = wf.run(executor="concurrent", return_mode="leaves", initial_data={"a": 1})
    assert res["a"] == 1


def test_concurrent_map_accepts_set_input():
    wf = Workflow(name="Conc-Map-Set")

    @wf.node
    def make_set():
        return {1, 2}

    def _double(x: int = wf.map_dep(make_set)):
        return x * 2

    mapped = wf.map_node(name="double_set")(_double)
    out = wf.run(executor="concurrent", return_mode="leaves")
    # Order is not guaranteed for sets; check as a multiset
    assert sorted(out[mapped.node_id]) == [2, 4]
