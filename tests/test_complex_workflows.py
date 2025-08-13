import time
from dataclasses import dataclass
from typing import Any

from autoworkflow.core import Workflow, BaseContext, CONTEXT


def test_retry_bind_case_context_and_map_concurrent():
    wf = Workflow(name="Complex Case+Map+Bind+Retry Concurrent")

    # Context with a threshold factor
    class Ctx(BaseContext):
        def __init__(self, factor: int):
            self.factor = factor

    @wf.node
    def numbers():
        return [1, 2, 3, 4]

    # map: square each number (also simulate a tiny delay)
    def _square_impl(n: int = wf.map_dep(numbers)):
        time.sleep(0.01)
        return n * n

    squared = wf.map_node(name="square")(_square_impl)

    @wf.node
    def total(vals: list[int] = wf.dep(squared)):
        return sum(vals)

    # Inject context to compute threshold
    @wf.node
    def threshold(ctx: Any = CONTEXT):
        return ctx.factor * 5  # factor=3 => 15

    @wf.node
    def is_high(x: int = wf.dep(total), t: int = wf.dep(threshold)):
        return x > t

    executed = {"high": 0, "low": 0}

    @wf.node
    def mark_high(prefix: str = "", suffix: str = ""):
        executed["high"] += 1
        return f"{prefix}{suffix}"

    @wf.node
    def mark_low(prefix: str = "", suffix: str = ""):
        executed["low"] += 1
        return f"{prefix}{suffix}"

    # branches with binding constants
    case_node = wf.case(
        on=is_high,
        branches={
            True: (mark_high, {"prefix": "HIGH", "suffix": "!"}),
            False: (mark_low, {"prefix": "LOW", "suffix": "!"}),
        },
    )

    # flaky node with retry after the decision, to ensure retry logic integrates well
    attempts = {"cnt": 0}

    @wf.node(retries=1)
    def flaky(value: str | None = wf.dep(case_node)):
        attempts["cnt"] += 1
        if attempts["cnt"] == 1:
            raise RuntimeError("transient failure")
        return f"ok:{value}"

    @wf.node
    def final(result: str | None = wf.dep(flaky)):
        return result

    res = wf.run(executor="concurrent", context=Ctx(factor=3), return_mode="leaves")
    # numbers squares: [1,4,9,16] sum=30; threshold=15 => True => HIGH
    assert res.get("final") == "ok:HIGH!"
    # At least the chosen branch must execute (base node may have run too)
    assert executed["high"] >= 1
    assert attempts["cnt"] >= 2  # retried once


def test_attribute_and_item_access_concurrent():
    wf = Workflow(name="Attr/Item Access Concurrent")

    @dataclass
    class Outer:
        inner: dict

    @wf.node
    def make_outer():
        return Outer(inner={"y": 7})

    # Use access path: .inner["y"]
    # We bind via a new node to keep typing simple in tests
    @wf.node
    def picked(v: int = wf.dep(make_outer).inner["y"]):
        return v

    res = wf.run(executor="concurrent", return_mode="leaves")
    assert res.get("picked") == 7


def test_parallel_map_runs_and_preserves_order_concurrent():
    wf_conc = Workflow(name="Map Speed Concurrent Only")

    @wf_conc.node
    def items():
        return list(range(8))

    def _slow_double(x: int = wf_conc.map_dep(items)):
        time.sleep(0.05)
        return x * 2

    wf_conc.map_node(name="slow_double")(_slow_double)

    res_conc = wf_conc.run(executor="concurrent", return_mode="leaves")
    assert res_conc.get("slow_double") == [i * 2 for i in range(8)]
