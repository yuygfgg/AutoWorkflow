import time
from typing import Any

from autoworkflow.core import Workflow
from autoworkflow.services.engine import Engine


class _StateBackend:
    def __init__(self) -> None:
        self.saved = {}  # (run_id, node_id) -> result

    def save_result(self, run_id: str, node_id: str, result: Any) -> None:
        self.saved[(run_id, node_id)] = result


def _count_results(backend: _StateBackend, node_id: str) -> int:
    return sum(1 for (rid, nid) in backend.saved.keys() if nid == node_id)


def test_engine_event_runs_execute_concurrently():
    # Configure engine to allow sufficient parallelism
    eng = Engine(config={"event_max_workers": 8})

    backend = _StateBackend()

    wf = Workflow(name="parallel_runs")

    @wf.node
    def slow(delay: float) -> float:
        time.sleep(delay)
        return delay

    eng.register_workflow(wf)
    eng.set_state_backend(backend)

    class FakeTrigger:
        def attach(self, callback):
            # no-op for this test; we'll use the bound callback directly
            pass

        def tick(self, now=None):
            pass

    trg = FakeTrigger()
    eng.add_trigger(trg, workflow_name=wf.name)

    # Fire multiple events quickly; runs should overlap
    per_run_delay = 0.2
    num_events = 4

    start = time.time()
    cb = trg._autoworkflow_bound_start  # type: ignore[attr-defined]
    for _ in range(num_events):
        cb({"delay": per_run_delay})

    # Wait for all runs to finish (results saved in backend)
    deadline = time.time() + 3.0
    while _count_results(backend, "slow") < num_events and time.time() < deadline:
        time.sleep(0.01)

    elapsed = time.time() - start

    # If runs were strictly serial, elapsed ~= num_events * per_run_delay.
    # With concurrency, elapsed should be significantly less.
    assert (
        elapsed < per_run_delay * (num_events / 2.0) + 0.05
    ), f"Runs not concurrent enough: elapsed={elapsed:.3f}s"
