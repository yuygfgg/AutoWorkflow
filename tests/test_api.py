from __future__ import annotations

from typing import Any

from autoworkflow.api import flow, task, param, each, switch


def test_simple_flow_and_params():
    with flow("api-simple") as f:
        @task
        def fetch(source_id: str) -> dict[str, Any]:
            return {"id": source_id, "data": "raw"}

        @task
        def process(raw: dict[str, Any]) -> dict[str, Any]:
            return {**raw, "processed": True}

        @task
        def publish(result: dict[str, Any], destination: str = "default") -> str:
            return f"Published {result['id']} to {destination}"

        out = publish(
            process(fetch(param("source_id"))),
            destination=param("destination"),
        )

    res = f.run(initial_data={"source_id": "demo-1", "destination": "bucket-xyz"})
    assert res[out.node_id] == "Published demo-1 to bucket-xyz"


def test_map_short_syntax():
    with flow("api-map") as f:
        @task
        def get_items() -> list[str]:
            return ["a", "b", "c"]

        @task
        def upcase(item: str) -> str:
            return item.upper()

        mapped = each(upcase).over(get_items())

    leaves = f.run(return_mode="leaves")
    assert leaves[mapped.node_id] == ["A", "B", "C"]


def test_map_via_task_dot_map():
    with flow("api-map2") as f:
        @task
        def nums() -> list[int]:
            return [1, 2, 3]

        @task
        def double(x: int) -> int:
            return x * 2

        mapped = double.map(over=nums())

    leaves = f.run(return_mode="leaves")
    assert leaves[mapped.node_id] == [2, 4, 6]


def test_switch_when_otherwise_constants_and_tasks():
    with flow("api-switch") as f:
        @task
        def which() -> str:
            return "b"

        @task
        def A() -> str:
            return "A"

        @task
        def B() -> str:
            return "B"

        choice = switch(which()).when("a", A).when("b", B).otherwise("skip")

        @task
        def final(x: Any) -> Any:
            return x

        out = final(choice)

    res = f.run()
    assert res[out.node_id] == "B"


