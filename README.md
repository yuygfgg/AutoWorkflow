## AutoWorkflow

[![codecov](https://codecov.io/gh/yuygfgg/AutoWorkflow/graph/badge.svg?token=OXK7R357AX)](https://codecov.io/gh/yuygfgg/AutoWorkflow)

Declarative, type-safe workflow engine for Python. Define tasks as plain functions, wire dependencies declaratively, and run them with a concurrent executor. For long-running automations, use the Engine with pluggable Triggers, Plugins, and an optional Web UI.

### Why AutoWorkflow?

- **Clear Python functions**: tasks are normal functions; dependencies are explicit.
- **Type-friendly primitives**: helpers make dependencies and map operations obvious to readers and tools.
- **Concurrent execution**: run with a thread pool executor.
- **Event-driven services**: Triggers and Plugins turn workflows into always-on automation.

---

## Installation

Requirements:

- Python 3.10+

```bash
pip install git+https://github.com/yuygfgg/autoworkflow
```

---

## Two Complementary APIs

AutoWorkflow provides two first-class ways to define workflows. Both compile to the same execution engine and can be used independently.

- **Workflow API (declarative)**: register nodes on a `Workflow` and express dependencies with `workflow.dep(...)`.
- **Flow/Task API (functional composition)**: compose tasks by calling them like regular functions within a `flow(...)` context.

Choose the style that best matches your codebase. Examples below show feature parity.

---

## Quick Start

### Workflow API (declarative)

```python
from autoworkflow import Workflow

workflow = Workflow(name="My First Workflow")

@workflow.node
def fetch(source_id: str):
    return {"id": source_id, "data": "raw"}

@workflow.node
def process(raw = workflow.dep(fetch)):
    return {**raw, "processed": True}

@workflow.node
def publish(result = workflow.dep(process), destination: str = "default"):
    return f"Published {result['id']} to {destination}"

out = workflow.run(initial_data={
    "source_id": "demo-1",
    "destination": "bucket-xyz",
})

print(out[publish.node_id])  # "Published demo-1 to bucket-xyz"
```

Run concurrently or return only leaf results:

```python
workflow.run(initial_data={"source_id": "id", "destination": "dest"}, executor="concurrent")
workflow.run(return_mode="leaves")
```

### Flow/Task API (functional composition)

```python
from autoworkflow.api import flow, task, param, each, switch

with flow("My First Workflow") as f:
    @task
    def fetch(source_id: str):
        return {"id": source_id, "data": "raw"}

    @task
    def process(raw: dict):
        return {**raw, "processed": True}

    @task
    def publish(result: dict, destination: str = "default"):
        return f"Published {result['id']} to {destination}"

    out = publish(process(fetch(param("source_id"))), destination=param("destination"))

res = f.run(initial_data={"source_id": "demo-1", "destination": "bucket-xyz"})
print(res[out.node_id])
```

Notes:
- Outside `with flow(...)`, `@task` returns the original function unchanged (helpful for unit tests).
- `param("key")` marks a runtime input resolved from `run(initial_data=...)`.

---

## Workflow API Reference (declarative)

### Registering nodes and dependencies

- `@workflow.node(name=..., retries=...)` registers a function as a node.
- Declare dependencies with `workflow.dep(upstream)` as parameter defaults.

```python
@workflow.node(name="transform", retries=2)
def transform(data: dict):
    return {**data, "ok": True}

@workflow.node
def combine(a = workflow.dep(node_a), b = workflow.dep(node_b)):
    return (a, b)

user_info = workflow.node(fetch_user)

@workflow.node
def greet(user_name = workflow.dep(user_info)["name"]):
    return f"Hello {user_name}!"
```

### Context injection

```python
from typing import Any
from autoworkflow import BaseContext, CONTEXT

class AppContext(BaseContext):
    db: Any | None = None

@workflow.node
def use_db(ctx: AppContext = CONTEXT):
    assert hasattr(ctx, "db")

workflow.run(context=AppContext())
```

### Map nodes (fan-out)

```python
@workflow.node
def get_items():
    return ["a", "b", "c"]

def _process(item = workflow.map_dep(get_items)):
    return item.upper()

process_items = workflow.map_node(name="process_items")(_process)

leaves = workflow.run(return_mode="leaves")
assert leaves[process_items.node_id] == ["A", "B", "C"]
```

### Conditional branching (case, bind/select)

```python
@workflow.node
def which():
    return "a"

@workflow.node
def branch_a():
    return "Branch A"

@workflow.node
def branch_b():
    return "Branch B"

case_out = workflow.case(
    on=which,
    branches={
        "a": branch_a,
        "b": branch_b,
        "none": "skip",
    },
)

@workflow.node
def final(x = case_out):
    return x
```

Pre-binding styles:

```python
@workflow.node
def make_item():
    return {"id": "42", "data": "raw"}

@workflow.node
def ship(item: dict, priority: str = "normal", warehouse: str = "A"):
    return f"Ship {item['id']} with {priority} from {warehouse}"

@workflow.node
def choose_priority():
    return "high"

case_via_tuple = workflow.case(
    on=choose_priority,
    branches={
        "high": (ship, {"item": make_item, "priority": "high", "warehouse": "B"}),
        "low": (ship, {"item": make_item, "priority": "low"}),
        "none": "skip",
    },
)

high_shipping = workflow.bind(ship, item=make_item, priority="high", warehouse="B")
low_shipping = workflow.bind(ship, item=make_item, priority="low")
case_via_bind = workflow.case(
    on=choose_priority,
    branches={
        "high": high_shipping,
        "low": low_shipping,
        "none": "skip",
    },
)

case_via_select = workflow.case(
    on=choose_priority,
    branches={
        "high": workflow.select(ship, item=make_item, priority="high", warehouse="B"),
        "low": workflow.select(ship, item=make_item, priority="low"),
        "none": "skip",
    },
)
```

### Retries, executors, return modes

- All node registrations accept `retries=int`.
- Executors: `"concurrent"` (thread pool) or provide a custom `Executor`.
- Return modes: `"all"` (default) or `"leaves"`.

---

## Flow/Task API Reference (functional composition)

This API lives in `autoworkflow.api` and compiles to the same engine.

### Building flows

- `with flow(name, description=...) as f:` opens a build context.
- `@task` inside a flow registers a node and returns a `Task` wrapper.
- Calling a `Task` within the flow composes dependencies; outside the flow it calls the original Python function.

```python
from autoworkflow.api import flow, task

with flow("compose-demo") as f:
    @task
    def plus(a: int, b: int = 1) -> int:
        return a + b

    out = plus(plus(39, b=1), b=2)  # build-time composition

res = f.run()
print(res[out.node_id])  # 42
```

### Runtime parameters: `param("key")`

Use `param(...)` as a placeholder for values supplied via `f.run(initial_data=...)`. Placeholders are ignored during binding so they resolve at runtime.

```python
from autoworkflow.api import flow, task, param

with flow("params-demo") as f:
    @task
    def publish(result: dict, destination: str = "default") -> str:
        return f"Published {result['id']} to {destination}"

    @task
    def fetch(source_id: str) -> dict:
        return {"id": source_id}

    out = publish(fetch(param("source_id")), destination=param("destination"))

print(f.run(initial_data={"source_id": "s-1", "destination": "bucket"})[out.node_id])
```

### Binding arguments explicitly

- `task_obj.bind(**kwargs)` creates a pre-bound node handle.
- Constants are embedded; dependencies can be other `Task` or `NodeOutput` values.

```python
with flow("bind-demo") as f:
    @task
    def ship(item: dict, priority: str = "normal") -> str:
        return f"ship-{item['id']}-{priority}"

    @task
    def make() -> dict:
        return {"id": "42"}

    hi = ship.bind(item=make(), priority="high")
    out = hi

print(f.run()[out.node_id])  # "ship-42-high"
```

### Map operations: `each(...).over(...)` or `task.map(over=...)`

Both forms create a `map` node that applies a task to each element from a list-producing upstream node. Optional `name`, `retries`, and constant bindings are supported.

```python
from autoworkflow.api import flow, task, each

with flow("map-demo") as f:
    @task
    def items() -> list[str]:
        return ["a", "b", "c"]

    @task
    def upcase(x: str) -> str:
        return x.upper()

    ups1 = each(upcase).over(items())
    ups2 = upcase.map(over=items())

assert f.run(return_mode="leaves")[ups1.node_id] == ["A", "B", "C"]
assert f.run(return_mode="leaves")[ups2.node_id] == ["A", "B", "C"]
```

### Conditional branching: `switch(...).when(...).otherwise(...)`

- `when(key_or_pred, target, **bindings)` adds a branch where `key_or_pred` is either a literal key or a predicate `(value) -> bool`.
- `target` can be a constant, a `Task`, or a `NodeOutput`. When a `Task` is given, extra keyword arguments are bound as constants.

```python
from autoworkflow.api import flow, task, switch

with flow("case-demo") as f:
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
    def final(x: str) -> str:
        return x

    out = final(choice)

res = f.run()
assert res[out.node_id] == "B"
```

### Running flows

- `f.run(initial_data=..., context=..., executor=..., return_mode=...)` mirrors `Workflow.run(...)`.
- Flows automatically prune isolated helper nodes so only the composed subgraph is executed.

---

## Long-Running Automation with Engine

```python
from autoworkflow import Engine
from autoworkflow.services.triggers import ScheduledTrigger

engine = Engine(config={})
engine.register_workflow(workflow)

# Run the workflow every minute
engine.add_trigger(ScheduledTrigger("* * * * *"), workflow_name=workflow.name)

engine.run()  # blocks; Ctrl+C to stop
```

Built-in triggers:

- `ScheduledTrigger(cron: str)`: minimal cron-like schedules, e.g. `"every 10s"`, `"*/5 * * * *"`, or `"* * * * *"` (every minute).
- `DirectoryWatchTrigger(...)`: poll a directory for new files with a suffix; emits file path payloads.

---

### Engine ↔ Workflow: inputs and context

How Engine supplies inputs to a Workflow, and how a Workflow should receive them.

- **Data flow**:
  - When a trigger fires, it emits a payload `Dict[str, Any]`.
  - Engine merges `initial_data` (provided at `add_trigger(...)`) and the trigger payload into one `initial_data` for the run.
  - Merge rule: first `initial_data`, then payload; on key conflicts, the payload value overwrites.
- **Context flow**:
  - Provide a `context` per trigger via `add_trigger(..., context=...)`, or set a per-run factory via `engine.set_context_factory(lambda: AppContext())`.
  - Plugins can inject providers into the context automatically (see Plugins section).

Receive these inputs in your Workflow by declaring node parameters without defaults (Workflow API), or by using `param("key")` placeholders (Flow/Task API). Keys must match.

#### Example: DirectoryWatchTrigger → Workflow API

```python
from autoworkflow import Workflow
from autoworkflow.services.triggers import DirectoryWatchTrigger
from autoworkflow.services.engine import Engine

workflow = Workflow(name="ingest-files")

@workflow.node
def read_file(file_path: str) -> bytes:
    with open(file_path, "rb") as f:
        return f.read()

@workflow.node
def store(content = workflow.dep(read_file), destination: str = "bucket") -> str:
    # pretend to store and return an id
    return f"stored://{destination}/obj-123"

engine = Engine(config={})
engine.register_workflow(workflow)

trigger = DirectoryWatchTrigger(
    watch_dir="/incoming",
    suffix=".txt",
    payload_key="file_path",  # payload will be {"file_path": "/incoming/xyz.txt"}
)

engine.add_trigger(
    trigger,
    workflow_name=workflow.name,
    initial_data={"destination": "my-bucket"},  # merged, overridden by payload on conflicts
)

engine.run()
```

Notes:
- The node `read_file` requires `file_path`, which is provided by the trigger payload.
- The node `store` receives `destination` from `initial_data`.

#### Example: DirectoryWatchTrigger → Flow/Task API

```python
from autoworkflow.api import flow, task, param
from autoworkflow.services.triggers import DirectoryWatchTrigger
from autoworkflow.services.engine import Engine

with flow("ingest-files-flow") as f:
    @task
    def read_file(file_path: str) -> bytes:
        with open(file_path, "rb") as fp:
            return fp.read()

    @task
    def store(content: bytes, destination: str = "bucket") -> str:
        return f"stored://{destination}/obj-123"

    out = store(read_file(param("file_path")), destination=param("destination"))

engine = Engine(config={})
engine.register_workflow(f.workflow)  # register the compiled Workflow

trigger = DirectoryWatchTrigger(
    watch_dir="/incoming",
    suffix=".txt",
    payload_key="file_path",
)

engine.add_trigger(
    trigger,
    workflow_name=f.workflow.name,
    initial_data={"destination": "my-bucket"},
)

engine.run()
```

#### Context: passing and injecting

```python
from typing import Any
from autoworkflow import BaseContext
from autoworkflow.services.engine import Engine

class AppContext(BaseContext):
    db: Any | None = None

engine = Engine(config={})

# Option A: provide a fixed context for one trigger
ctx = AppContext()
engine.add_trigger(trigger, workflow_name="ingest-files", context=ctx)

# Option B: create a fresh context per run
engine.set_context_factory(lambda: AppContext())
```

If plugins are registered, Engine will inject providers into the context instance before running the workflow. In a node, request the context via `ctx: AppContext = CONTEXT`.

#### Multiple triggers to one workflow with different inputs

```python
engine.add_trigger(ScheduledTrigger("every 10s"), workflow_name=workflow.name, initial_data={"destination": "cold"})
engine.add_trigger(DirectoryWatchTrigger("/hot", suffix=".bin"), workflow_name=workflow.name, initial_data={"destination": "hot"})
```

Each trigger produces its own `initial_data` per run, merged with its payload.

---

## Web UI (Optional)

The optional Web UI (FastAPI + Uvicorn) visualizes runs and node status in real time.

Enable it:

```python
engine = Engine(config={}, web_ui=True)
engine.run()
```

Custom host/port:

```python
engine = Engine(config={}).enable_web_ui(host="127.0.0.1", port=8008)
```

The UI provides:

- Registered workflows and structure
- Live-updating runs
- Per-node status (pending, running, done, failed)

---

## Plugins (Service Injection)

Plugins initialize services (e.g., API clients) and inject them into the `BaseContext` used during execution.

Plugin protocol (simplified):

```python
from typing import Protocol, Dict, Any

class Plugin(Protocol):
    @property
    def name(self) -> str: ...
    def setup(self, engine, config: Dict[str, Any]): ...
    def get_context_provider(self) -> Any: ...
    def get_context_injections(self) -> Dict[str, Any]: ...  # optional mapping attr->provider
    def teardown(self): ...
```

Registering a plugin:

```python
engine.register_plugin(MyPlugin())
engine.set_context_factory(lambda: AppContext())
```

### qBittorrent Plugin

Provided in `autoworkflow_plugins.qbittorrent.QBittorrentPlugin`. It authenticates a qBittorrent WebUI client and injects it into the context under `qbittorrent` attributes.

Configuration keys under `engine = Engine(config={"qbittorrent": {...}})`:

- `host` (str, default `"localhost"`)
- `port` (int, default `8080`)
- `username` (str, default `"admin"`)
- `password` (str, default `"adminadmin"`)
- `use_https` (bool, default `False`)
- `verify_ssl` (bool, default `True`)
- `timeout` (int seconds, default `30`)

Or initialize explicitly:

```python
from autoworkflow_plugins.qbittorrent import QBittorrentPlugin

engine.register_plugin(QBittorrentPlugin(
    host="localhost",
    port=8080,
    username="admin",
    password="adminadmin"
))
```

Implement the `HasQBittorrent` protocol in your Context:

```python
from autoworkflow_plugins.qbittorrent import HasQBittorrent, Client

class MyContext(BaseContext, HasQBittorrent):
    def __init__(self):
        self.qbittorrent: Client | None = None
```

See `examples/torrent_watch_qb.py` for a complete example (directory watch → add torrent → wait for completion → optional 7-Zip compression).

Run:

```bash
python examples/torrent_watch_qb.py \
  --watch-dir "/path/to/incoming" \
  --download-dir "/path/to/downloads" \
  --qb-host localhost --qb-port 8080 \
  --qb-username admin --qb-password adminadmin
```

### Bangumi.moe Plugin

Provided in `autoworkflow_plugins.BangumiMoe.BangumiMoePlugin`. It authenticates against Bangumi.moe and injects a `BangumiMoeClient` into the context under `bangumi_moe`.

Configuration keys under `engine = Engine(config={"bangumi_moe": {...}})`:

- `base_url` (str, default `"https://bangumi.moe"`)
- `username` (str, required)
- `password` (str, required)
- `verify_ssl` (bool, default `True`)
- `timeout` (int seconds, default `30`)

Or initialize explicitly:

```python
from autoworkflow_plugins.BangumiMoe import BangumiMoePlugin, HasBangumiMoe, BangumiMoeClient

engine.register_plugin(BangumiMoePlugin(
    username="your_username",
    password="your_password",
    base_url="https://bangumi.moe",
    verify_ssl=True,
    timeout=30,
))
```

Implement the `HasBangumiMoe` protocol in your Context:

```python
from autoworkflow import BaseContext
from autoworkflow_plugins.BangumiMoe import HasBangumiMoe, BangumiMoeClient

class MyContext(BaseContext, HasBangumiMoe):
    def __init__(self):
        self.bangumi_moe: BangumiMoeClient | None = None
```

Example node using context injection:

```python
from autoworkflow import Workflow, CONTEXT

workflow = Workflow(name="bangumi-upload")

@workflow.node
def publish(ctx: MyContext = CONTEXT, torrent_path: str = "/path/to/file.torrent"):
    client = ctx.bangumi_moe
    assert client is not None, "BangumiMoe client missing"
    return client.publish(
        torrent_path,
        title="My Release",
        introduction="Description of the release",
    )
```

---

## Persisting State (advanced)

Executors can persist per-node results via a state backend. Implement `StateBackend` and pass it to `Workflow.run(..., state_backend=...)` or `Engine.set_state_backend(...)`.

```python
from typing import Any, Dict
from autoworkflow.services.state import StateBackend

class MemoryBackend(StateBackend):
    def __init__(self):
        self._runs: Dict[str, Dict[str, Any]] = {}
    def save_result(self, run_id: str, node_id: str, result: Any):
        self._runs.setdefault(run_id, {})[node_id] = result
    def load_result(self, run_id: str, node_id: str) -> Any:
        return self._runs.get(run_id, {}).get(node_id)
    def get_run_state(self, run_id: str) -> Dict[str, Any]:
        return dict(self._runs.get(run_id, {}))
```

---

## Errors and Debugging

- `DefinitionError`: bad graph definitions (e.g., duplicate node ids, missing required `initial_data`).
- `ExecutionError`: a node failed even after retries; the error includes `node_id` when available.

Tips:

- Use `return_mode="all"` to inspect intermediate results.
- Add `retries` to transient nodes that may fail.

---

## Public API Surface

From `autoworkflow`:

- `Workflow`
- `BaseContext`
- `CONTEXT`
- `Engine`
- `ScheduledTrigger`
- Exceptions: `WorkflowError`, `DefinitionError`, `ExecutionError`

From `autoworkflow.api`:

- `flow`
- `task`
- `param`
- `each`
- `switch`

---

## Running Tests

```bash
pytest -q
```

---

## License

GPL-3.0-or-later. See `LICENSE` for details.
