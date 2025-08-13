## AutoWorkflow

[![codecov](https://codecov.io/gh/yuygfgg/AutoWorkflow/graph/badge.svg?token=OXK7R357AX)](https://codecov.io/gh/yuygfgg/AutoWorkflow)

Declarative, type-safe automation/workflow engine for Python. Define tasks as plain functions, declare dependencies with simple helpers, and run them with a concurrent executor. For long-running automations, use the Engine with pluggable Triggers and Plugins.

### Why AutoWorkflow?

- Clear, Pythonic API: tasks are normal functions; dependencies are declared via defaults.
- Typed and explicit: helpers make dependencies and map operations obvious to readers and tools.
- Flexible execution: concurrent execution with a thread pool.
- Event-driven service: run workflows continuously with Triggers; inject services via Plugins.

---

## Installation

Requirements:

- Python 3.10+

```bash
pip install git+https://github.com/yuygfgg/autoworkflow
```

---

## Quick Start

Define a workflow, add nodes with dependencies, and run:

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

Run concurrently:

```python
workflow.run(initial_data={"source_id": "id", "destination": "dest"}, executor="concurrent")
```

Return only leaf-node results:

```python
workflow.run(return_mode="leaves")
```

---

## Core Concepts

### Workflow and Nodes

- `Workflow`: container for nodes and their dependency graph.
- `@workflow.node`: register a function as a node. The node's name defaults to the function name, or you may set `name="..."`. You may also pass `retries=N` for simple retry logic.

```python
@workflow.node(name="transform", retries=2)
def transform(data: dict):
    return {**data, "ok": True}
```

### Declaring Dependencies

Use `workflow.dep(upstream_node)` to bind a parameter's default value to the output of an upstream node.

```python
@workflow.node
def combine(a = workflow.dep(node_a), b = workflow.dep(node_b)):
    return (a, b)
```

You can also access parts of an upstream output using attribute or key access on the handle:

```python
user_info = workflow.node(fetch_user)

@workflow.node
def greet(user_name = workflow.dep(user_info)["name"]):
    return f"Hello {user_name}!"
```

### Context Injection (CONTEXT)

Define a typed context by subclassing `BaseContext`. Use the `CONTEXT` sentinel to request the context in a node.

```python
from typing import Any
from autoworkflow import BaseContext, CONTEXT

class AppContext(BaseContext):
    db: Any | None = None

@workflow.node
def use_db(ctx: AppContext = CONTEXT):
    assert hasattr(ctx, "db")
    # ...
```

Provide the context at runtime:

```python
workflow.run(context=AppContext())
```

### Map Nodes (Fan-out for lists)

`workflow.map_node` defines a node that operates on each item of a list produced upstream. Declare the mapped item with `workflow.map_dep(...)`.

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

### Conditional Branching (case)

Use `workflow.case(on=..., branches={...})` to choose a branch at runtime. Branch targets can be node handles or constants. You can also pre-bind arguments before inserting targets into `branches` using `workflow.bind(...)` (alias: `workflow.select(...)`).

Basic usage (choose node or constant directly):

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
        "a": branch_a,   # pick node directly
        "b": branch_b,   # pick node directly
        "none": "skip", # pick constant directly
    },
)

@workflow.node
def final(x = case_out):
    return x
```

Pre-binding parameters (bind/select) with three equivalent styles:

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

# Style 1: use a (node, {bindings}) tuple directly inside `branches`
# - Values in the dict can be constants or other NodeOutput(s) (as dependencies)
case_via_tuple = workflow.case(
    on=choose_priority,
    branches={
        "high": (ship, {"item": make_item, "priority": "high", "warehouse": "B"}),
        "low": (ship, {"item": make_item, "priority": "low"}),
        "none": "skip",
    },
)

# Style 2: create pre-bound nodes via `bind` and then place them into `branches`
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

# Style 3: use `select` (an alias of `bind`)
case_via_select = workflow.case(
    on=choose_priority,
    branches={
        "high": workflow.select(ship, item=make_item, priority="high", warehouse="B"),
        "low": workflow.select(ship, item=make_item, priority="low"),
        "none": "skip",
    },
)

@workflow.node
def done1(x = case_via_tuple):
    return x

@workflow.node
def done2(x = case_via_bind):
    return x

@workflow.node
def done3(x = case_via_select):
    return x
```

### Retries

All node decorators accept `retries=int`. Failures raise `ExecutionError` after all attempts.

```python
@workflow.node(retries=3)
def fragile():
    # may raise
    return 42
```

### Executors and Return Modes

- Executors: `"concurrent"` (thread pool). You can also pass a custom executor implementing the `Executor` protocol.
- Return modes: `"all"` (default) returns every node's output; `"leaves"` returns only leaf-node outputs.

---

## Long-Running Automation with Engine

`Engine` manages workflows, plugins, and triggers to build event-driven services that run indefinitely.

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

## Web UI (Optional)

AutoWorkflow includes an optional, lightweight Web UI to visualize workflow runs and their status in real-time. It's built with FastAPI and provides a simple dashboard to monitor the engine.

To enable it, set `web_ui=True` when initializing the `Engine`:

```python
engine = Engine(config={}, web_ui=True)
engine.run()
```

By default, the UI is available at `http://127.0.0.1:8008`. You can customize the host and port:

```python
engine = Engine(config={}).enable_web_ui(host="127.0.0.1", port=8008)
```

The UI provides:

- A list of all registered workflows and their structure.
- A live-updating list of workflow runs.
- The status of each node within a run (e.g., `pending`, `running`, `done`, `failed`).

This feature is self-contained and requires `fastapi` and `uvicorn` to be installed.

---

## Plugins (Service Injection)

Plugins initialize services (e.g., clients) and inject them into the runtime context.

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

Configuration keys under `engine = Engine(config={"qbittorrent": {...}})` 

- `host` (str, default `"localhost"`)
- `port` (int, default `8080`)
- `username` (str, default `"admin"`)
- `password` (str, default `"adminadmin"`)
- `use_https` (bool, default `False`)
- `verify_ssl` (bool, default `True`)
- `timeout` (int seconds, default `30`)

or when initializing the QBittorrentPlugin:

```python
from autoworkflow_plugins.qbittorrent import QBittorrentPlugin

engine.register_plugin(QBittorrentPlugin(
    host="localhost",
    port=8080,
    username="admin",
    password="adminadmin"
    # ...
))
```

Implement the `HasQBittorrent` protocol in your Context:

```python
from autoworkflow_plugins.qbittorrent import HasQBittorrent, Client

class MyContext(BaseContext, HasQBittorrent):
    def __init__(self):
        self.qbittorrent: Client | None = None
```

See `examples/torrent_watch_qb.py`. It watches a directory for new `.torrent` files, adds them to qBittorrent, waits for completion with progress display, and optionally compresses the result with 7-Zip when size exceeds a threshold.

Prerequisites:

- qBittorrent WebUI running and accessible
- `qbittorrent-api` installed (`pip install qbittorrent-api`)
- `7z` CLI available in PATH (install p7zip / 7-Zip)

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

Configuration keys under `engine = Engine(config={"bangumi_moe": {...}})`

- `base_url` (str, default `"https://bangumi.moe"`)
- `username` (str, required)
- `password` (str, required)
- `verify_ssl` (bool, default `True`)
- `timeout` (int seconds, default `30`)

or when initializing the BangumiMoePlugin:

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

Example: publish a torrent file in a node using context injection:

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

Prerequisites:

- Bangumi.moe account with valid credentials
- A valid `.torrent` file to upload

---

## Persisting State (Advanced)

Executors can persist per-node results via a state backend. Implement the `StateBackend` protocol and pass it to `Workflow.run(..., state_backend=...)` or `Engine.set_state_backend(...)`.

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
- `ExecutionError`: a node failed even after retries. The error includes a `node_id` attribute when available.

Tips:

- Use `return_mode="all"` to inspect intermediate results.
- Add `retries` to transient nodes that may fail.

---

## Public API (from autoworkflow)

- `Workflow`
- `BaseContext`
- `CONTEXT`
- `Engine`
- `ScheduledTrigger`
- Exceptions: `WorkflowError`, `DefinitionError`, `ExecutionError`

---

## Running Tests

```bash
pytest -q
```

---

## License

GPL-3.0-or-later. See `LICENSE` for details.
