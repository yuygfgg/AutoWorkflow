"""
High-level, ergonomic API sugar over autoworkflow.core.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional
import inspect
import threading

from .core import Workflow, NodeOutput


# Thread-local current flow for build-mode interception
_flow_local = threading.local()


class Param:
    """Represents a runtime parameter provided via initial_data at run()."""

    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:  # English for code outputs
        return f"Param({self.name!r})"


def param(name: str) -> str:
    # Typed as 'str' for ergonomic type-checking; runtime object carries the key.
    return Param(name)  # type: ignore[return-value]


class Flow:
    """Context wrapper for a core Workflow with ergonomic helpers."""

    def __init__(self, name: str, description: str = ""):
        self._wf = Workflow(name=name, description=description)
        self.name = name
        self.description = description
        # Compiled/pruned workflow after build-mode ends
        self._compiled: Optional[Workflow] = None

    def __enter__(self) -> "Flow":
        setattr(_flow_local, "current", self)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Clear only if we are the current
        cur = getattr(_flow_local, "current", None)
        if cur is self:
            setattr(_flow_local, "current", None)
        # On exiting build-mode, prune unused template nodes introduced by the sugar API
        try:
            wf = self._wf
            all_nodes = dict(wf._nodes)
            used: set[str] = set()

            # Seed with: nodes that have deps, nodes referenced as deps, case nodes, and case targets
            for nd in all_nodes.values():
                if nd.dependencies:
                    used.add(nd.id)
                    for dep in nd.dependencies.values():
                        used.add(dep.node_id)
                if getattr(nd, "kind", "normal") == "case":
                    used.add(nd.id)
                    branches: Dict[Any, Any] = nd.meta.get("branches", {})
                    for _cond, target in branches.items():
                        if hasattr(target, "node_id"):
                            used.add(getattr(target, "node_id"))

            # Transitive closure over dependencies and case targets
            grew = True
            while grew:
                prev_len = len(used)
                for nid in list(used):
                    nd = all_nodes.get(nid)
                    if not nd:
                        continue
                    for dep in nd.dependencies.values():
                        used.add(dep.node_id)
                    if getattr(nd, "kind", "normal") == "case":
                        branches = nd.meta.get("branches", {})
                        for _cond, target in branches.items():
                            if hasattr(target, "node_id"):
                                used.add(getattr(target, "node_id"))
                grew = len(used) != prev_len

            if used:
                new_wf = Workflow(name=wf.name, description=wf.description)
                for nid in used:
                    if nid in all_nodes:
                        new_wf._nodes[nid] = all_nodes[nid]
                self._compiled = new_wf
            else:
                self._compiled = wf
        except Exception:
            # In case pruning fails for any reason, fallback to raw workflow
            self._compiled = self._wf

    # Surface selected Workflow methods
    def run(self, *args, **kwargs):
        wf = self._wf
        all_nodes = dict(wf._nodes)
        used: set[str] = set()

        # Seed with: nodes that have deps, nodes referenced as deps, case nodes, and case targets
        for nd in all_nodes.values():
            if nd.dependencies:
                used.add(nd.id)
                for dep in nd.dependencies.values():
                    used.add(dep.node_id)
            if getattr(nd, "kind", "normal") == "case":
                used.add(nd.id)
                branches: Dict[Any, Any] = nd.meta.get("branches", {})
                for _cond, target in branches.items():
                    if hasattr(target, "node_id"):
                        used.add(getattr(target, "node_id"))

        # Transitive closure over dependencies and case targets
        grew = True
        while grew:
            prev_len = len(used)
            for nid in list(used):
                nd = all_nodes.get(nid)
                if not nd:
                    continue
                for dep in nd.dependencies.values():
                    used.add(dep.node_id)
                if getattr(nd, "kind", "normal") == "case":
                    branches = nd.meta.get("branches", {})
                    for _cond, target in branches.items():
                        if hasattr(target, "node_id"):
                            used.add(getattr(target, "node_id"))
            grew = len(used) != prev_len

        if used:
            new_wf = Workflow(name=wf.name, description=wf.description)
            for nid in used:
                if nid in all_nodes:
                    new_wf._nodes[nid] = all_nodes[nid]
            return new_wf.run(*args, **kwargs)
        return wf.run(*args, **kwargs)

    @property
    def workflow(self) -> Workflow:
        if self._compiled is None:
            try:
                wf = self._wf
                all_nodes = dict(wf._nodes)
                used: set[str] = set()

                for nd in all_nodes.values():
                    if nd.dependencies:
                        used.add(nd.id)
                        for dep in nd.dependencies.values():
                            used.add(dep.node_id)
                    if getattr(nd, "kind", "normal") == "case":
                        used.add(nd.id)
                        branches: Dict[Any, Any] = nd.meta.get("branches", {})
                        for _cond, target in branches.items():
                            if hasattr(target, "node_id"):
                                used.add(getattr(target, "node_id"))

                grew = True
                while grew:
                    prev_len = len(used)
                    for nid in list(used):
                        nd = all_nodes.get(nid)
                        if not nd:
                            continue
                        for dep in nd.dependencies.values():
                            used.add(dep.node_id)
                        if getattr(nd, "kind", "normal") == "case":
                            branches = nd.meta.get("branches", {})
                            for _cond, target in branches.items():
                                if hasattr(target, "node_id"):
                                    used.add(getattr(target, "node_id"))
                    grew = len(used) != prev_len

                if used:
                    new_wf = Workflow(name=wf.name, description=wf.description)
                    for nid in used:
                        if nid in all_nodes:
                            new_wf._nodes[nid] = all_nodes[nid]
                    self._compiled = new_wf
                else:
                    self._compiled = wf
            except Exception:
                self._compiled = self._wf
        return self._compiled


def flow(name: str, description: str = "") -> Flow:
    return Flow(name=name, description=description)


class Task:
    """Wrapper around a registered node to enable call-as-binding semantics."""

    def __init__(
        self,
        flow: Flow,
        func: Callable[..., Any],
        handle: NodeOutput[Any],
        *,
        name: str,
        retries: int,
    ):
        self._flow = flow
        self._func = func
        self._handle = handle
        self._name = name
        self._retries = retries

        # Signature for mapping args -> param names
        self._sig = inspect.signature(func)

    @property
    def node_id(self) -> str:
        return self._handle.node_id

    @property
    def handle(self) -> NodeOutput[Any]:
        return self._handle

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # If not in this flow's build-mode, execute the function normally.
        current: Optional[Flow] = getattr(_flow_local, "current", None)
        if current is None or current is not self._flow:
            return self._func(*args, **kwargs)

        # In build-mode, interpret the call as producing a bound NodeOutput
        bindings: Dict[str, Any] = {}
        ba = self._sig.bind_partial(*args, **kwargs)
        for pname, value in ba.arguments.items():
            # Skip runtime Params → leave to initial_data resolution
            if isinstance(value, Param):
                continue
            bindings[pname] = value

        if not bindings:
            return self._handle

        return self._flow.workflow.bind(self._handle, **bindings)

    def bind(self, **bindings: Any) -> NodeOutput[Any]:
        # Ignore Param(...) to defer to initial_data
        clean: Dict[str, Any] = {
            k: v for k, v in bindings.items() if not isinstance(v, Param)
        }
        if not clean:
            return self._handle
        return self._flow.workflow.bind(self._handle, **clean)

    # Map sugar: task.map(over=list_output, name=None, retries=None, **consts)
    def map(
        self,
        *,
        over: NodeOutput[Any],
        name: Optional[str] = None,
        retries: Optional[int] = None,
        **consts: Any,
    ) -> NodeOutput[Any]:
        return _map_from_task(
            self._flow, self, over, name=name, retries=retries, **consts
        )


def task(func: Any = None, *, name: Optional[str] = None, retries: int = 0) -> Any:
    """
    Decorator for registering a function as a node on the active Flow.

    Outside a Flow context, returns the original function unchanged so it can
    be used directly.
    """

    def _decorate(f: Callable[..., Any]):
        current: Optional[Flow] = getattr(_flow_local, "current", None)
        if current is None:
            # Not in build-mode: return function as-is
            return f
        # Register on current workflow
        handle = current.workflow.node(f, name=name, retries=retries)
        node_name = name or f.__name__
        return Task(current, f, handle, name=node_name, retries=retries)

    if func is not None:
        return _decorate(func)
    return _decorate


class _EachBuilder:
    def __init__(
        self, flow: Flow, task_obj: Task, *, default_name: Optional[str] = None
    ):
        self._flow = flow
        self._task = task_obj
        self._default_name = default_name

    def over(
        self,
        source: Any,
        *,
        name: Optional[str] = None,
        retries: Optional[int] = None,
        **consts: Any,
    ) -> Any:
        return _map_from_task(
            self._flow,
            self._task,
            source,
            name=name or self._default_name,
            retries=retries,
            **consts,
        )


def each(task_obj: Any, *, name: Optional[str] = None) -> _EachBuilder:
    current: Optional[Flow] = getattr(_flow_local, "current", None)
    if current is None:
        raise RuntimeError("each(task) must be used inside a flow context")
    default_name = name or f"{task_obj.node_id}__map"
    return _EachBuilder(current, task_obj, default_name=default_name)


def _map_from_task(
    flow_obj: Flow,
    task_obj: Any,
    source: Any,
    *,
    name: Optional[str] = None,
    retries: Optional[int] = None,
    **consts: Any,
) -> Any:
    # Wrap original function into a one-arg function whose default is map_dep(source)
    wf = flow_obj.workflow

    def _mapped(item=wf.map_dep(source)):
        return task_obj._func(item)

    node_name = name or f"{task_obj.node_id}__map"
    handle = wf.map_node(name=node_name, retries=retries or 0)(_mapped)
    # Apply const bindings (Param ignored)
    clean_consts: Dict[str, Any] = {
        k: v for k, v in consts.items() if not isinstance(v, Param)
    }
    if clean_consts:
        handle = wf.bind(handle, **clean_consts)
    return handle


class _SwitchBuilder:
    def __init__(self, flow_obj: Flow, on: NodeOutput[Any]):
        self._flow = flow_obj
        self._on = on
        # preserve order for deterministic fallback
        self._pairs: list[tuple[Any, Any]] = []

    def when(
        self, key_or_pred: Any, target: Any, /, **bindings: Any
    ) -> "_SwitchBuilder":
        target_spec: Any = _normalize_target(self._flow, target, **bindings)
        self._pairs.append((key_or_pred, target_spec))
        return self

    def otherwise(self, target: Any, /, **bindings: Any) -> Any:
        target_spec: Any = _normalize_target(self._flow, target, **bindings)
        # last-catch: always-true predicate
        self._pairs.append((lambda _v: True, target_spec))
        branches: Dict[Any, Any] = {}
        for k, v in self._pairs:
            branches[k] = v
        return self._flow.workflow.case(on=self._on, branches=branches)


def switch(on: Any) -> _SwitchBuilder:
    current: Optional[Flow] = getattr(_flow_local, "current", None)
    if current is None:
        raise RuntimeError("switch(on) must be used inside a flow context")
    return _SwitchBuilder(current, on)


def _normalize_target(flow_obj: Flow, target: Any, /, **bindings: Any) -> Any:
    # Accept constants, NodeOutput, Task, or (Task/NodeOutput, {bindings})
    if isinstance(target, Task):
        clean = {k: v for k, v in bindings.items() if not isinstance(v, Param)}
        if clean:
            return flow_obj.workflow.bind(target.handle, **clean)
        return target.handle
    return target
