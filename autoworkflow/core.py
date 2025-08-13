"""
Core Definition Layer
This module contains all the core building blocks that users interact with
to define a workflow, its nodes, and their dependencies.
"""

from __future__ import annotations

from typing import (
    TypeVar,
    Generic,
    Callable,
    Any,
    Dict,
    List,
    Optional,
    Mapping,
    TYPE_CHECKING,
    overload,
    cast,
)
import dataclasses
import time
import logging
import inspect

from .exceptions import DefinitionError
from .graph import WorkflowGraph, NodeDefinition

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")

if TYPE_CHECKING:
    from .executors.base import Executor

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class NodeOutput(Generic[T]):
    """
    A typed, symbolic proxy representing the future output of a node.
    It is not meant to be instantiated by the user directly.
    """

    node_id: str
    output_type: Any
    access_path: tuple[tuple[str, Any], ...] = dataclasses.field(default_factory=tuple)

    def _with_path(self, kind: str, value: Any) -> "NodeOutput[Any]":
        return NodeOutput(
            node_id=self.node_id,
            output_type=Any,
            access_path=(*self.access_path, (kind, value)),
        )

    def __getitem__(self, key: str | int) -> "NodeOutput[Any]":
        """Creates a proxy for accessing an item from a dict-like or list-like output."""
        return self._with_path("key", key)

    def __getattr__(self, name: str) -> "NodeOutput[Any]":
        """Creates a proxy for accessing an attribute from an object-like output."""
        if name in {"node_id", "output_type", "access_path", "_with_path"}:
            return super().__getattribute__(name)
        return self._with_path("attr", name)


class BaseContext:
    """
    The base class for user-defined, type-safe contexts.

    Users should subclass this and add their own typed attributes for
    configurations, secrets, and plugin clients.
    """

    pass


CONTEXT = object()


class Workflow:
    """A container for registering nodes and defining a computation graph."""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        # Internal registry of node definitions
        self._nodes: Dict[str, NodeDefinition] = {}
        # Optional lifecycle hooks
        self._on_success: Optional[Callable[[Dict[str, Any], BaseContext], None]] = None
        self._on_failure: Optional[Callable[[str, Exception, BaseContext], None]] = None

    @overload
    def node(
        self, func: Callable[..., T], *, name: str | None = None, retries: int = 0
    ) -> NodeOutput[T]: ...

    @overload
    def node(
        self, func: None = ..., *, name: str | None = None, retries: int = 0
    ) -> Callable[[Callable[..., T]], NodeOutput[T]]: ...

    def node(
        self,
        func: Optional[Callable[..., Any]] = None,
        *,
        name: str | None = None,
        retries: int = 0,
    ) -> Any:
        """
        Registers a standard function as a node in the workflow.

        Supports usage styles:
        - @workflow.node
        - @workflow.node(name="...")
        - handle = workflow.node(func)
        """

        def _register(f: Callable[..., T]) -> NodeOutput[T]:
            node_name = name or f.__name__
            if node_name in self._nodes:
                raise DefinitionError(f"Duplicate node id detected: '{node_name}'")

            # Discover dependencies declared via default NodeOutput or CONTEXT
            dependencies: Dict[str, NodeOutput[Any]] = {}
            signature = inspect.signature(f)
            for param_name, param in signature.parameters.items():
                if param.default is inspect._empty:
                    continue
                default_val = param.default
                if isinstance(default_val, NodeOutput):
                    dependencies[param_name] = default_val

            node_def = NodeDefinition(
                id=node_name,
                func=f,
                dependencies=dependencies,
                retries=retries,
                kind="normal",
            )
            self._nodes[node_name] = node_def
            return NodeOutput(node_id=node_name, output_type=Any)

        if callable(func):
            return _register(func)
        return _register

    @overload
    def map_node(
        self,
        func: Callable[[Any], U],
        *,
        name: str | None = None,
        retries: int = 0,
    ) -> NodeOutput[List[U]]: ...

    @overload
    def map_node(
        self,
        func: None = ...,
        *,
        name: str | None = None,
        retries: int = 0,
    ) -> Callable[[Callable[[Any], U]], NodeOutput[List[U]]]: ...

    def map_node(
        self,
        func: Optional[Callable[[Any], Any]] = None,
        *,
        name: str | None = None,
        retries: int = 0,
    ) -> Any:
        """
        Registers a function as a dynamic `map` node.
        """

        def _decorator(f: Callable[[Any], U]) -> NodeOutput[List[U]]:
            node_name = name or f.__name__
            if node_name in self._nodes:
                raise DefinitionError(f"Duplicate node id detected: '{node_name}'")

            signature = inspect.signature(f)
            params = list(signature.parameters.values())

            if len(params) != 1:
                raise DefinitionError(
                    "Map node function must have exactly one parameter."
                )

            mapped_param = params[0]
            dependencies = {}
            if isinstance(mapped_param.default, NodeOutput):
                dependencies = {mapped_param.name: mapped_param.default}

            node_def = NodeDefinition(
                id=node_name,
                func=f,
                dependencies=dependencies,
                retries=retries,
                kind="map",
                meta={"mapped_param_name": mapped_param.name},
            )
            self._nodes[node_name] = node_def
            return NodeOutput(node_id=node_name, output_type=List[U])

        if callable(func):
            return _decorator(func)
        return _decorator

    def case(
        self,
        on: Any,
        branches: Mapping[Any | Callable[[Any], bool], Any],
    ) -> NodeOutput[Any]:
        """
        Constructs a conditional execution branch in the workflow.

        Syntax supported for branch targets:
        - NodeOutput: use the node as-is
        - (node, {bindings}): tuple where `node` is NodeOutput or node id, and bindings are kwargs
        - constants: returned directly when matched
        """
        node_name = f"case_on_{on.node_id}"
        if node_name in self._nodes:
            raise DefinitionError(f"Duplicate node id detected: '{node_name}'")

        # Case depends only on `on`; branches are described in meta for short-circuiting executors
        dependencies: Dict[str, NodeOutput[Any]] = {"on": on}

        def _case_placeholder(
            on: Any, **kwargs: Any
        ) -> Any:  # not used in short-circuit executor
            return on

        # Normalize branches: materialize any (node, bindings) tuples into bound NodeOutputs
        normalized_branches: Dict[Any | Callable[[Any], bool], Any] = {}
        for cond, target in branches.items():
            bound_target = target
            if isinstance(target, tuple) and len(target) == 2:
                cand_node, cand_bind = target
                if isinstance(cand_bind, dict) and (
                    hasattr(cand_node, "node_id") or isinstance(cand_node, str)
                ):
                    bound_target = self.bind(cand_node, **cand_bind)
            normalized_branches[cond] = bound_target

        node_def = NodeDefinition(
            id=node_name,
            func=_case_placeholder,
            dependencies=dependencies,
            retries=0,
            kind="case",
            meta={
                "branches": normalized_branches,
            },
        )
        self._nodes[node_name] = node_def
        return NodeOutput(node_id=node_name, output_type=Any)

    def bind(
        self, target: NodeOutput[Any] | str, /, **bindings: Any
    ) -> NodeOutput[Any]:
        """
        Creates a new node by binding specific arguments for an existing node definition.

        This enables expressions like workflow.case(..., branches={cond: wf.bind(ship_node, arg=other_node)})
        """
        if isinstance(target, str):
            orig_id = target
        else:
            orig_id = target.node_id

        if orig_id not in self._nodes:
            raise DefinitionError(f"Cannot bind unknown node id: {orig_id}")

        orig = self._nodes[orig_id]
        # build new id
        suffix = 1
        base_name = f"{orig.id}__bind"
        new_id = base_name
        while new_id in self._nodes:
            suffix += 1
            new_id = f"{base_name}_{suffix}"

        # dependencies: copy and overlay NodeOutput bindings
        new_deps: Dict[str, NodeOutput[Any]] = dict(orig.dependencies)
        const_bindings: Dict[str, Any] = {}
        for k, v in bindings.items():
            if isinstance(v, NodeOutput):
                new_deps[k] = v
            else:
                const_bindings[k] = v

        # copy meta and extend with const bindings
        new_meta = dict(orig.meta)
        if const_bindings:
            merged_consts = dict(new_meta.get("const_bindings", {}))
            merged_consts.update(const_bindings)
            new_meta["const_bindings"] = merged_consts

        node_def = NodeDefinition(
            id=new_id,
            func=orig.func,
            dependencies=new_deps,
            retries=orig.retries,
            kind=orig.kind,
            meta=new_meta,
        )
        self._nodes[new_id] = node_def
        return NodeOutput(node_id=new_id, output_type=Any)

    def select(
        self, target: NodeOutput[Any] | str, /, **bindings: Any
    ) -> NodeOutput[Any]:
        """
        Syntactic sugar for binding a node with arguments for use in case branches.
        Equivalent to bind(target, **bindings).
        """
        return self.bind(target, **bindings)

    def dep(self, output: "NodeOutput[T]") -> T:
        """
        A small typing helper to annotate node default values.
        At runtime it returns the same NodeOutput; for type-checkers it is T.
        """
        return cast(T, output)

    def map_dep(self, output: "NodeOutput[Any]") -> Any:
        """
        Typing helper for map nodes: declare a single-item parameter for a
        `map_node`.

        Runtime behavior supports:
        - List/tuple/set of items → mapped over each item
        - Scalar item (non-list) → treated as single-item list
        - None → treated as empty list (no calls)

        Use a permissive type to match runtime flexibility and avoid invariant
        generic issues with `NodeOutput[List[T]]`.
        """
        return output

    def _build_graph(self) -> WorkflowGraph:
        graph = WorkflowGraph()
        for node_def in self._nodes.values():
            graph.add_node(node_def)
        return graph

    def set_on_success(self, handler: Callable[[Dict[str, Any], BaseContext], None]):
        self._on_success = handler

    def set_on_failure(self, handler: Callable[[str, Exception, BaseContext], None]):
        self._on_failure = handler

    def run(
        self,
        initial_data: Dict[str, Any] | None = None,
        context: BaseContext | None = None,
        executor: "Executor | str" = "concurrent",
        *,
        return_mode: str = "all",
        run_id: str | None = None,
        state_backend: Any | None = None,
        event_sink: Any | None = None,
    ) -> Dict[str, Any]:
        """
        Parses, resolves, and executes the workflow.

        This is a high-level orchestration method.
        """
        from .executors.concurrent import ConcurrentExecutor

        logger.info("Running workflow '%s'...", self.name)
        initial_data = initial_data or {}

        # Validate required root inputs
        required_inputs: set[str] = set()
        for node_def in self._nodes.values():
            sig = inspect.signature(node_def.func)
            for param_name, param in sig.parameters.items():
                if param_name in node_def.dependencies:
                    continue
                if param.default is not inspect._empty:
                    # CONTEXT sentinel or default provided
                    if param.default is CONTEXT:
                        continue
                    continue
                # skip *args/**kwargs
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                required_inputs.add(param_name)

        missing = [k for k in required_inputs if k not in initial_data]
        if missing:
            raise DefinitionError(
                f"Missing required initial_data keys: {missing}. Provide them in Workflow.run(initial_data=...)."
            )

        graph = self._build_graph()
        exec_impl: Any
        if isinstance(executor, str):
            if executor.lower() == "concurrent":
                exec_impl = ConcurrentExecutor()
            else:
                raise DefinitionError("Unknown executor: %r" % executor)
        else:
            exec_impl = executor

        # Ensure context object
        runtime_context: BaseContext = context if context is not None else BaseContext()
        from .exceptions import ExecutionError

        try:
            results: Dict[str, Any] = exec_impl.execute(
                graph=graph,
                initial_data=initial_data,
                context=runtime_context,
                run_id=run_id,
                state_backend=state_backend,
                event_sink=event_sink,
            )
        except ExecutionError as exc:
            # Attempt to parse node id from message; executors should raise with node_id attribute if possible
            node_id = getattr(exc, "node_id", "<unknown>")
            if event_sink and run_id:
                try:
                    from .services.telemetry import NodeEvent

                    event_sink.on_node_event(
                        NodeEvent(
                            run_id, node_id, "failed", time.time(), {"error": str(exc)}
                        )
                    )
                    event_sink.on_run_finished(run_id, False)
                except Exception:
                    pass
            if self._on_failure is not None:
                try:
                    self._on_failure(node_id, exc, runtime_context)
                finally:
                    pass
            raise

        if event_sink and run_id:
            try:
                event_sink.on_run_finished(run_id, True)
            except Exception:
                pass

        if self._on_success is not None:
            try:
                self._on_success(results, runtime_context)
            finally:
                pass

        if return_mode == "all":
            return results
        elif return_mode == "leaves":
            # leaves: nodes with no dependents
            dependents: Dict[str, int] = {k: 0 for k in graph.nodes}
            for _node, deps in graph.dependencies.items():
                for dep in deps:
                    dependents[dep] += 1
            leaves = {k: v for k, v in results.items() if dependents.get(k, 0) == 0}
            return leaves
        else:
            raise DefinitionError("Unsupported return_mode: %r" % return_mode)
