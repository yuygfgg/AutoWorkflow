"""
A concurrent executor using a thread pool with dynamic scheduling.

This implementation executes ready nodes immediately and, as soon as any
node completes, advances the topological sorter to schedule newly-ready
nodes without waiting for the entire "layer" to finish. It also parallelizes
`map` items at the item-level while keeping orchestration on the main thread
to avoid thread-pool self-deadlocks.
"""

from __future__ import annotations

from typing import Dict, Any, Optional, cast, List, Tuple
import inspect
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED
import time
import logging

from .base import Executor
from ..core import BaseContext
from ..exceptions import ExecutionError
from ..graph import WorkflowGraph

logger = logging.getLogger(__name__)


class ConcurrentExecutor(Executor):
    """
    Executes nodes in parallel using a thread pool.
    This is the default executor for the Engine.
    """

    def __init__(self, max_workers: int | None = None):
        self.max_workers = max_workers

    def execute(
        self,
        graph: WorkflowGraph,
        initial_data: Dict[str, Any],
        context: BaseContext,
        *,
        run_id: Optional[str] = None,
        state_backend: Any | None = None,
        event_sink: Any | None = None,
    ) -> Dict[str, Any]:
        logger.info(
            "Executing workflow with ConcurrentExecutor (max_workers=%s)...",
            self.max_workers,
        )

        # Dynamic, event-driven scheduling
        results: Dict[str, Any] = dict(initial_data)
        disabled_nodes: set[str] = set()
        sorter = graph.resolve()

        # Build maps for case branches to prevent premature scheduling of non-selected targets
        case_to_targets: Dict[str, List[str]] = {}
        target_to_case: Dict[str, str] = {}
        for _nid, _ndef in graph.nodes.items():
            if _ndef.kind == "case":
                _branches = _ndef.meta.get("branches", {})
                targets: List[str] = []
                for _cond, _t in _branches.items():
                    if hasattr(_t, "node_id"):
                        tid = cast(Any, _t).node_id  # type: ignore[attr-defined]
                        targets.append(tid)
                        target_to_case[tid] = _ndef.id
                if targets:
                    case_to_targets[_ndef.id] = targets
        logger.debug(
            "[INIT] case_to_targets=%s target_to_case=%s",
            case_to_targets,
            target_to_case,
        )
        # Log graph roots (nodes with no dependencies)
        try:
            roots = [nid for nid, deps in graph.dependencies.items() if not deps]
            logger.debug("[INIT] graph roots=%s", roots)
        except Exception:
            pass

        # Track case resolution and selected target
        resolved_cases: set[str] = set()
        allowed_target_for_case: Dict[str, str] = {}

        # helpers
        def _apply_access_path(
            value: Any, access_path: tuple[tuple[str, Any], ...]
        ) -> Any:
            current = value
            for kind, meta in access_path:
                if kind == "attr":
                    current = getattr(current, meta)
                elif kind == "key":
                    current = current[meta]
            return current

        def _resolve_dep_value(dep) -> Any:
            base = results[dep.node_id]
            return _apply_access_path(base, getattr(dep, "access_path", ()))

        def _run_with_retries(
            func, kwargs: Dict[str, Any], retries: int, node_id: str
        ) -> Any:
            attempt = 0
            delay = 0.1
            while True:
                try:
                    return func(**kwargs)
                except Exception as exc:
                    if attempt >= retries:
                        err = ExecutionError(
                            f"Node '{node_id}' failed after {attempt+1} attempt(s): {exc}"
                        )
                        setattr(err, "node_id", node_id)
                        raise err
                    time.sleep(delay)
                    delay = min(delay * 2, 2.0)
                    attempt += 1

        # Internal envelope for case decisions
        class _CaseSelection:
            def __init__(self, kind: str, value: Any, disabled: List[str]):
                self.kind = kind  # 'constant' or 'target'
                self.value = value  # constant value or target node id
                self.disabled = disabled

        def _run_case(on_val: Any, branches_map: Dict[Any, Any]) -> _CaseSelection:
            disabled: List[str] = []
            chosen: Any = None
            chosen_is_target = False
            for cond, target in branches_map.items():
                matched = False
                if callable(cond):
                    try:
                        matched = bool(cond(on_val))
                    except Exception:
                        matched = False
                else:
                    matched = cond == on_val
                if matched:
                    if hasattr(target, "node_id"):
                        chosen = cast(Any, target).node_id  # type: ignore[attr-defined]
                        chosen_is_target = True
                    else:
                        chosen = target
                    # collect other branch node ids to disable
                    for _cond2, _target2 in branches_map.items():
                        if hasattr(_target2, "node_id"):
                            tid = cast(Any, _target2).node_id  # type: ignore[attr-defined]
                            if not (chosen_is_target and tid == chosen):
                                disabled.append(tid)
                    break
            if chosen_is_target:
                return _CaseSelection("target", cast(str, chosen), disabled)
            return _CaseSelection("constant", chosen, disabled)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            # Orchestration state
            future_to_info: Dict[Future, Tuple[str, str, Optional[int]]] = {}
            # kind: 'node' | 'case' | 'map_item'; for 'map_item' we keep item index in the tuple
            map_results: Dict[str, List[Any]] = {}
            map_expected_counts: Dict[str, int] = {}
            map_remaining_counts: Dict[str, int] = {}
            case_pending: Dict[str, str] = {}  # case_id -> target_node_id
            deferred_ready: set[str] = set()  # nodes gated by unresolved case

            def finalize_downstream_cases(completed_node_id: str):
                """Recursively finalize cases that depend on the newly completed node."""
                # Find cases that were waiting for the node that just finished.
                to_finalize_now: List[str] = [
                    cid for cid, tid in case_pending.items() if tid == completed_node_id
                ]

                for case_id in to_finalize_now:
                    logger.debug(
                        "[FINALIZE_CHAIN] Finalizing case %s because its target %s completed.",
                        case_id,
                        completed_node_id,
                    )

                    # The result of the case is the result of its target.
                    results[case_id] = results[completed_node_id]

                    if state_backend and run_id:
                        try:
                            state_backend.save_result(run_id, case_id, results[case_id])
                        except Exception:
                            pass
                    if event_sink and run_id:
                        try:
                            from ..services.telemetry import NodeEvent

                            event_sink.on_node_event(
                                NodeEvent(
                                    run_id,
                                    case_id,
                                    "finished",
                                    time.time(),
                                    {"value": None},
                                )
                            )
                        except Exception:
                            pass

                    # Mark the case as done in the sorter.
                    sorter.done(case_id)
                    del case_pending[case_id]

                    # IMPORTANT: A case completing might complete another case waiting on it. Recurse.
                    finalize_downstream_cases(case_id)

            def schedule_ready_nodes() -> None:
                ready_nodes = list(sorter.get_ready())
                if ready_nodes:
                    logger.debug("[SCHEDULE] Ready nodes: %s", ready_nodes)
                else:
                    logger.debug("[SCHEDULE] No ready nodes at this tick")
                for node_id in ready_nodes:
                    # Skip if already computed or disabled
                    if node_id in results or node_id in disabled_nodes:
                        logger.debug(
                            "[SCHEDULE] Skipping %s (computed=%s disabled=%s)",
                            node_id,
                            node_id in results,
                            node_id in disabled_nodes,
                        )
                        sorter.done(node_id)
                        continue

                    # Gate case branch targets: don't schedule them until case is resolved and selected
                    if node_id in target_to_case:
                        case_id = target_to_case[node_id]
                        if case_id not in resolved_cases:
                            # Defer scheduling; case decision will enable the selected target later
                            deferred_ready.add(node_id)
                            logger.debug(
                                "[SCHEDULE] Deferring branch target %s waiting case %s",
                                node_id,
                                case_id,
                            )
                            continue
                        selected = allowed_target_for_case.get(case_id)
                        if selected is not None and (
                            selected == "__CONST__" or node_id != selected
                        ):
                            # Non-selected (or constant) branch targets: mark skipped and finalize
                            disabled_nodes.add(node_id)
                            logger.debug(
                                "[SCHEDULE] Disabling non-selected branch %s for case %s (selected=%s)",
                                node_id,
                                case_id,
                                selected,
                            )
                            if event_sink and run_id:
                                try:
                                    from ..services.telemetry import NodeEvent

                                    event_sink.on_node_event(
                                        NodeEvent(
                                            run_id,
                                            node_id,
                                            "finished",
                                            time.time(),
                                            {"status": "skipped"},
                                        )
                                    )
                                except Exception:
                                    pass
                            sorter.done(node_id)
                            continue

                    node_def = graph.nodes[node_id]
                    sig = inspect.signature(node_def.func)
                    kwargs: Dict[str, Any] = {}

                    # Dependencies (all should be available since sorter marked them ready)
                    for arg_name, dep in node_def.dependencies.items():
                        kwargs[arg_name] = _resolve_dep_value(dep)

                    # Context and initial/root args
                    for param_name, param in sig.parameters.items():
                        if param_name in kwargs:
                            continue
                        default_val = param.default
                        if default_val is not inspect._empty:
                            from ..core import CONTEXT as CONTEXT_SENTINEL

                            if default_val is CONTEXT_SENTINEL:
                                kwargs[param_name] = context
                                continue
                        if param_name in results:
                            kwargs[param_name] = results[param_name]

                    # Apply const bindings for normal and map
                    for ck, cv in node_def.meta.get("const_bindings", {}).items():
                        kwargs.setdefault(ck, cv)

                    if event_sink and run_id:
                        try:
                            from ..services.telemetry import (
                                NodeEvent,
                            )  # local import to avoid cycles
                        except Exception:
                            NodeEvent = None  # type: ignore
                        if NodeEvent is not None:
                            try:
                                # Mark case branches as 'scheduled' only when they are truly scheduled.
                                # For normal nodes mark scheduled here.
                                if node_def.kind != "case":
                                    event_sink.on_node_event(
                                        NodeEvent(
                                            run_id, node_id, "scheduled", time.time()
                                        )
                                    )
                            except Exception:
                                pass

                    if node_def.kind == "map":
                        logger.debug("[MAP] Scheduling map %s", node_id)
                        mapped_param_name = cast(
                            str, node_def.meta.get("mapped_param_name")
                        )
                        iterable = kwargs.get(mapped_param_name)
                        if iterable is None:
                            items: List[Any] = []
                        elif isinstance(iterable, (list, tuple, set)):
                            items = list(iterable)
                        else:
                            items = [iterable]

                        map_results[node_id] = [None] * len(items)
                        map_expected_counts[node_id] = len(items)
                        map_remaining_counts[node_id] = len(items)

                        if len(items) == 0:
                            # Immediately finalize empty map
                            results[node_id] = []
                            if state_backend and run_id:
                                try:
                                    state_backend.save_result(
                                        run_id, node_id, results[node_id]
                                    )
                                except Exception:
                                    pass
                            sorter.done(node_id)
                            continue

                        for idx, item in enumerate(items):
                            per_kwargs: Dict[str, Any] = {mapped_param_name: item}
                            # Ensure const bindings applied for each item
                            for ck, cv in node_def.meta.get(
                                "const_bindings", {}
                            ).items():
                                per_kwargs.setdefault(ck, cv)
                            logger.debug("[MAP] %s[%d] -> schedule", node_id, idx)
                            fut = pool.submit(
                                _run_with_retries,
                                node_def.func,
                                per_kwargs,
                                node_def.retries,
                                node_id,
                            )
                            future_to_info[fut] = (node_id, "map_item", idx)

                    elif node_def.kind == "case":
                        # Now emit 'scheduled' for the case meta-node itself
                        if event_sink and run_id:
                            try:
                                from ..services.telemetry import NodeEvent

                                event_sink.on_node_event(
                                    NodeEvent(run_id, node_id, "scheduled", time.time())
                                )
                            except Exception:
                                pass
                        on_value = kwargs.get("on")
                        branches = node_def.meta.get("branches", {})
                        logger.debug("[CASE] %s scheduled on=%r", node_id, on_value)

                        fut = pool.submit(_run_case, on_value, branches)
                        future_to_info[fut] = (node_id, "case", None)

                    else:
                        logger.debug("[NODE] Scheduling %s", node_id)
                        if event_sink and run_id:
                            try:
                                from ..services.telemetry import NodeEvent

                                event_sink.on_node_event(
                                    NodeEvent(run_id, node_id, "started", time.time())
                                )
                            except Exception:
                                pass
                        fut = pool.submit(
                            _run_with_retries,
                            node_def.func,
                            kwargs,
                            node_def.retries,
                            node_id,
                        )
                        future_to_info[fut] = (node_id, "node", None)

            # Initial scheduling
            schedule_ready_nodes()

            # Process completions progressively
            while sorter.is_active() or future_to_info:
                if not future_to_info:
                    # No running work but sorter shows active; schedule what we can
                    logger.debug(
                        "[LOOP] No futures; sorter_active=%s deferred=%d deferred_set=%s",
                        sorter.is_active(),
                        len(deferred_ready),
                        list(deferred_ready),
                    )
                    schedule_ready_nodes()
                    if not future_to_info:
                        # nothing schedulable; prevent tight loop
                        logger.debug("[LOOP] Idle sleep")
                        time.sleep(0.005)
                        continue

                done, _pending = wait(
                    set(future_to_info.keys()), return_when=FIRST_COMPLETED
                )
                for fut in done:
                    node_id, kind, maybe_idx = future_to_info.pop(fut)
                    if kind == "node":
                        value = fut.result()
                        logger.debug("[DONE] node %s value=%r", node_id, value)
                        results[node_id] = value
                        if state_backend and run_id:
                            try:
                                state_backend.save_result(run_id, node_id, value)
                            except Exception:
                                pass
                        if event_sink and run_id:
                            try:
                                from ..services.telemetry import NodeEvent

                                event_sink.on_node_event(
                                    NodeEvent(
                                        run_id,
                                        node_id,
                                        "finished",
                                        time.time(),
                                        {"value": None},
                                    )
                                )
                            except Exception:
                                pass
                        sorter.done(node_id)
                        finalize_downstream_cases(node_id)

                    elif kind == "map_item":
                        idx = cast(int, maybe_idx)
                        item_value = fut.result()
                        logger.debug(
                            "[DONE] map item %s[%d] value=%r", node_id, idx, item_value
                        )
                        map_results[node_id][idx] = item_value
                        map_remaining_counts[node_id] -= 1
                        if map_remaining_counts[node_id] == 0:
                            assembled = list(map_results[node_id])
                            logger.debug(
                                "[DONE] map %s assembled %d items",
                                node_id,
                                len(assembled),
                            )
                            results[node_id] = assembled
                            if state_backend and run_id:
                                try:
                                    state_backend.save_result(
                                        run_id, node_id, assembled
                                    )
                                except Exception:
                                    pass
                            if event_sink and run_id:
                                try:
                                    from ..services.telemetry import NodeEvent

                                    event_sink.on_node_event(
                                        NodeEvent(
                                            run_id,
                                            node_id,
                                            "finished",
                                            time.time(),
                                            {"value": None},
                                        )
                                    )
                                except Exception:
                                    pass
                            sorter.done(node_id)
                            finalize_downstream_cases(node_id)
                            # cleanup
                            del map_results[node_id]
                            del map_expected_counts[node_id]
                            del map_remaining_counts[node_id]

                    elif kind == "case":
                        decision = fut.result()
                        assert isinstance(decision, _CaseSelection)
                        # Mark case resolved; track disabled branches and selected target
                        resolved_cases.add(node_id)
                        for tid in decision.disabled:
                            disabled_nodes.add(tid)
                        if decision.kind == "constant":
                            logger.debug(
                                "[CASE] %s -> constant %r; disable=%s",
                                node_id,
                                decision.value,
                                decision.disabled,
                            )
                            results[node_id] = decision.value
                            if state_backend and run_id:
                                try:
                                    state_backend.save_result(
                                        run_id, node_id, decision.value
                                    )
                                except Exception:
                                    pass
                            if event_sink and run_id:
                                try:
                                    from ..services.telemetry import NodeEvent

                                    event_sink.on_node_event(
                                        NodeEvent(
                                            run_id,
                                            node_id,
                                            "finished",
                                            time.time(),
                                            {"value": None},
                                        )
                                    )
                                except Exception:
                                    pass
                            # Mark this case as a constant decision to disable all targets going forward
                            allowed_target_for_case[node_id] = "__CONST__"
                            # Disable and finalize any deferred branch targets for this case
                            # Use both metadata mapping and the decision-disabled list to be robust
                            all_branch_tids = set(
                                case_to_targets.get(node_id, [])
                            ) | set(decision.disabled)
                            for tid in all_branch_tids:
                                disabled_nodes.add(tid)
                                if tid in deferred_ready:
                                    logger.debug(
                                        "[CASE] %s constant -> skip deferred branch %s",
                                        node_id,
                                        tid,
                                    )
                                    if event_sink and run_id:
                                        try:
                                            from ..services.telemetry import NodeEvent

                                            event_sink.on_node_event(
                                                NodeEvent(
                                                    run_id,
                                                    tid,
                                                    "finished",
                                                    time.time(),
                                                    {"status": "skipped"},
                                                )
                                            )
                                        except Exception:
                                            pass
                                    try:
                                        sorter.done(tid)
                                    except Exception:
                                        pass
                                    deferred_ready.discard(tid)
                            sorter.done(node_id)
                            finalize_downstream_cases(node_id)
                        else:
                            # Wait for target node result; schedule will pick it up when ready
                            target_id = cast(str, decision.value)
                            logger.debug(
                                "[CASE] %s -> target %s; disable=%s",
                                node_id,
                                target_id,
                                decision.disabled,
                            )
                            allowed_target_for_case[node_id] = target_id
                            # For deferred nodes: schedule selected, disable others
                            for tid in case_to_targets.get(node_id, []):
                                if tid == target_id:
                                    if tid in deferred_ready:
                                        # schedule selected target now
                                        nd = graph.nodes[tid]
                                        target_sig = inspect.signature(nd.func)
                                        target_kwargs: Dict[str, Any] = {}
                                        for an, dep in nd.dependencies.items():
                                            target_kwargs[an] = _resolve_dep_value(dep)
                                        for pn, prm in target_sig.parameters.items():
                                            if pn in target_kwargs:
                                                continue
                                            dv = prm.default
                                            if dv is not inspect._empty:
                                                from ..core import (
                                                    CONTEXT as CONTEXT_SENTINEL,
                                                )

                                                if dv is CONTEXT_SENTINEL:
                                                    target_kwargs[pn] = context
                                                    continue
                                            if pn in results:
                                                target_kwargs[pn] = results[pn]
                                        for ck, cv in nd.meta.get(
                                            "const_bindings", {}
                                        ).items():
                                            target_kwargs.setdefault(ck, cv)
                                        if nd.kind == "case":
                                            if event_sink and run_id:
                                                try:
                                                    from ..services.telemetry import (
                                                        NodeEvent,
                                                    )

                                                    event_sink.on_node_event(
                                                        NodeEvent(
                                                            run_id,
                                                            tid,
                                                            "scheduled",
                                                            time.time(),
                                                        )
                                                    )
                                                except Exception:
                                                    pass
                                            on_value = target_kwargs.get("on")
                                            branches = nd.meta.get("branches", {})
                                            logger.debug(
                                                "[CASE] scheduling selected target %s (deferred case) on=%r",
                                                tid,
                                                on_value,
                                            )
                                            fut2 = pool.submit(
                                                _run_case, on_value, branches
                                            )
                                            future_to_info[fut2] = (tid, "case", None)
                                        else:
                                            if event_sink and run_id:
                                                try:
                                                    from ..services.telemetry import (
                                                        NodeEvent,
                                                    )

                                                    event_sink.on_node_event(
                                                        NodeEvent(
                                                            run_id,
                                                            tid,
                                                            "scheduled",
                                                            time.time(),
                                                        )
                                                    )
                                                except Exception:
                                                    pass
                                            if event_sink and run_id:
                                                try:
                                                    from ..services.telemetry import (
                                                        NodeEvent,
                                                    )

                                                    event_sink.on_node_event(
                                                        NodeEvent(
                                                            run_id,
                                                            tid,
                                                            "started",
                                                            time.time(),
                                                        )
                                                    )
                                                except Exception:
                                                    pass
                                            logger.debug(
                                                "[CASE] scheduling selected target %s (deferred)",
                                                tid,
                                            )
                                            fut2 = pool.submit(
                                                _run_with_retries,
                                                nd.func,
                                                target_kwargs,
                                                nd.retries,
                                                tid,
                                            )
                                            future_to_info[fut2] = (tid, "node", None)
                                        deferred_ready.discard(tid)
                                else:
                                    if tid in deferred_ready:
                                        disabled_nodes.add(tid)
                                        logger.debug(
                                            "[CASE] disabling non-selected deferred %s for case %s",
                                            tid,
                                            node_id,
                                        )
                                        if event_sink and run_id:
                                            try:
                                                from ..services.telemetry import (
                                                    NodeEvent,
                                                )

                                                event_sink.on_node_event(
                                                    NodeEvent(
                                                        run_id,
                                                        tid,
                                                        "finished",
                                                        time.time(),
                                                        {"status": "skipped"},
                                                    )
                                                )
                                            except Exception:
                                                pass
                                        try:
                                            sorter.done(tid)
                                        except Exception:
                                            pass
                                        deferred_ready.discard(tid)
                            if target_id in results:
                                logger.debug(
                                    "[CASE] %s immediate finalize from %s",
                                    node_id,
                                    target_id,
                                )
                                results[node_id] = results[target_id]
                                if state_backend and run_id:
                                    try:
                                        state_backend.save_result(
                                            run_id, node_id, results[node_id]
                                        )
                                    except Exception:
                                        pass
                                if event_sink and run_id:
                                    try:
                                        from ..services.telemetry import NodeEvent

                                        event_sink.on_node_event(
                                            NodeEvent(
                                                run_id,
                                                node_id,
                                                "finished",
                                                time.time(),
                                                {"value": None},
                                            )
                                        )
                                    except Exception:
                                        pass
                                sorter.done(node_id)
                                finalize_downstream_cases(node_id)
                            else:
                                logger.debug(
                                    "[CASE] %s pending on target %s", node_id, target_id
                                )
                                case_pending[node_id] = target_id

                # After processing completions, schedule any new ready nodes
                schedule_ready_nodes()

        return results
