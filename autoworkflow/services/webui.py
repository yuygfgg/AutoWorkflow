"""
FastAPI-based Web UI server for AutoWorkflow.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import threading
import os


from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
import uvicorn


from .telemetry import InMemoryEventSink


class WebUIServer:
    """Embeddable HTTP server to visualize workflows and live status."""

    def __init__(self, *, host: str = "127.0.0.1", port: int = 8008):
        self.host = host
        self.port = port
        self._sink: InMemoryEventSink | None = None
        self._app: Any = None
        self._thread: Optional[threading.Thread] = None

    def set_sink(self, sink: InMemoryEventSink) -> None:
        self._sink = sink

    def _build_app(self, engine_ref: Any) -> Any:
        app = FastAPI(title="AutoWorkflow UI")

        sink = self._sink

        @app.get("/")
        def index() -> HTMLResponse:
            with open(
                os.path.join(os.path.dirname(__file__), "static", "index.html"), "r"
            ) as f:
                html = f.read()
            return HTMLResponse(content=html)

        @app.get("/runs")
        def runs() -> JSONResponse:
            data = sink.list_runs() if sink else {}
            return JSONResponse(content=data)

        @app.get("/runs/{run_id}")
        def run_detail(run_id: str) -> JSONResponse:
            data = sink.get_run(run_id) if sink else None
            return JSONResponse(content=data or {})

        def _label_condition(cond: Any) -> str:
            try:
                if not callable(cond):
                    return str(cond)
                cond_str = str(cond)
                # Heuristic: lambdas created by otherwise contain 'otherwise' in qualname
                if "otherwise" in cond_str:
                    return "otherwise"
                name = getattr(cond, "__name__", None) or getattr(
                    cond, "__qualname__", None
                )
                if name and name != "<lambda>":
                    return str(name)
                return "predicate"
            except Exception:
                return "predicate"

        @app.get("/workflows")
        def workflows() -> JSONResponse:
            out: Dict[str, Any] = {}
            for name, wf in engine_ref._workflows.items():
                graph = wf._build_graph()

                nodes_with_meta = []
                for node_id, node_def in graph.nodes.items():
                    meta = {
                        "kind": node_def.kind,
                    }
                    if node_def.kind == "case":
                        # The 'on' dependency is the source of the branches.
                        if node_def.dependencies:
                            on_dep_id = list(node_def.dependencies.values())[0].node_id
                            meta["on"] = on_dep_id

                        branches = []
                        for cond, target in node_def.meta.get("branches", {}).items():
                            branch_info: Dict[str, Any] = {"condition": _label_condition(cond)}
                            if hasattr(target, "node_id"):
                                branch_info["target_node"] = target.node_id
                            else:
                                branch_info["target_node"] = None  # constant value
                            branches.append(branch_info)

                        meta["branches"] = branches

                    nodes_with_meta.append({"id": node_id, **meta})

                # Build labeled dependency edges per argument
                deps_labeled: Dict[str, list[Dict[str, str]]] = {}
                try:
                    for node_id, node_def in graph.nodes.items():
                        labeled_items: list[Dict[str, str]] = []
                        for arg_name, dep in node_def.dependencies.items():
                            try:
                                src_id = getattr(dep, "node_id", None)
                                if not src_id:
                                    continue
                                path_parts = []
                                for kind, meta_part in getattr(dep, "access_path", ()):
                                    if kind in ("attr", "key"):
                                        path_parts.append(str(meta_part))
                                subpath = ".".join(path_parts) if path_parts else ""
                                labeled_items.append(
                                    {
                                        "from": src_id,
                                        "arg": str(arg_name),
                                        "path": subpath,
                                    }
                                )
                            except Exception:
                                continue
                        if labeled_items:
                            deps_labeled[node_id] = labeled_items
                except Exception:
                    deps_labeled = {}

                out[name] = {
                    "nodes": list(graph.nodes.keys()),
                    "nodes_meta": nodes_with_meta,
                    "dependencies": {k: list(v) for k, v in graph.dependencies.items()},
                    "dependencies_labeled": deps_labeled,
                    "description": wf.description,
                }
            return JSONResponse(content=out)

        @app.get("/events")
        def events(request: Request):
            # Server-Sent Events endpoint
            import json
            import asyncio

            assert isinstance(request, Request)

            local_sink = self._sink
            if local_sink is None:

                async def _empty():
                    yield 'data: {"type": "events", "seq": 0}\n\n'
                    await asyncio.sleep(1.0)

                return StreamingResponse(_empty(), media_type="text/event-stream")

            async def event_generator():
                # send a snapshot first
                workflows_data = {}
                for name, wf in engine_ref._workflows.items():
                    graph = wf._build_graph()
                    nodes_with_meta = []
                    for node_id, node_def in graph.nodes.items():
                        meta = {"kind": node_def.kind}
                        if node_def.kind == "case":
                            if node_def.dependencies:
                                on_dep_id = list(node_def.dependencies.values())[
                                    0
                                ].node_id
                                meta["on"] = on_dep_id
                            branches = []
                            for cond, target in node_def.meta.get(
                                "branches", {}
                            ).items():
                                branch_info: Dict[str, Any] = {"condition": _label_condition(cond)}
                                if hasattr(target, "node_id"):
                                    branch_info["target_node"] = target.node_id
                                else:
                                    branch_info["target_node"] = None
                                branches.append(branch_info)
                            meta["branches"] = branches
                        nodes_with_meta.append({"id": node_id, **meta})

                    # labeled dependencies for snapshot
                    deps_labeled: Dict[str, list[Dict[str, str]]] = {}
                    try:
                        for node_id, node_def in graph.nodes.items():
                            labeled_items: list[Dict[str, str]] = []
                            for arg_name, dep in node_def.dependencies.items():
                                try:
                                    src_id = getattr(dep, "node_id", None)
                                    if not src_id:
                                        continue
                                    path_parts = []
                                    for kind, meta_part in getattr(dep, "access_path", ()):
                                        if kind in ("attr", "key"):
                                            path_parts.append(str(meta_part))
                                    subpath = ".".join(path_parts) if path_parts else ""
                                    labeled_items.append(
                                        {
                                            "from": src_id,
                                            "arg": str(arg_name),
                                            "path": subpath,
                                        }
                                    )
                                except Exception:
                                    continue
                            if labeled_items:
                                deps_labeled[node_id] = labeled_items
                    except Exception:
                        deps_labeled = {}

                    workflows_data[name] = {
                        "nodes": list(graph.nodes.keys()),
                        "nodes_meta": nodes_with_meta,
                        "dependencies": {
                            k: list(v) for k, v in graph.dependencies.items()
                        },
                        "dependencies_labeled": deps_labeled,
                        "description": wf.description,
                    }

                snap = json.dumps(
                    {
                        "type": "snapshot",
                        "seq": 0,
                        "runs": local_sink.list_runs(),
                        "workflows": workflows_data,
                    }
                )
                yield f"data: {snap}\n\n"
                last = 0
                while True:
                    if await request.is_disconnected():
                        break
                    # block until new seq in a worker thread
                    latest = await asyncio.to_thread(
                        local_sink.wait_for_new_events, last, 15.0
                    )
                    if latest > last:
                        events, last = local_sink.get_events_since(last)
                        data = json.dumps(
                            {"type": "events", "seq": last, "events": events}
                        )
                        yield f"data: {data}\n\n"

            return StreamingResponse(event_generator(), media_type="text/event-stream")

        return app

    def start(self, engine_ref: Any) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._app = self._build_app(engine_ref)

        def _run():
            uvicorn.run(self._app, host=self.host, port=self.port, log_level="info")

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
