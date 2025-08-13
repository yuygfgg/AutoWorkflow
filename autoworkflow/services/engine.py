"""
The long-running service Engine for event-driven workflows.
"""

from __future__ import annotations

from typing import List, Dict, Any, Optional, Callable
import uuid
import time
import logging
from concurrent.futures import ThreadPoolExecutor

from ..core import Workflow, BaseContext
from .triggers import BaseTrigger
from .plugins import Plugin
from .telemetry import InMemoryEventSink, NodeEventSink
from .webui import WebUIServer

logger = logging.getLogger(__name__)


class Engine:
    """
    Manages the lifecycle of workflows, plugins, and triggers for
    creating long-running, event-driven automation services.
    """

    def __init__(self, config: Optional[Dict[str, Any]] | None = None):
        self.config = config or {}
        self._workflows: Dict[str, Workflow] = {}
        self._triggers: List[BaseTrigger] = []
        self._plugins: Dict[str, Plugin] = {}
        # Thread pool to execute workflow runs triggered by events
        max_workers = int(self.config.get("event_max_workers", 8))
        self._event_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="aw-event")
        self._context_factory: Optional[Callable[[], BaseContext]] = None
        self._state_backend: Any | None = None
        # Optional telemetry sink and embedded Web UI server
        self._event_sink: NodeEventSink | None = None
        self._web_ui: WebUIServer | None = None

    def register_workflow(self, workflow: Workflow):
        """Registers a workflow with the engine."""
        self._workflows[workflow.name] = workflow

    def add_trigger(
        self,
        trigger: BaseTrigger,
        workflow_name: str,
        initial_data: Dict | None = None,
        context: BaseContext | None = None,
    ):
        """Binds a trigger to a specific workflow."""
        self._triggers.append(trigger)

        def _callback(payload: Dict[str, Any]):
            wf = self._workflows.get(workflow_name)
            if not wf:
                logger.warning("Workflow '%s' not found.", workflow_name)
                return
            data = {}
            if initial_data:
                data.update(initial_data)
            if payload:
                data.update(payload)
            run_context = context
            if run_context is None and self._context_factory is not None:
                try:
                    run_context = self._context_factory()
                except Exception:
                    run_context = None
            # inject plugin providers into context if present
            if run_context is not None:
                try:
                    self.inject_plugins_into_context(run_context)
                except Exception:
                    pass
            run_id = uuid.uuid4().hex
            # notify run started
            try:
                if self._event_sink:
                    self._event_sink.on_run_started(run_id, wf.name)
            except Exception:
                pass

            def _run_workflow():
                wf.run(
                    initial_data=data,
                    context=run_context,
                    executor="concurrent",
                    run_id=run_id,
                    state_backend=self._state_backend,
                    event_sink=self._event_sink,
                )

            try:
                self._event_executor.submit(_run_workflow)
            except Exception:
                logger.exception("Failed to submit workflow run to executor")

        trigger._autoworkflow_bound_start = _callback  # type: ignore[attr-defined]

    def run(self):
        """Starts the engine and all its triggers, running indefinitely."""
        logger.info("Automation engine is starting...")
        try:
            self.setup_plugins()
            # Start Web UI if configured
            if self._web_ui is not None:
                try:
                    self._web_ui.start(self)
                except Exception:
                    logger.exception("Failed to start Web UI server")
            for trg in self._triggers:
                start_fn = getattr(trg, "_autoworkflow_bound_start", None)
                attach = getattr(trg, "attach", None)
                if callable(attach):
                    attach(start_fn if start_fn else (lambda _payload: None))
                else:
                    logger.warning("Trigger missing attach(); skipping bind")

            # Cooperative polling loop
            while True:
                for trg in self._triggers:
                    tick = getattr(trg, "tick", None)
                    if callable(tick):
                        try:
                            tick()
                        except Exception:
                            logger.exception("Trigger tick() error")
                # Short sleep to avoid busy loop; triggers keep their own cadence
                time.sleep(0.05)
        except KeyboardInterrupt:
            logger.info("Automation engine shutting down.")
            # teardown plugins on shutdown
            for plugin in self._plugins.values():
                try:
                    plugin.teardown()
                except Exception:
                    pass
            try:
                self._event_executor.shutdown(wait=False, cancel_futures=False)
            except Exception:
                pass

    def register_plugin(self, plugin: Plugin):
        self._plugins[plugin.name] = plugin

    def setup_plugins(self):
        for plugin in self._plugins.values():
            try:
                plugin.setup(self, self.config)
            except Exception:
                logger.exception("Plugin '%s' setup failed", plugin.name)

    def set_context_factory(self, factory: Callable[[], BaseContext]):
        self._context_factory = factory

    def set_state_backend(self, backend: Any):
        self._state_backend = backend

    def inject_plugins_into_context(self, context: BaseContext) -> BaseContext:
        for name, plugin in self._plugins.items():
            # Preferred: bulk injections mapping
            injections: Dict[str, Any] | None = None
            try:
                injections = plugin.get_context_injections()
            except Exception:
                injections = None
            if injections:
                for attr, provider in injections.items():
                    try:
                        setattr(context, attr, provider)
                    except Exception:
                        # Some user-defined contexts may restrict attribute setting (e.g., __slots__)
                        # In that case, just skip this attribute gracefully.
                        pass
                continue

            provider = None
            try:
                provider = plugin.get_context_provider()
            except Exception:
                provider = None
            if provider is None:
                continue
            try:
                setattr(context, name, provider)
            except Exception:
                pass
        return context

    def enable_web_ui(self, *, host: str = "127.0.0.1", port: int = 8008) -> None:
        """
        Enable and start a background Web UI server.
        """
        if self._event_sink is None:
            self._event_sink = InMemoryEventSink()
        srv = WebUIServer(host=host, port=port)
        srv.set_sink(self._event_sink)  # type: ignore[arg-type]
        self._web_ui = srv

    def set_event_sink(self, sink: NodeEventSink) -> None:
        """Set a custom event sink to consume run/node events (for external UI)."""
        self._event_sink = sink
