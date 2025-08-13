"""
Base protocol for all Executor implementations.
"""

from __future__ import annotations

from typing import Protocol, Dict, Any, Optional
from ..core import BaseContext
from ..graph import WorkflowGraph


class Executor(Protocol):
    """
    The protocol (interface) that all execution strategies must implement.
    """

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
        """
        Executes a resolved workflow graph.

        Args:
            graph: The machine-readable workflow graph.
            initial_data: Input data for the root nodes.
            context: The workflow context object.
            run_id: Optional unique id for this run, used for persistence/recovery.
            state_backend: Optional state backend used to persist node results.

        Returns:
            A dictionary containing the outputs of the leaf nodes.
        """

        ...
