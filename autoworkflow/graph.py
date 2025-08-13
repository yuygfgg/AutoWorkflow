"""
Internal Graph Representation
This module handles the internal representation of the workflow as a graph,
and provides utilities for its validation and resolution.
This is not part of the public API.
"""

from __future__ import annotations

import dataclasses
from typing import Callable, Dict, Any, TYPE_CHECKING, Literal
from graphlib import TopologicalSorter

from .exceptions import DefinitionError

if TYPE_CHECKING:
    from .core import NodeOutput


@dataclasses.dataclass
class NodeDefinition:
    """Internal metadata for a single node."""

    id: str
    func: Callable[..., Any]
    dependencies: Dict[str, "NodeOutput"]
    retries: int
    kind: Literal["normal", "map", "case"] = "normal"
    meta: Dict[str, Any] = dataclasses.field(default_factory=dict)


class WorkflowGraph:
    """Representation of the workflow."""

    def __init__(self):
        self.nodes: Dict[str, NodeDefinition] = {}
        self.dependencies: Dict[str, set[str]] = {}

    def add_node(self, node_def: NodeDefinition):
        """Adds a node and its dependencies to the graph."""
        from .core import NodeOutput

        self.nodes[node_def.id] = node_def
        dep_ids: set[str] = set()
        for dep in node_def.dependencies.values():
            if isinstance(dep, NodeOutput):
                dep_ids.add(dep.node_id)
        self.dependencies[node_def.id] = dep_ids

    def resolve(self) -> TopologicalSorter:
        """
        Performs a topological sort on the graph to get an execution plan.

        Raises:
            DefinitionError: If a cycle is detected in the graph.
        """
        sorter = TopologicalSorter(self.dependencies)
        try:
            sorter.prepare()
            return sorter
        except Exception as exc:
            raise DefinitionError(f"Cycle detected in workflow graph: {exc}")
