"""State backend protocol for persisting workflow state."""

from __future__ import annotations

from typing import Protocol, Dict, Any


class StateBackend(Protocol):
    """
    The protocol for state backends, allowing workflow state to be
    persisted across runs or for distributed executors.

    This is an advanced feature for fault tolerance and recovery.
    """

    def save_result(self, run_id: str, node_id: str, result: Any):
        """Saves the result of a single node for a given workflow run."""
        ...

    def load_result(self, run_id: str, node_id: str) -> Any:
        """Loads the previously saved result of a node."""
        ...

    def get_run_state(self, run_id: str) -> Dict[str, Any]:
        """Retrieves the entire state (all node results) for a given run."""
        ...
