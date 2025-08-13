"""
Custom Exceptions
This module defines custom, specific exceptions for the library,
allowing users to write more precise error handling logic.
"""


class WorkflowError(Exception):
    """Base exception for all errors raised by the autoworkflow library."""

    pass


class DefinitionError(WorkflowError):
    """An error occurred during the definition phase of a workflow (e.g., a cycle)."""

    pass


class ExecutionError(WorkflowError):
    """An error occurred during the execution of a workflow node."""

    pass


class ConfigurationError(WorkflowError):
    """An error related to engine or plugin configuration."""

    pass
