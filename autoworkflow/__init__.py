"""
AutoWorkflow: Public API
This module defines the primary user-facing API for the autoworkflow library,
making key components directly accessible to the user.
"""

from .core import Workflow, BaseContext, CONTEXT
from .services.engine import Engine
from .services.webui import WebUIServer
from .services.triggers import ScheduledTrigger
from .exceptions import WorkflowError, DefinitionError, ExecutionError

__all__ = [
    "Workflow",
    "BaseContext",
    "CONTEXT",
    "Engine",
    "WebUIServer",
    "ScheduledTrigger",
    "WorkflowError",
    "DefinitionError",
    "ExecutionError",
]
