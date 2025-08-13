"""
Plugin protocol.
"""

from __future__ import annotations

from typing import Protocol, Dict, Any, TYPE_CHECKING, Generic, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from .engine import Engine


ProviderT = TypeVar("ProviderT", covariant=True)


@runtime_checkable
class Plugin(Protocol, Generic[ProviderT]):
    """
    The protocol for creating installable plugins.

    Plugins are used to initialize services (like database clients) and
    inject them into the workflow context.
    """

    @property
    def name(self) -> str:
        """A unique name for the plugin."""
        ...

    def setup(self, engine: "Engine", config: Dict[str, Any]):
        """
        Sets up the plugin. This method is called by the Engine at startup.
        It should initialize any clients or services.
        """
        ...

    def get_context_provider(self) -> ProviderT | None:
        """
        Returns the object to be injected into the context.
        """
        ...

    def get_context_injections(self) -> Dict[str, ProviderT] | Dict[str, Any]:
        """
        Optionally return a mapping of context attribute names to provider objects
        to be injected into a context instance.
        """
        ...

    def teardown(self):
        """Cleans up resources when the engine shuts down."""
        ...
