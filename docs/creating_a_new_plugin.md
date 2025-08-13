# Creating a New Plugin

This guide provides a comprehensive walkthrough for creating a new plugin for the `autoworkflow` system. Plugins are the primary way to extend `autoworkflow` with new capabilities, such as integrating with external services, databases, or APIs.

## What is a Plugin?

At its core, a plugin is a self-contained module that initializes a service (like a client for an API) and makes it available to your workflows. This is achieved by "injecting" the initialized service or client into the `Workflow Context`.

The key benefits of using plugins are:
- **Configuration Management**: Centralize API keys, connection strings, and other configurations.
- **Lifecycle Management**: Plugins have `setup` and `teardown` hooks, allowing for graceful startup and shutdown of services (e.g., opening/closing database connections).
- **Reusability**: Write a plugin once and use it across multiple workflows.
- **Decoupling**: Keep your workflow logic clean and separate from the implementation details of the services it uses.

## The `Plugin` Protocol

To create a valid plugin, you must implement the `Plugin` protocol defined in `autoworkflow.services.plugins`. A class implementing this protocol must have the following methods and properties:

- **`name` (property)**: A unique string identifier for the plugin (e.g., `"my_database_plugin"`).
- **`setup(engine, config)`**: Called by the `Engine` at startup. This is where you initialize your service client.
- **`get_context_provider()`**: Returns the initialized object that will be injected into the workflow context.
- **`get_context_injections()`**: An alternative to `get_context_provider` that allows injecting multiple objects into the context.
- **`teardown()`**: Called by the `Engine` on shutdown for resource cleanup.

---

## Step-by-Step Guide to Creating a Plugin

Let's create a practical example: a plugin for a fictional "Cat Facts" API.

### Step 1: Define the Plugin Class

First, create a new Python file for your plugin. For this example, let's call it `autoworkflow_plugins/cat_facts.py`.

Inside this file, define a class that implements the `Plugin` protocol.

```python
# autoworkflow_plugins/cat_facts.py

from typing import Dict, Any
import requests
from autoworkflow.services.plugins import Plugin
from autoworkflow.services.engine import Engine

# This is the client we want to inject into our workflows.
class CatFactsClient:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def get_random_fact(self) -> str:
        try:
            response = requests.get(f"{self.api_url}/fact")
            response.raise_for_status()
            return response.json().get("fact", "Could not fetch a cat fact.")
        except requests.RequestException as e:
            return f"Error fetching cat fact: {e}"

# Our plugin implementation
class CatFactsPlugin(Plugin[CatFactsClient]):
    def __init__(self):
        self._client = None

    @property
    def name(self) -> str:
        return "cat_facts"

    def setup(self, engine: "Engine", config: Dict[str, Any]):
        """
        Initialize the CatFactsClient using configuration passed to the Engine.
        """
        plugin_config = config.get(self.name, {})
        api_url = plugin_config.get("api_url", "https://catfact.ninja")
        self._client = CatFactsClient(api_url=api_url)
        print("CatFactsPlugin setup complete.")

    def get_context_provider(self) -> CatFactsClient | None:
        """
        Return the initialized client.
        The Engine will inject this into the context.
        """
        return self._client

    def get_context_injections(self) -> Dict[str, Any]:
        """
        Alternatively, you can inject multiple items.
        For this plugin, we'll stick to the simpler get_context_provider.
        """
        return {}

    def teardown(self):
        """
        Clean up resources, if any.
        """
        self._client = None
        print("CatFactsPlugin teardown complete.")

```

### Step 2: Define the Workflow Context and Protocol

The "Workflow Context" is a bridge between the `Engine` and your running `Workflow`. It's a simple Python class that you define to hold shared resources.

To make your workflow nodes more modular and easier to test, it's a best practice to also define a `Protocol` that describes the shape of the context that your nodes expect.

```python
# in your main workflow definition file, e.g., my_context.py

from typing import Protocol
from autoworkflow import BaseContext
from .cat_facts import CatFactsClient # Assuming cat_facts.py is in another file

# Best Practice: Define a Protocol for type hinting
class HasCatFacts(Protocol):
    """A protocol to identify contexts that have a cat_facts client."""
    cat_facts: CatFactsClient

# Your concrete context class for the application
class MyAppContext(BaseContext):
    # This class now implicitly satisfies the HasCatFacts protocol.
    # The attribute name 'cat_facts' MUST match the plugin's 'name' property.
    cat_facts: CatFactsClient
```

**How does the injection work?**

When a workflow is triggered, the `Engine` does the following:
1. Creates an instance of your concrete context class (`MyAppContext`).
2. Iterates through all registered plugins.
3. For each plugin, it calls `plugin.get_context_provider()`.
4. It then looks for an attribute on the context object with the **same name** as the plugin's `name` property (e.g., `cat_facts`).
5. If it finds a match, it sets the context attribute's value to the object returned by `get_context_provider()`.

### Step 3: Use the Plugin in a Workflow

Now you can access the `CatFactsClient` directly from any workflow node. By type-hinting with the `HasCatFacts` protocol, your node becomes decoupled from the concrete `MyAppContext`, making it more reusable and easier to test.

```python
# in your main workflow definition file, e.g., my_workflow.py

from autoworkflow import Workflow, CONTEXT
from .cat_facts import CatFactsPlugin # Import the plugin
from .my_context import MyAppContext, HasCatFacts # Import your context and protocol

# 1. Create a workflow instance
wf = Workflow(name="cat_facts_workflow", description="Fetches a cat fact.")

# 2. Define a node that uses the context via the protocol
@wf.node
def get_cat_fact(context: HasCatFacts = CONTEXT) -> str:
    """
    This node uses the client from the context to get a fact.
    """
    fact = context.cat_facts.get_random_fact()
    return fact

@wf.node
def print_fact(fact: str = get_cat_fact):
    """
    This node prints the fact fetched by the previous node.
    """
    print(f"Cat Fact: {fact}")

```
The `CONTEXT` sentinel tells the workflow to inject the populated context object into the `context` parameter of the `get_cat_fact` function.

### Why is using a Protocol better?

1.  **Decoupling**: The `get_cat_fact` node no longer depends on the specific `MyAppContext` class. It only cares that *some* context object with a `cat_facts` attribute is provided.
2.  **Reusability**: You could create another, completely different context for another application, and as long as it also has a `cat_facts` attribute, you could reuse this exact same node.
3.  **Easier Testing**: When writing unit tests for this node, you don't need to create a full `MyAppContext` instance. You can use a simple mock object or `SimpleNamespace` that satisfies the `HasCatFacts` protocol.

```python
# Example of testing the node
from types import SimpleNamespace

# Create a mock client
class MockCatFactsClient:
    def get_random_fact(self) -> str:
        return "A testable fact."

# Create a mock context that satisfies the protocol
mock_context = SimpleNamespace(cat_facts=MockCatFactsClient())

# Test the node's raw function
result = get_cat_fact.func(context=mock_context)
assert result == "A testable fact."
```
This approach makes your system more robust, flexible, and maintainable.

### Step 4: Register the Plugin with the Engine

The final step is to tie everything together in your main application entry point where you configure and run the `Engine`.

```python
# in your main application entry point, e.g., main.py

from autoworkflow import Engine, ScheduledTrigger
from my_workflow import wf, MyAppContext
from autoworkflow_plugins.cat_facts import CatFactsPlugin

def main():
    # 1. Create an Engine instance
    # You can pass configuration here, which gets passed to the plugin's setup method.
    engine_config = {
        "cat_facts": {
            "api_url": "https://catfact.ninja"
        }
    }
    engine = Engine(config=engine_config)

    # 2. Register your plugin with the engine
    engine.register_plugin(CatFactsPlugin())

    # 3. Set the context factory
    # The engine will call this to create a new context for each workflow run.
    engine.set_context_factory(lambda: MyAppContext())

    # 4. Register your workflow
    engine.register_workflow(wf)

    # 5. Add a trigger to run the workflow
    # For this example, we'll run it every 10 seconds.
    engine.add_trigger(
        trigger=ScheduledTrigger(cron="every 10s"),
        workflow_name=wf.name
    )

    # 6. Start the engine
    print("Starting Cat Facts Engine...")
    engine.run()

if __name__ == "__main__":
    main()
```

### How It All Connects

1.  **`engine.run()`**: The engine starts.
2.  **Plugin `setup`**: The engine calls `setup()` on the `CatFactsPlugin`. The plugin creates a `CatFactsClient` instance.
3.  **Trigger Fires**: The `ScheduledTrigger` fires.
4.  **Context Creation**: The engine calls the function provided to `set_context_factory()`, creating a new `MyAppContext` instance.
5.  **Context Injection**: The engine calls `get_context_provider()` on the `CatFactsPlugin` and gets the `CatFactsClient` instance. It sees that the plugin's name is `"cat_facts"` and assigns the client to the `my_app_context.cat_facts` attribute.
6.  **Workflow Execution**: The engine starts executing the workflow.
7.  **Node Execution**: When the `get_cat_fact` node is run, the engine passes the fully populated `MyAppContext` instance to it. The node can then access `context.cat_facts.get_random_fact()`.

This architecture ensures that your workflow logic is clean, testable, and completely decoupled from the concrete implementation of the services it relies on.
