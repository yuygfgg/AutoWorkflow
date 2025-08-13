# How to Create a New Trigger in AutoWorkflow

This guide provides a comprehensive, step-by-step explanation of how to create a custom trigger for your workflows. Triggers are the starting point of your automation; they listen for events and kick off a workflow run with a specific payload.

## 1. Understanding Triggers

In AutoWorkflow, a **Trigger** is a Python class responsible for monitoring an external event source. When a specific event occurs (e.g., a file is created, a web request is received, a scheduled time is reached), the trigger's job is to notify the `Engine`. The `Engine` then starts the corresponding workflow, passing along any data (the "payload") from the trigger.

All triggers must adhere to a simple interface defined by the `BaseTrigger` protocol.

## 2. The `BaseTrigger` Protocol

The `BaseTrigger` protocol is located in `autoworkflow.services.triggers`. It defines two essential methods that your custom trigger must implement:

- **`attach(self, callback: Callable[[Dict[str, Any]], None]) -> None`**:
    - The `Engine` calls this method once when you add the trigger.
    - Its purpose is to "attach" a callback function to your trigger.
    - You **must** save this `callback` function to an instance variable (e.g., `self._callback`) so you can call it later when your trigger's condition is met.
    - The `callback` function expects a single argument: a dictionary payload.

- **`tick(self, now: Optional[float] = None) -> None`**:
    - The `Engine` calls this method repeatedly in a loop.
    - This is where you should implement the core logic of your trigger. Inside `tick`, you will check if your event has occurred.
    - If the event has occurred, you should:
        1.  Prepare a payload dictionary with relevant data.
        2.  Call the `self._callback` function with the payload.

## 3. Step-by-Step: Creating a `FileExistsTrigger`

Let's create a new trigger that watches for a specific file to appear in a directory. When the file is found, it will trigger the workflow and then delete the file to prevent re-triggering.

### Step 3.1: Define the Class

Create a new Python class. It's good practice to have it inherit from `object` or nothing, as it only needs to conform to the `BaseTrigger` protocol (duck typing).

```python
import os
import time
import logging
from typing import Callable, Dict, Any, Optional

logger = logging.getLogger(__name__)

class FileExistsTrigger:
    """
    Triggers a workflow when a specific file is found in a directory.
    The file is consumed (deleted) after triggering.
    """
    def __init__(self, watch_dir: str, filename: str, payload_key: str = "found_file_path"):
        self.watch_dir = os.path.abspath(watch_dir)
        self.filename_to_watch = filename
        self.payload_key = payload_key
        self._callback: Optional[Callable[[Dict[str, Any]], None]] = None

        # Ensure the watch directory exists
        if not os.path.isdir(self.watch_dir):
            logger.warning(f"Watch directory '{self.watch_dir}' does not exist. Creating it.")
            os.makedirs(self.watch_dir, exist_ok=True)

```

**Explanation:**
- The `__init__` method takes configuration parameters: the directory to watch and the filename to look for.
- `payload_key` allows the user to customize the key under which the file path will be available in the workflow's initial data.
- We initialize `self._callback` to `None`. It will be set by the `Engine` via the `attach` method.

### Step 3.2: Implement the `attach` Method

This method is straightforward. You just need to save the callback function provided by the engine.

```python
# Inside the FileExistsTrigger class

    def attach(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        Called by the Engine to attach the trigger's callback.
        """
        logger.info(
            f"FileExistsTrigger attached for file '{self.filename_to_watch}' in '{self.watch_dir}'"
        )
        self._callback = callback
```

### Step 3.3: Implement the `tick` Method

This is the core of your trigger. The `Engine` will call this method periodically.

```python
# Inside the FileExistsTrigger class

    def tick(self, now: Optional[float] = None) -> None:
        """
        Called by the Engine in a loop. Checks for the existence of the file.
        """
        # Do nothing if the trigger hasn't been attached yet.
        if self._callback is None:
            return

        target_file_path = os.path.join(self.watch_dir, self.filename_to_watch)

        if os.path.exists(target_file_path):
            logger.info(f"Found trigger file: {target_file_path}")

            # 1. Prepare the payload
            payload = {self.payload_key: target_file_path}

            try:
                # 2. Call the callback to start the workflow
                self._callback(payload)

                # 3. Consume the file to prevent re-triggering
                os.remove(target_file_path)
                logger.info(f"Consumed trigger file: {target_file_path}")

            except Exception:
                logger.exception(
                    f"FileExistsTrigger callback failed for file '{target_file_path}'"
                )

```

**Explanation:**
1.  We first check if `self._callback` has been set.
2.  We construct the full path to the file we're looking for.
3.  If `os.path.exists()` returns `True`, our event has occurred.
4.  **Passing the Payload**: We create a `payload` dictionary. The key is `self.payload_key` (which defaults to `"found_file_path"`) and the value is the path to the file that was found. This dictionary becomes the `initial_data` for the workflow run.
5.  We call `self._callback(payload)`, which signals the `Engine` to start the workflow.
6.  We wrap the call in a `try...except` block, which is good practice.
7.  Finally, we delete the file so that the trigger doesn't fire again for the same file on the next `tick`.

## 4. Using the New Trigger

Now that you've defined `FileExistsTrigger`, here is how you would use it in an `autoworkflow` application.

```python
# main.py
from autoworkflow import Workflow, Engine
from your_triggers import FileExistsTrigger # Assuming you saved the class in your_triggers.py

# 1. Define a workflow that uses the payload from the trigger
wf = Workflow(name="file_processor", description="Processes a file found by the trigger.")

@wf.node
def process_file(found_file_path: str):
    """
    This node receives the file path from our trigger's payload.
    The argument name 'found_file_path' must match the payload_key.
    """
    print(f"Workflow received file: {found_file_path}")
    with open(found_file_path, 'w') as f:
        f.write("This file has been processed by the workflow.")
    print("File processing complete.")

# 2. Set up the Engine
engine = Engine()
engine.register_workflow(wf)

# 3. Create an instance of your new trigger
# This trigger will watch the 'incoming/' directory for a file named 'trigger.txt'
trigger = FileExistsTrigger(watch_dir="incoming", filename="trigger.txt")

# 4. Add the trigger to the engine and bind it to the workflow
engine.add_trigger(
    trigger=trigger,
    workflow_name="file_processor"
)

# 5. Run the engine
if __name__ == "__main__":
    print("Engine starting. To trigger the workflow, create a file named 'trigger.txt' in the 'incoming/' directory.")
    # You can do this from another terminal: `mkdir incoming && touch incoming/trigger.txt`
    engine.run()
```

### How the Payload is Passed

- In `FileExistsTrigger`, we created the payload: `{'found_file_path': '/path/to/incoming/trigger.txt'}`.
- The `Engine` receives this payload and passes it as `initial_data` to `workflow.run()`.
- The `process_file` node in our workflow has a parameter named `found_file_path`. AutoWorkflow automatically matches the key from the payload to the parameter name and injects the value.

This completes the guide on creating and using a custom trigger. You can adapt this pattern to create triggers for any event source you need, such as listening on a network socket, polling a database, or integrating with a message queue.
