import pytest

from autoworkflow.core import Workflow
from autoworkflow.exceptions import DefinitionError


def test_cycle_detection_raises_definition_error():
    wf = Workflow(name="cycle")

    @wf.node
    def a(b=wf.dep(None)):  # type: ignore[arg-type]
        # We'll patch the dependency later by directly accessing internals
        return 1

    @wf.node
    def b(a_out=wf.dep(a)):
        return 2

    # Create an artificial cycle: make 'a' depend on 'b' by mutating the internal graph
    a_id = a.node_id
    # 'b' here is the NodeOutput for node b; set it as a dependency of a
    wf._nodes[a_id].dependencies = {"b": b}

    with pytest.raises(DefinitionError, match="Cycle detected"):
        wf.run()
