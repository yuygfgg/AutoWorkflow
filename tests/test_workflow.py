import pytest
from autoworkflow.core import Workflow, DefinitionError


def fetch_data(source_id: str):
    """A mock function to simulate fetching data."""
    return {"id": source_id, "data": "raw_data"}


def process_data(raw_data: dict):
    """A mock function to simulate processing data."""
    processed_data = raw_data.copy()
    processed_data["processed"] = True
    return processed_data


def publish_results(processed: dict, destination: str):
    """A mock function to simulate publishing results."""
    return f"Published {processed['id']} to {destination}"


def test_simple_linear_workflow():
    data_pipeline = Workflow(name="Test Simple Pipeline")

    # Create nodes and define dependencies using default values
    fetch_node = data_pipeline.node(fetch_data)

    @data_pipeline.node
    def process_node(raw_data: dict = data_pipeline.dep(fetch_node)):
        return process_data(raw_data)

    @data_pipeline.node
    def publish_node(
        processed: dict = data_pipeline.dep(process_node),
        destination: str = "default_dest",
    ):
        return publish_results(processed, destination)

    # Run the workflow
    results = data_pipeline.run(
        initial_data={"source_id": "test_db_1", "destination": "test_bucket"}
    )

    assert results.get(publish_node.node_id) == "Published test_db_1 to test_bucket"


def test_simple_linear_workflow_concurrent():
    data_pipeline = Workflow(name="Test Simple Pipeline Concurrent")

    fetch_node = data_pipeline.node(fetch_data)

    @data_pipeline.node
    def process_node(raw_data: dict = data_pipeline.dep(fetch_node)):
        return process_data(raw_data)

    @data_pipeline.node
    def publish_node(
        processed: dict = data_pipeline.dep(process_node),
        destination: str = "default_dest",
    ):
        return publish_results(processed, destination)

    results = data_pipeline.run(
        initial_data={
            "source_id": "test_db_1",
            "destination": "test_bucket",
        },
        executor="concurrent",
    )

    assert results.get(publish_node.node_id) == "Published test_db_1 to test_bucket"


def test_missing_initial_data():
    data_pipeline = Workflow(name="Test Missing Data")
    data_pipeline.node(fetch_data)  # fetch_data requires source_id

    with pytest.raises(DefinitionError, match="Missing required initial_data keys"):
        data_pipeline.run(initial_data={})  # Missing 'source_id'


def test_dependency_resolution_and_result():
    workflow = Workflow(name="Test Dependency Resolution")

    @workflow.node
    def node_a():
        return "A"

    @workflow.node
    def node_b(a_out: str = workflow.dep(node_a)):
        return "B" + a_out

    @workflow.node
    def node_c(b_out: str = workflow.dep(node_b)):
        return "C" + b_out

    result = workflow.run()
    assert result == {node_a.node_id: "A", node_b.node_id: "BA", node_c.node_id: "CBA"}

    # Test with return_mode='leaves'
    result_leaves = workflow.run(return_mode="leaves")
    assert result_leaves == {node_c.node_id: "CBA"}
