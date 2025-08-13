from autoworkflow.core import Workflow


def test_map_node():
    workflow = Workflow(name="Test Map Node")

    @workflow.node
    def get_items():
        return ["item1", "item2", "item3"]

    # The function passed to map_node should operate on a single item.
    # The dependency should be on the node that returns the list.
    def _process_item_impl(item: str = workflow.map_dep(get_items)):
        return item.upper()

    _ = workflow.map_node(name="process_item")(_process_item_impl)

    result = workflow.run(return_mode="leaves")
    # The default name of the map_node is the function name.
    assert result.get("process_item") == ["ITEM1", "ITEM2", "ITEM3"]


def test_map_node_concurrent():
    workflow = Workflow(name="Test Map Node Concurrent")

    @workflow.node
    def get_items():
        return ["item1", "item2", "item3"]

    def _process_item_impl(item: str = workflow.map_dep(get_items)):
        return item.upper()

    _ = workflow.map_node(name="process_item")(_process_item_impl)

    result = workflow.run(return_mode="leaves", executor="concurrent")
    assert result.get("process_item") == ["ITEM1", "ITEM2", "ITEM3"]


def test_case_statement():
    workflow = Workflow(name="Test Case Statement")

    @workflow.node
    def get_input_branch():
        return "a"

    @workflow.node
    def branch_a_node():
        return "Branch A taken"

    @workflow.node
    def branch_b_node():
        return "Branch B taken"

    case_node = workflow.case(
        on=get_input_branch,
        branches={
            "a": branch_a_node,
            "b": branch_b_node,
        },
    )

    # We need a final node to get the result from the case node.
    @workflow.node
    def final_node(case_result=case_node):
        return case_result

    result = workflow.run(return_mode="leaves")
    assert result.get(final_node.node_id) == "Branch A taken"


def test_case_statement_concurrent():
    workflow = Workflow(name="Test Case Statement Concurrent")

    @workflow.node
    def get_input_branch():
        return "a"

    @workflow.node
    def branch_a_node():
        return "Branch A taken"

    @workflow.node
    def branch_b_node():
        return "Branch B taken"

    case_node = workflow.case(
        on=get_input_branch,
        branches={
            "a": branch_a_node,
            "b": branch_b_node,
        },
    )

    @workflow.node
    def final_node(case_result=case_node):
        return case_result

    result = workflow.run(return_mode="leaves", executor="concurrent")
    assert result.get(final_node.node_id) == "Branch A taken"
