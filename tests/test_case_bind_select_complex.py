import pytest

from autoworkflow.core import Workflow, DefinitionError


def test_case_with_callable_predicates_and_tuple_binding_prefers_first_match():
    wf = Workflow(name="case-callable-order")

    @wf.node
    def get_num():
        return 7

    @wf.node(name="double")
    def double(x: int = 0):
        return x * 2

    decision = wf.case(
        on=get_num,
        branches={
            # Both predicates match for 7; the first one must win and execute the tuple-bound node
            (lambda n: n > 5): ("double", {"x": get_num}),
            (lambda n: n % 2 == 1): 999,
            0: "zero",
        },
    )

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    res = wf.run()
    assert res["out"] == 14


def test_case_nested_with_select_and_constant_none():
    wf = Workflow(name="case-nested-none")

    @wf.node
    def make_item():
        return {"id": "42", "data": "raw"}

    @wf.node
    def choose_priority():
        return "none"

    @wf.node
    def ship(item: dict | None = None, priority: str = "normal", warehouse: str = "A"):
        if item is None:
            return "NOOP"
        return f"Ship {item['id']} with {priority} from {warehouse}"

    inner = wf.case(
        on=choose_priority,
        branches={
            "high": wf.select("ship", item=make_item, priority="high", warehouse="B"),
            "low": wf.select(ship, item=make_item, priority="low"),
            "none": None,
        },
    )

    # Outer case just forwards the inner result to exercise case->case wiring
    @wf.node
    def choose_priority_outer():
        return "none"

    outer = wf.case(
        on=choose_priority_outer,
        branches={
            "none": inner,
            "high": "unused",
        },
    )

    @wf.node
    def out(v=wf.dep(outer)):
        return v

    res = wf.run(executor="concurrent")
    assert res["out"] is None


@pytest.mark.parametrize(
    "init,expected",
    [
        (
            {"x": 1, "y": 2},
            3,
        ),  # initial_data should take precedence over const bindings
        ({"x": 1}, 201),  # missing key falls back to const binding
    ],
)
def test_bind_const_bindings_vs_initial_data_precedence(init, expected):
    wf = Workflow(name="bind-const-vs-initial")

    @wf.node
    def base(x: int = 0, y: int = 0):
        return x + y

    bound = wf.select(base, x=100, y=200)

    @wf.node
    def out(v=wf.dep(bound)):
        return v

    res = wf.run(initial_data=dict(init))
    assert res["out"] == expected


def test_case_tuple_bind_with_str_node_id_and_dep_key_access():
    wf = Workflow(name="case-bind-str-id-access")

    @wf.node
    def make_user():
        return {"name": "Ann", "age": 30}

    @wf.node(name="greet")
    def greet(user_name: str = ""):
        return f"Hello {user_name}!"

    # Drive selection to "A" and bind by string id with dep key access
    @wf.node
    def choose():
        return "A"

    decision = wf.case(
        on=choose,
        branches={
            "A": ("greet", {"user_name": make_user["name"]}),
            "B": "x",
        },
    )

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    res = wf.run()
    assert res["out"] == "Hello Ann!"


def test_duplicate_case_on_same_on_node_id_raises():
    wf = Workflow(name="case-dup")

    @wf.node
    def switch():
        return True

    wf.case(on=switch, branches={True: "a", False: "b"})
    with pytest.raises(DefinitionError):
        wf.case(on=switch, branches={True: "x"})


def test_case_with_predicate_exception_is_ignored():
    wf = Workflow(name="case-predicate-exc")

    @wf.node
    def val():
        return 3

    def bad_predicate(_):
        raise RuntimeError("boom")

    decision = wf.case(
        on=val,
        branches={
            bad_predicate: 111,  # should be ignored due to exception
            3: 222,  # matched equality
        },
    )

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    res = wf.run()
    assert res["out"] == 222
