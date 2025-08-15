import pytest

from autoworkflow.core import (
    Workflow,
    DefinitionError,
    NodeOutput,
    BaseContext,
    CONTEXT,
)


def test_duplicate_node_names_raise():
    wf = Workflow(name="dup")

    @wf.node(name="x")
    def a():
        return 1

    with pytest.raises(DefinitionError):

        @wf.node(name="x")
        def b():
            return 2


def test_bind_and_select_constant_and_node_targets():
    wf = Workflow(name="bind/select")

    @wf.node
    def base(x: int = 0, y: int = 0):
        return x + y

    @wf.node
    def argx():
        return 4

    # Bind 'y' constant and 'x' dependency to argx node
    bound = wf.bind(base, y=3, x=argx)

    case_node = wf.case(
        on=argx,
        branches={
            4: bound,
            5: 99,
        },
    )

    @wf.node
    def out(v=wf.dep(case_node)):
        return v

    res = wf.run()
    # When on==4, chosen branch is node target, so output should be base(x=4, y=3) => 7
    assert res["out"] == 7


def test_unknown_executor_raises_definition_error():
    wf = Workflow(name="bad-executor")

    @wf.node
    def a():
        return 1

    with pytest.raises(DefinitionError):
        wf.run(executor="unknown")


def test_on_success_and_on_failure_hooks_invoked():
    # Success case
    wf_ok = Workflow(name="ok")

    @wf_ok.node
    def a():
        return 1

    state = {"ok": 0, "fail": 0}

    def on_success(results, ctx):
        state["ok"] += 1

    wf_ok.set_on_success(on_success)
    wf_ok.run(return_mode="leaves")
    assert state["ok"] == 1

    # Failure case
    wf_fail = Workflow(name="fail")

    @wf_fail.node
    def a2():
        return 1

    @wf_fail.node
    def b2(x=wf_fail.dep(a2)):
        raise RuntimeError("boom")

    def on_failure(node_id, exc, ctx):
        state["fail"] += 1

    wf_fail.set_on_failure(on_failure)
    with pytest.raises(Exception):
        wf_fail.run(return_mode="leaves")
    assert state["fail"] == 1


def test_invalid_return_mode_raises():
    wf = Workflow(name="bad-return")

    @wf.node
    def a():
        return 1

    with pytest.raises(DefinitionError):
        wf.run(return_mode="invalid")


def test_map_node_invalid_param_count_raises():
    wf = Workflow(name="Core-Map-Invalid")

    def f(a, b):
        return a, b

    with pytest.raises(DefinitionError):
        wf.map_node(f)  # wrong arity # type: ignore[arg-type]


def test_bind_unknown_node_id_raises():
    wf = Workflow(name="Core-Bind-Unknown")

    @wf.node
    def a():
        return 1

    with pytest.raises(DefinitionError):
        wf.bind("missing", x=NodeOutput(node_id="a", output_type=int))


def test_nodeoutput_access_path_attr_and_key():
    wf = Workflow(name="Core-AccessPath")

    @wf.node
    def base():
        class Obj:
            def __init__(self):
                self.m = {"k": 5}

        return Obj()

    @wf.node
    def take(v=wf.dep(base).m["k"]):
        return v

    out = wf.run(return_mode="leaves")
    assert out["take"] == 5


def test_case_constant_branch_disables_targets_and_leaves_return_mode():
    wf = Workflow(name="Core Case Const+Leaves")

    @wf.node
    def on():
        return 0

    @wf.node
    def t1():
        return "T1"

    @wf.node
    def t2():
        return "T2"

    decision = wf.case(on=on, branches={0: 42, 1: t1, 2: t2})

    @wf.node
    def out(v=wf.dep(decision)):
        return v

    res_all = wf.run(return_mode="all")
    assert res_all["out"] == 42
    res_leaves = wf.run(return_mode="leaves")
    assert res_leaves == {"out": 42}


def test_select_is_alias_of_bind_and_context_sentinel_passes_context():
    wf = Workflow(name="Core select/bind + CONTEXT")

    class Ctx(BaseContext):
        def __init__(self):
            self.value = 3

    @wf.node
    def base(x: int = 0, ctx: BaseContext = CONTEXT):  # type: ignore[no-untyped-def]
        return x + ctx.value  # type: ignore[attr-defined]

    sel = wf.select(base, x=5)

    @wf.node
    def out(v=wf.dep(sel)):
        return v

    res = wf.run(context=Ctx())
    assert res["out"] == 8


def test_run_unknown_return_mode_raises():
    wf = Workflow(name="bad-return-mode2")

    @wf.node
    def a():
        return 1

    with pytest.raises(DefinitionError):
        wf.run(return_mode="something-else")
