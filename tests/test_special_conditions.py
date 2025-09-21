import pytest
from langdag import Node, LangDAG, run_dag
from langdag.utils import (
    ContainsAll,
    SubsetOf,
    Empty,
    NotEmpty,
    EmptyDict,
    NotEmptyDict,
    InstanceOf,
    Check,
    CheckNot,
)

def run_conditional_dag(output_value, condition):
    """Helper function to run a simple conditional DAG."""
    with LangDAG() as dag:
        node_a = Node(node_id="A", func_transform=lambda p, u, d: output_value)
        node_b = Node(node_id="B", func_transform=lambda p, u, d: "Success")
        dag.add_node(node_a, node_b)
        node_a >> condition >> node_b
        run_dag(dag, verbose=False)
    return dag.get_node("B").execution_state

# --- Tests for each special condition class ---

def test_contains_all():
    assert run_conditional_dag([1, 2, 3], ContainsAll([1, 2])) == "finished"
    assert run_conditional_dag([1, 2], ContainsAll([1, 2, 3])) == "aborted"
    assert run_conditional_dag(list("abc"), ContainsAll(list("ab"))) == "finished"

def test_subset_of():
    assert run_conditional_dag([1, 2], SubsetOf([1, 2, 3])) == "finished"
    assert run_conditional_dag([1, 2, 3], SubsetOf([1, 2])) == "aborted"
    assert run_conditional_dag(list("ab"), SubsetOf(list("abc"))) == "finished"

def test_empty():
    assert run_conditional_dag([], Empty()) == "finished"
    assert run_conditional_dag({}, Empty()) == "finished"
    assert run_conditional_dag("", Empty()) == "finished"
    assert run_conditional_dag(None, Empty()) == "finished"
    assert run_conditional_dag(0, Empty()) == "finished"
    assert run_conditional_dag(False, Empty()) == "finished"
    assert run_conditional_dag([1], Empty()) == "aborted"
    assert run_conditional_dag("a", Empty()) == "aborted"

def test_not_empty():
    assert run_conditional_dag([1], NotEmpty()) == "finished"
    assert run_conditional_dag({"a": 1}, NotEmpty()) == "finished"
    assert run_conditional_dag("a", NotEmpty()) == "finished"
    assert run_conditional_dag(1, NotEmpty()) == "finished"
    assert run_conditional_dag(True, NotEmpty()) == "finished"
    assert run_conditional_dag([], NotEmpty()) == "aborted"
    assert run_conditional_dag(None, NotEmpty()) == "aborted"

def test_empty_dict():
    assert run_conditional_dag({}, EmptyDict()) == "finished"
    assert run_conditional_dag({"a": 1}, EmptyDict()) == "aborted"
    assert run_conditional_dag([], EmptyDict()) == "aborted" # Must be a dict

def test_not_empty_dict():
    assert run_conditional_dag({"a": 1}, NotEmptyDict()) == "finished"
    assert run_conditional_dag({}, NotEmptyDict()) == "aborted"
    assert run_conditional_dag("a", NotEmptyDict()) == "aborted" # Must be a dict

def test_instance_of():
    assert run_conditional_dag({"a": 1}, InstanceOf(dict)) == "finished"
    assert run_conditional_dag([1, 2], InstanceOf(list)) == "finished"
    assert run_conditional_dag("hello", InstanceOf(str)) == "finished"
    assert run_conditional_dag(123, InstanceOf(str)) == "aborted"
    assert run_conditional_dag(None, InstanceOf(dict)) == "aborted"

def test_check():
    condition = Check(lambda x: x.get("status"), "ok")
    assert run_conditional_dag({"status": "ok"}, condition) == "finished"
    assert run_conditional_dag({"status": "error"}, condition) == "aborted"
    assert run_conditional_dag({}, condition) == "aborted" # key error -> false

def test_check_not():
    condition = CheckNot(lambda x: x.get("status"), "error")
    assert run_conditional_dag({"status": "ok"}, condition) == "finished"
    assert run_conditional_dag({"status": "error"}, condition) == "aborted"
    assert run_conditional_dag({}, condition) == "finished" # key error -> true
