"""
Unit tests for the langdag package.
"""

import pytest
import asyncio
from langdag import Node, LangDAG, run_dag, arun_dag, resume_dag
from langdag.processor import SequentialProcessor, MultiThreadProcessor
from langdag.executor import LangExecutor, AsyncLangExecutor
from langdag.decorator import make_node
from langdag.utils import default, ContainsAll
from paradag.error import VertexExecutionError

# Fixtures for reusable components
# =================================

@pytest.fixture
def simple_nodes():
    """Provides a set of simple, reusable nodes for testing."""
    return {
        "start": Node(node_id="start", func_transform=lambda p, u, d: "start_data"),
        "process": Node(node_id="process", func_transform=lambda p, u, d: f"processed_{default(u)}"),
        "end": Node(node_id="end", func_transform=lambda p, u, d: f"ended_with_{default(u)}")
    }

@pytest.fixture
def decorated_node():
    """Provides a node created with the @make_node decorator."""
    @make_node(node_id="decorated_node")
    def my_node(prompt, upstream_output, dag_state):
        return f"decorated_{default(upstream_output)}"
    return my_node

# Core Feature Tests
# =================================

def test_node_creation_and_properties():
    """Tests basic node instantiation and attribute access."""
    node = Node(node_id="test_id", node_desc="a_desc", prompt="a_prompt")
    assert node.node_id == "test_id"
    assert node.node_desc == "a_desc"
    assert node.prompt == "a_prompt"
    assert node.execution_state == "initialized"

def test_dag_creation_and_node_addition(simple_nodes):
    """Tests creating a DAG and adding nodes to it."""
    with LangDAG() as dag:
        dag += simple_nodes["start"]
        dag += simple_nodes["process"]
    
    assert len(dag.vertices()) == 2
    assert simple_nodes["start"] in dag.vertices()

def test_iadd_with_list(simple_nodes):
    """Tests the `dag += [node1, node2]` syntax."""
    with LangDAG() as dag:
        nodes_to_add = [simple_nodes["start"], simple_nodes["process"]]
        dag += nodes_to_add
    
    assert len(dag.vertices()) == 2
    assert simple_nodes["start"] in dag.vertices()
    assert simple_nodes["process"] in dag.vertices()

    with LangDAG() as dag:
        invalid_nodes = [simple_nodes["start"], "not_a_node"]
        with pytest.raises(Exception):
            dag += invalid_nodes


def test_sequential_execution(simple_nodes):
    """Tests a simple A -> B -> C sequential workflow."""
    with LangDAG() as dag:
        dag += simple_nodes["start"]
        dag += simple_nodes["process"]
        dag += simple_nodes["end"]

        simple_nodes["start"] >> simple_nodes["process"] >> simple_nodes["end"]
        
        run_dag(dag, processor=SequentialProcessor())

    assert dag.dag_state["output"] == "ended_with_processed_start_data"
    assert simple_nodes["end"].execution_state == "finished"

def test_multithread_execution():
    """Tests that concurrent execution runs without errors."""
    node_a = Node(node_id="a", func_transform=lambda p, u, d: 1)
    node_b = Node(node_id="b", func_transform=lambda p, u, d: 2)
    node_c = Node(node_id="c", func_transform=lambda p, u, d: sum(u.values()))

    with LangDAG() as dag:
        dag.add_node(node_a, node_b, node_c)
        node_a >> node_c
        node_b >> node_c
        
        run_dag(dag, processor=MultiThreadProcessor())

    assert dag.dag_state["output"] == 3

def test_conditional_branching_true():
    """Tests that a conditional branch is taken when the condition is met."""
    start_node = Node(node_id="start", func_transform=lambda p, u, d: True)
    true_branch = Node(node_id="true_branch", func_transform=lambda p, u, d: "true_path")
    false_branch = Node(node_id="false_branch", func_transform=lambda p, u, d: "false_path")

    with LangDAG() as dag:
        dag.add_node(start_node, true_branch, false_branch)
        start_node >> True >> true_branch
        start_node >> False >> false_branch
        
        run_dag(dag)

    assert true_branch.execution_state == "finished"
    assert false_branch.execution_state == "aborted"
    assert true_branch.node_output == "true_path"

def test_exec_if_any_upstream_acceptable():
    """Tests the 'exec_if_any_upstream_acceptable' logic."""
    cond_node = Node(node_id="cond", func_transform=lambda p, u, d: "A")
    branch_a = Node(node_id="branch_a", func_transform=lambda p, u, d: 1)
    branch_b = Node(node_id="branch_b", func_transform=lambda p, u, d: 2)
    merger = Node(node_id="merger", func_transform=lambda p, u, d: sum(u.values()))
    merger.exec_if_any_upstream_acceptable()

    with LangDAG() as dag:
        dag.add_node(cond_node, branch_a, branch_b, merger)
        cond_node >> "A" >> branch_a
        cond_node >> "B" >> branch_b  # This branch will be aborted
        branch_a >> merger
        branch_b >> merger
        
        run_dag(dag)

    assert merger.execution_state == "finished"
    assert merger.node_output == 1

def test_dag_state_manipulation():
    """Tests that nodes can read from and write to the shared dag_state."""
    @make_node(node_id="writer")
    def writer(p, u, d):
        d["shared_key"] = "shared_value"
        return 1

    @make_node(node_id="reader")
    def reader(p, u, d):
        return d.get("shared_key")

    with LangDAG() as dag:
        dag.add_node(writer, reader)
        writer >> reader
        run_dag(dag)

    assert dag.dag_state["output"] == "shared_value"

def test_hooks_are_called():
    """Tests that start and finish hooks are executed for each node."""
    start_hook_calls = []
    finish_hook_calls = []

    def my_start_hook(node):
        start_hook_calls.append(node.node_id)

    def my_finish_hook(node):
        finish_hook_calls.append(node.node_id)

    executor = LangExecutor(func_start_hook=my_start_hook, func_finish_hook=my_finish_hook)
    
    node_a = Node(node_id="a")
    node_b = Node(node_id="b")

    with LangDAG() as dag:
        dag += node_a
        dag += node_b
        node_a >> node_b
        run_dag(dag, executor=executor)

    assert start_hook_calls == ["a", "b"]
    assert finish_hook_calls == ["a", "b"]

def test_snapshot_and_recovery(tmp_path):
    """Tests that a DAG can be snapshotted and recovered correctly."""
    snapshot_file = tmp_path / "test.dill"
    
    with LangDAG() as dag:
        dag += Node(node_id="a", func_transform=lambda p, u, d: 1)
        run_dag(dag)
        dag.snapshot(snapshot_file)

    recovered_dag = LangDAG.recover(snapshot_file)
    
    assert recovered_dag is not None
    assert len(recovered_dag.vertices()) == 1
    # Check if node state is preserved
    recovered_node = list(recovered_dag.vertices())[0]
    assert recovered_node.node_id == "a"
    assert recovered_node.execution_state == "finished"
    assert recovered_node.node_output == 1

def test_node_reset(simple_nodes):
    """Tests that a node's state can be reset after execution."""
    node = simple_nodes["start"]
    with LangDAG() as dag:
        dag += node
        run_dag(dag)
    
    assert node.execution_state == "finished"
    assert node.node_output is not None
    
    node.reset()
    
    assert node.execution_state == "initialized"
    assert node.node_output is None

def test_decorated_node_execution(simple_nodes, decorated_node):
    """Tests a workflow that includes a node created by a decorator."""
    with LangDAG() as dag:
        dag += simple_nodes["start"]
        dag += decorated_node
        simple_nodes["start"] >> decorated_node
        run_dag(dag)
        
    assert dag.dag_state["output"] == "decorated_start_data"

@pytest.mark.asyncio
async def test_async_execution():
    """Tests asynchronous execution of a DAG with async nodes."""
    async def a_transform(p, u, d):
        await asyncio.sleep(0.01)
        return f"async_{default(u)}"

    node_start = Node(node_id="start", func_transform=lambda p, u, d: "start")
    node_async = Node(node_id="async_node", func_transform=a_transform)

    with LangDAG() as dag:
        dag.add_node(node_start, node_async)
        node_start >> node_async
        await arun_dag(dag)

    assert dag.dag_state["output"] == "async_start"

def test_conditional_edge_with_special_class():
    """Tests conditional edges using special classes like ContainsAll."""
    node_a = Node(node_id="a", func_transform=lambda p, u, d: [1, 2, 3])
    node_b = Node(node_id="b", func_transform=lambda p, u, d: "success")

    with LangDAG() as dag:
        dag.add_node(node_a, node_b)
        node_a >> ContainsAll([1, 3]) >> node_b
        run_dag(dag)

    assert node_b.execution_state == "finished"
    assert dag.dag_state["output"] == "success"

def test_dag_input():
    """Tests that dag_input is accessible to nodes."""
    node = Node(node_id="input_reader", func_transform=lambda p, u, d: d["input"])
    with LangDAG(dag_input="test_input") as dag:
        dag += node
        run_dag(dag)
    assert dag.dag_state["output"] == "test_input"

def test_get_node_by_id(simple_nodes):
    """Tests retrieving a node from the DAG by its ID."""
    with LangDAG() as dag:
        dag += simple_nodes["start"]
        dag += simple_nodes["process"]

        retrieved_node = dag.get_node("start")
        assert retrieved_node is not None
        assert retrieved_node.node_id == "start"
        assert retrieved_node is simple_nodes["start"]

        non_existent_node = dag.get_node("non_existent")
        assert non_existent_node is None

def test_error_propagation_on_fail():
    """Tests that an exception in a node's transform is propagated."""
    failing_node = Node(node_id="failing", func_transform=lambda p, u, d: 1 / 0)
    with LangDAG() as dag:
        dag += failing_node
        with pytest.raises(VertexExecutionError):
            run_dag(dag)

from paradag.error import VertexExecutionError

def test_resume_dag(tmp_path):
    """Tests resuming a partially completed DAG from a snapshot."""
    snapshot_file = tmp_path / "resume_test.dill"

    node_a = Node(node_id="a", func_transform=lambda p, u, d: 1)
    node_b = Node(node_id="b", func_transform=lambda p, u, d: 1 / 0)  # This will fail
    node_c = Node(node_id="c", func_transform=lambda p, u, d: default(u) + 1)

    with LangDAG() as dag:
        dag.add_node(node_a, node_b, node_c)
        node_a >> node_b >> node_c
        try:
            run_dag(dag, snapshot_on_error_path=snapshot_file)
        except VertexExecutionError:
            pass  # Expected failure

    recovered_dag = LangDAG.recover(snapshot_file)
    # Manually fix the failing node
    for node in recovered_dag.vertices():
        if node.node_id == "b":
            node.func_transform = lambda p, u, d: 2

    resume_dag(recovered_dag)
    assert recovered_dag.dag_state["output"] == 3