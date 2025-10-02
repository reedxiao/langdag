import pytest
import asyncio
from langdag import Node, LangDAG, run_dag, arun_dag
from langdag.executor import LangExecutor, AsyncLangExecutor
from langdag.plugins.base import Plugin
from paradag.error import VertexExecutionError

class MockPlugin(Plugin):
    def __init__(self):
        self.call_log = []

    def before_dag_execute(self, dag):
        self.call_log.append(("before_dag_execute", dag.dag_state.get("input")))

    def after_dag_execute(self, dag):
        self.call_log.append(("after_dag_execute", dag.dag_state.get("output")))

    def before_node_execute(self, node):
        self.call_log.append(("before_node_execute", node.node_id))

    def after_node_execute(self, node):
        self.call_log.append(("after_node_execute", node.node_id))

    def on_node_success(self, node):
        self.call_log.append(("on_node_success", node.node_id))

    def on_node_error(self, node, error):
        self.call_log.append(("on_node_error", node.node_id, str(error)))
def test_plugin_error_handling_flow():
    """Tests that the correct plugin hooks are called when a node fails."""
    error_message = "This node was designed to fail"
    def failing_transform(p, u, d):
        raise ValueError(error_message)

    node1 = Node(node_id="node1", func_transform=lambda p, u, d: "output1")
    node_fail = Node(node_id="node_fail", func_transform=failing_transform)
    node3 = Node(node_id="node3", func_transform=lambda p, u, d: "output3")
    
    mock_plugin = MockPlugin()
    executor = LangExecutor(plugins=[mock_plugin])

    with pytest.raises(VertexExecutionError) as excinfo:
        with LangDAG(dag_input="error_test") as dag:
            dag += [node1, node_fail, node3]
            node1 >> node_fail >> node3
            run_dag(dag, executor=executor, progressbar=False, verbose=False)

    assert error_message in str(excinfo.value)

    # Note: after_dag_execute is not called because the DAG run is aborted by the exception.
    # This is the expected behavior as the runner re-raises the exception.
    expected_log = [
        ("before_dag_execute", "error_test"),
        ("before_node_execute", "node1"),
        ("on_node_success", "node1"),
        ("after_node_execute", "node1"),
        ("before_node_execute", "node_fail"),
        ("on_node_error", "node_fail", error_message),
        ("after_node_execute", "node_fail"),
    ]
    assert mock_plugin.call_log == expected_log
