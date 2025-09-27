# src/langdag/plugins/langfuse.py

from langdag.plugins.base import Plugin
from langfuse import Langfuse

class LangfusePlugin(Plugin):
    def __init__(self, **kwargs):
        self.langfuse = Langfuse(**kwargs)
        self.trace = None
        self.spans = {}

    def before_dag_execute(self, dag):
        self.trace = self.langfuse.trace(
            name="langdag-trace", # will be improved
            metadata=dag.dag_state
        )

    def before_node_execute(self, node):
        if self.trace:
            span = self.trace.span(
                name=node.node_id,
                metadata={"description": node.node_desc},
                input=node.upstream_output
            )
            self.spans[node.node_id] = span

    def on_node_success(self, node):
        if node.node_id in self.spans:
            self.spans[node.node_id].end(output=node.node_output)

    def on_node_error(self, node, error):
        if node.node_id in self.spans:
            self.spans[node.node_id].end(level='ERROR', status_message=str(error))
            
    def after_dag_execute(self, dag):
        if self.trace:
            self.trace.update(output=dag.dag_state["output"])
