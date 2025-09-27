# src/langdag/plugins/langfuse.py

import hashlib
import json
from langdag.plugins.base import Plugin
from langfuse import get_client

class LangfusePlugin(Plugin):
    def __init__(self, **kwargs):
        self.langfuse = get_client(**kwargs)
        self.dag_span = None
        self.spans = {}

    def before_dag_execute(self, dag):
        dag_id = dag.dag_state.get("id")
        if dag_id is None:
            node_ids = sorted([str(node.node_id) for node in dag.vertices()])
            edges = sorted([(str(u.node_id), str(v.node_id)) for u, v in dag.edges()])
            dag_structure = {
                "nodes": node_ids,
                "edges": edges
            }
            structure_string = json.dumps(dag_structure, sort_keys=True)
            dag_id = hashlib.sha256(structure_string.encode()).hexdigest()

        self.dag_span = self.langfuse.start_span(
            name=dag_id,
            metadata=dag.dag_state
        )

    def before_node_execute(self, node):
        if self.dag_span:
            span = self.dag_span.start_span(
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
        if self.dag_span:
            self.dag_span.end(output=dag.dag_state.get("output"))
        self.langfuse.flush()