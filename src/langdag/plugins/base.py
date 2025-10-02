# src/langdag/plugins/base.py

class Plugin:
    def before_dag_execute(self, dag):
        pass

    def after_dag_execute(self, dag):
        pass

    def before_node_execute(self, node):
        pass

    def after_node_execute(self, node):
        pass

    def on_node_success(self, node):
        pass

    def on_node_error(self, node, error):
        pass
