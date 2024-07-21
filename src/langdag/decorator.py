from typing import Optional, Dict, Any, Callable
from langdag import Node

def make_node(  node_id: Optional[str] = None, 
                node_desc: Optional[str | Dict | Any] = None,
                prompt: Optional[str | Dict | Any] = None,
                spec: Optional[str | Dict | Any] = None,
                func_desc: Optional[str | Dict | Any] = None,
                func_set_dag_output_when: Optional[Callable[[str, Dict, Dict, Dict], bool]]=None,
                ):
    """
    Use the `@make_node()` decorator above a transforming function to create a node from that function. 
    This method is particularly suitable for creating simple nodes where the `node_id` is the function name, 
    and the node only needs a `func_transform`.

    Though using the `@make_node()` decorator simplifies the creation of nodes by directly associating the function 
    with the node's transformation logic, the `@make_node()` decorator has the same functionality as the `Node()` class. 
    It accepts the same parameters as `Node()`, except it uses the decorated function as `func_transform`, and the 
    `node_id` defaults to the name of the decorated function if not explicitly set.
    """
    def decorator(func_transform: Callable[[str, Dict, Dict], Any]):
        node = Node(
                node_id=func_transform.__name__ if not node_id else node_id,
                node_desc=node_desc ,
                prompt=prompt,
                spec=spec,
                func_desc=func_desc,
                func_transform=func_transform,
                func_set_dag_output_when=func_set_dag_output_when
        )
        return node
    return decorator



class Toolbox:
    """
    NOTE: Only use this if you do not want to create a Node for a function, and want to call 
    different functions by their names.
    Create an instance by `toolbox = Toolbox()` and use @toolbox.add_tool(spec="...") 
    on any function (spec is optional) to add function and its optional spec to this toolbox.
    You then can 
    - call the tool by its name in this toolbox by using `toolbox.call_tool_by_name("tool_name", *args, **kargs)`
    - and get its spec by `toolbox.get_spec_by_name("tool_name")`
    - get all specs by `toolbox.get_all_specs()`
    """
    # Dictionary to store function references
    def __init__(self) -> None:
        self.toolbox_registry = {}
        self.toolbox_specs = {}

    def add_tool(self, spec=None):
        """
        Create an instance by `toolbox = Toolbox()` and use @toolbox.add_tool(spec="...") 
        on any function (spec is optional) to add function and its optional spec to this toolbox.
        """
        def decorator(func):
            # Register the function in the dictionary
            print(self.toolbox_registry)
            if isinstance(func, Callable):
                self.toolbox_registry[func.__name__] = func
                if spec:
                    self.toolbox_specs[func.__name__] = spec
            elif isinstance(func, Node):
                self.toolbox_registry[func.node_id] = func.func_transform
                if spec:
                    self.toolbox_specs[func.node_id] = func.spec
            return func
        return decorator
    # Function to call the registered function by name
    def call_tool_by_name(self, func_name, *args, **kwargs):
        """
        Create an instance by `toolbox = Toolbox()` and use @toolbox.add_tool(spec="...") 
        on any function (spec is optional) to add function and its optional spec to this toolbox.
        You then can 
        - call the tool by its name in this toolbox by using `toolbox.call_tool_by_name("tool_name", *args, **kargs)`
        """
        if func_name in self.toolbox_registry:
            return self.toolbox_registry[func_name](*args, **kwargs)
        else:
            raise ValueError(f"Function '{func_name}' is not registered in the toolbox.")
    
    def get_spec_by_name(self, func_name):
        """
        Create an instance by `toolbox = Toolbox()` and use @toolbox.add_tool(spec="...") 
        on any function (spec is optional) to add function and its optional spec to this toolbox.
        You then can 
        - call the tool by its name in this toolbox by using `toolbox.call_tool_by_name("tool_name", *args, **kargs)`
        - and get its spec by `toolbox.get_spec_by_name("tool_name")`
        """
        return self.toolbox_specs.get(func_name)

    def get_all_specs(self):
        """
        Create an instance by `toolbox = Toolbox()` and use @toolbox.add_tool(spec="...") 
        on any function (spec is optional) to add function and its optional spec to this toolbox.
        You then can 
        - get all specs by `toolbox.get_all_specs()`
        """
        return list(self.toolbox_specs.values())
