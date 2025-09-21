from typing import Optional, Dict, Any, Callable, List
from langdag import Node
import inspect

import logging
from rich.logging import RichHandler

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", 
    format=FORMAT, 
    datefmt="[%X]", 
    handlers=[RichHandler()]
)

log = logging.getLogger("rich")

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
    A toolbox for registering functions that can be called by an LLM.

    Create an instance `toolbox = Toolbox()` and use the `@toolbox.add_tool()`
    decorator on any function to register it.

    Features:
    - Manually provide an OpenAI-compatible `spec`.
    - Automatically generate a `spec` from the function's signature and docstring
      by using `@toolbox.add_tool(auto_spec=True)`.
    - Call tools by name using `toolbox.call_tool_by_name(...)`.
    - Retrieve all specs for use in an LLM call with `toolbox.get_all_specs()`.
    """
    def __init__(self) -> None:
        self.toolbox_registry = {}
        self.toolbox_specs = {}

    def _map_type_to_json(self, py_type: Any) -> str:
        """Maps Python types to JSON schema types."""
        if py_type is str: return "string"
        if py_type in (int, float): return "number"
        if py_type is bool: return "boolean"
        if py_type is list: return "array"
        if py_type is dict: return "object"
        return "string" # Default for unknown or Any types

    def _generate_spec_from_func(self, func: Callable) -> Dict:
        """Automatically generates an OpenAI-compatible spec from a function."""
        sig = inspect.signature(func)
        docstring = inspect.getdoc(func) or ""
        description = docstring.split('\n')[0]

        parameters = {"type": "object", "properties": {}, "required": []}

        for name, param in sig.parameters.items():
            if name in ('self', 'cls'): continue

            param_type = self._map_type_to_json(param.annotation) if param.annotation != inspect.Parameter.empty else "string"
            
            param_desc = f"Parameter '{name}'"
            for line in docstring.split('\n'):
                if line.strip().startswith(f"{name} ("):
                    param_desc = line.split(":", 1)[1].strip()
                    break

            parameters["properties"][name] = {"type": param_type, "description": param_desc}

            if param.default == inspect.Parameter.empty:
                parameters["required"].append(name)

        return {
            "type": "function",
            "function": {
                "name": func.__name__ if hasattr(func, '__name__') else 'unknown',
                "description": description,
                "parameters": parameters
            }
        }

    def add_tool(self, spec: Optional[Dict] = None, auto_spec: bool = False):
        """
        Decorator to add a function or Node's transform to the toolbox.

        Args:
            spec (Dict, optional): An OpenAI-compatible function specification.
                                   If provided, this spec is used directly and takes highest precedence.
            auto_spec (bool, optional): If True and no manual `spec` is provided,
                                        a spec is auto-generated from the
                                        function's signature and docstring.
        """
        def decorator(func_or_node: Any):
            final_spec = spec
            
            if isinstance(func_or_node, Node):
                func_name = func_or_node.node_id
                the_callable = func_or_node.func_transform
                self.toolbox_registry[func_name] = the_callable
                if func_or_node.spec and final_spec is None:
                    final_spec = func_or_node.spec
            elif isinstance(func_or_node, Callable):
                func_name = func_or_node.__name__
                the_callable = func_or_node
                self.toolbox_registry[func_name] = the_callable
            else:
                return func_or_node

            if final_spec is None and auto_spec and the_callable:
                try:
                    final_spec = self._generate_spec_from_func(the_callable)
                except Exception as e:
                    log.warning(f"Could not auto-generate spec for '{func_name}': {e}")
            
            if final_spec:
                self.toolbox_specs[func_name] = final_spec
            
            return func_or_node
        return decorator

    def call_tool_by_name(self, func_name: str, *args, **kwargs):
        """Calls a registered function by its name."""
        if func_name in self.toolbox_registry:
            return self.toolbox_registry[func_name](*args, **kwargs)
        else:
            raise ValueError(f"Function '{func_name}' is not registered in the toolbox.")
    
    def get_spec_by_name(self, func_name: str) -> Optional[Dict]:
        """Retrieves the spec for a single function by its name."""
        return self.toolbox_specs.get(func_name)

    def get_all_specs(self) -> List[Dict]:
        """Returns a list of all registered function specs."""
        return list(self.toolbox_specs.values())

