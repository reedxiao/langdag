from typing import List, Set, Dict, Tuple, Optional, Any, Callable
from rich.tree import Tree
from contextvars import ContextVar
import dill
import inspect

from paradag import DAG
from langdag.utils import merge_dicts, show_tree
from langdag.error import LangdagSyntaxError

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

# This is a forward declaration for type hinting
class Node:
    pass

class _EmptySentinel:
    pass

class LangDAG(DAG):
    """A DAG for orchestrating large language model workflows

    Example:
        with LangDAG(user_query) as dag:
            ...

    Args:
        dag_input (`Any`, *optional*`): 
            input for a dag, accessible to func_transform in every Node.
        dag_id (`str`, *optional*):
            A unique identifier for the DAG. If not provided, it will be None.
    """
    _current_dag: ContextVar[Optional['LangDAG']] = ContextVar('current_dag', default=None)

    @staticmethod
    def get_current() -> "LangDAG":
        return LangDAG._current_dag.get()

    @staticmethod
    def set_current(dag: "LangDAG"):
        return LangDAG._current_dag.set(dag)

    @staticmethod
    def reset_current(token):
        LangDAG._current_dag.reset(token)
    
    def __init__(self, dag_input : Optional[ str | Any] = None, dag_id: Optional[str] = None):
        super().__init__()
        self.dag_state = {
                "id": dag_id,
                "input": dag_input,
                "specs": {},
                "output": None
                 }
        self._token = None
        
        
    def __enter__(self):
        self._token = LangDAG.set_current(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._token:
            LangDAG.reset_current(self._token)
            self._token = None
    
    def __iadd__(self, other):
        if isinstance(other, Node):
            self.add_node(other)
        elif isinstance(other, (list, tuple)):
            self.add_node(*other)
        else:
            raise LangdagSyntaxError('Please add only `Node` class instance or a list/tuple of Nodes to the DAG')
        return self
    
    def add_node(self, *nodes):
        """
        Add one or more nodes to this DAG instance.
        """
        
        if all([isinstance(node, Node) for node in nodes]):
            self.add_vertex(*nodes)
            for node in nodes:
                self.dag_state["specs"].update({node.node_id: node.spec})
        else:
            raise LangdagSyntaxError('Please only add `Node` class instances to the DAG')
        

    def add_conditional_edge(self, left_node, condition, right_node):
        """
        Add a conditional edge from `left_node` to `right_node` where condition is `condition`
        """
        if isinstance(right_node, list) or isinstance(right_node, tuple):
            if any(isinstance(x, Node) for x in right_node):
                er='Please do not put Nodes in a list or tuple when defining edges. This is not supported.'
                raise LangdagSyntaxError(er)

        if isinstance(left_node, Node) and isinstance(right_node, Node):
            left_node.downstream_execution_condition = merge_dicts(
                                                                    left_node.downstream_execution_condition, 
                                                                   {right_node.node_id: {left_node.node_id: condition} }
                                                                   )
            self.add_edge(left_node, right_node)
        else:
            er = (
                "When using `add_conditional_edge(self, left_node, condition, right_node)`, "
                "ensure left_node and right_node are both instances of Node."
            )
            raise LangdagSyntaxError(er)


    def reset_all_nodes(self) -> None:
        """
        Reset all nodes (node.reset) in this dag to its original state (when instantialized)
        """
        for node in self.vertices():
            node.reset()

    def inspect_execution(self) -> Tree:
        """
        Print to console a rich.tree to show DAG execution (dag.inspect_execution)
        """
        return show_tree(self)
    
    def get_info(self) -> Dict:
        """
        Returns a dict contains attributes of the nodes in the DAG.
        """
        info_dict = {}
        for x in self._DAG__data._dagData__graph:
            keys_to_exclude = ['_original_instance_dict']
            filtered_dict = {k: v for k, v in x.__dict__.items() if k not in keys_to_exclude}
            info_dict[x.node_id] = filtered_dict
        return info_dict

    def get_all_specs(self) -> List:
        """
        Returns a list containing all node.spec of all nodes in this DAG.
        Usefull in function / tool calling scenarios.
        """
        spec_list = []
        for node in self._DAG__data._dagData__graph:
            if node.spec:
                spec_list.append(node.spec)
        return spec_list
    
    def get_node(self, node_id: Any) -> Optional[Node]:
        """
        Returns the node instance with the given node_id from the DAG.
        Returns None if no node with the given id is found.
        """
        for node in self.vertices():
            if node.node_id == node_id:
                return node
        return None
    
    def __str__(self) -> str:
        for x in self._DAG__data._dagData__graph:
            print(f"Info dict of {x.node_id}:")
            keys_to_exclude = ['_original_instance_dict']
            filtered_dict = {k: v for k, v in x.__dict__.items() if k not in keys_to_exclude}
            print(filtered_dict)
        return ""

    def snapshot(self, path: str) -> None:
        """
        Save the current state of the DAG to a file.
        """
        with open(path, "wb") as f:
            dill.dump(self, f)

    @staticmethod
    def recover(path: str) -> "LangDAG":
        """
        Recover a DAG from a file.
        """
        with open(path, "rb") as f:
            return dill.load(f)

    def __getstate__(self):
        state = self.__dict__.copy()
        # Don't pickle the context token
        if '_token' in state:
            del state['_token']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Restore token to a default state
        self._token = None

class Node():
    """
    A node in LangDAG.

    Args:
        node_id (`Any`, *required*`):
            A unique identifier for the node.
        node_desc (`Any`, *optional*, defaults to `None`):
            A description of the node, accessible via `node.node_desc`.
        prompt (`Any`, *optional*, defaults to `None`):
            A predefined prompt for the node.
        spec (`Dict | Any`, *optional*, defaults to `None`):
            A optional property for saving specification of the node as a tool 
            (Example spec: https://cookbook.openai.com/examples/how_to_call_functions_with_chat_models#basic-concepts )
        func_desc (`Callable`, *optional*, defaults to `None`):
            A function that generates a dynamic description from `prompt`, `upstream_output`, and `dag_state`.
        func_transform (`Callable`, *optional*, defaults to `None`):
            A function that transforms `prompt`, `upstream_output`, and `dag_state` into the node's output.
        func_set_dag_output_when (`Callable`, *optional*, defaults to `None`):
            A function returns boolean that decides whether the `node_output` should be set as the final 
            output of the DAG (dag.dag_state["output"]) based on `prompt`, `upstream_output`, `node_output`, 
            and `execution_state`.
    """
    def __init__(
            self, 
            node_id: str, 
            node_desc: Optional[str | Dict | Any] = None,
            prompt: Optional[str | Dict | Any] = None,
            spec: Optional[ Dict | Any] = None,
            func_desc: Optional[str | Dict | Any] = None,
            func_transform: Optional[Callable[[str, Dict, Dict], Any]] =None, 
            func_set_dag_output_when: Optional[Callable[[str, Dict, Dict, Dict], bool]]=None
        ) -> None:
        self.node_id: str | int | Any = node_id
        self.node_desc: str | Any = node_desc
        # self.model =  "v1"
        self.prompt = prompt
        self.spec = spec
        self.func_desc = func_desc
        self.func_transform = func_transform
        self.func_set_dag_output_when = func_set_dag_output_when
        self.upstream_output: Dict[Any, Any]  = {}
        self.node_output: Any = None 
        self.upstream_execution_state: Dict[Any, Any] = {}
        self.execution_state: str = "initialized"

        self.downstream_execution_condition_temp = _EmptySentinel()
        self.downstream_execution_condition: Dict[Any, Any] = {}

        self.conditional_excecution: bool = False
        self.execution_condition: Dict[Any, Any] = {}

        self.allow_execution_only_when_all_upstream_nodes_acceptable: bool = True

        self._original_instance_dict: Dict[Any, Any] = self.__dict__.copy()

    def reset(self) -> None:
        """
        Resets the node to its original state as when instantiated.
        """
        self.__dict__.update(self._original_instance_dict)

    def get_info(self) -> Dict:
        """
        Returns a dict containing attributes of the node.
        """
        keys_to_exclude = ['_original_instance_dict']
        info_dict = {k: v for k, v in self.__dict__.items() if k not in keys_to_exclude}
        return info_dict
    
    def add_spec(self, spec_dict: Dict) -> None:
        """
        Save parameter `spec_dict` to node.spec
        """
        self.spec = spec_dict
    
    def set_desc(self) -> None:
        """
        A method accepts prompt, upstream_output, dag_state and use them to generate a dynamic description
        """
        if self.func_desc:
            self.node_desc = self.func_desc(self.prompt, 
                                            self.upstream_output, 
                                            LangDAG.get_current().dag_state)

    def transform(self) -> None:
        """
        A method accepts prompt, upstream_output, dag_state and use them to generate a node output
        """
        if self.func_transform:
            self.node_output = self.func_transform(self.prompt, 
                                                   self.upstream_output, 
                                                   LangDAG.get_current().dag_state)
        return self.node_output

    def __set_dag_output(self) -> None:
        """
        A method accepts prompt, upstream_output, node_output, execution_state and use them to get a boolean to decide whether the node_output will to set as the final output of DAG
        """
        if self.func_set_dag_output_when:
            if  self.func_set_dag_output_when(self.prompt, 
                                              self.upstream_output, 
                                              self.node_output, 
                                              self.execution_state):
                LangDAG.get_current().dag_state["output"] =  self.node_output
                LangDAG.get_current().dag_state["output_by_node_id"] =  self.node_id

    def exec_if_any_upstream_acceptable(self) -> "Node":
        """
        NOT default behavior.
        Configures the node to execute when 
        **any** upstream nodes acceptable. "acceptable" meaning see docs.
        """
        self.allow_execution_only_when_all_upstream_nodes_acceptable = False

        return self
        
    def run_node(self, verbose=True, func_start_hook=None) -> None:
        """
        Decide how node execute.
        """

        nodes_finished = [x[0] for x in self.upstream_execution_state.items() if x[1]=="finished"]   

        if self.allow_execution_only_when_all_upstream_nodes_acceptable:
            allow_execution_1 = all([x[1]=="finished" for x in self.upstream_execution_state.items()])
            if allow_execution_1 == False:
                allow_execution = False
            else:
                if self.conditional_excecution:
                    allow_execution_2 =  all(self.upstream_output.get(k) == v for k, v in self.execution_condition.items())
                else:
                    allow_execution_2 = True
                allow_execution = allow_execution_1 and allow_execution_2
            
        else:
            allow_execution_1 = any([x[1]=="finished" for x in self.upstream_execution_state.items()])
            if allow_execution_1 == False:
                allow_execution = False
            else:
                if self.conditional_excecution:
                    conditional_nodes_acceptable = [x[0] for x in self.execution_condition.items() if x in self.upstream_output.items()]
                    unconditional_nodes_finished = [x for x in nodes_finished if x not in self.execution_condition.keys()]
                    nodes_acceptable = conditional_nodes_acceptable + unconditional_nodes_finished
                    allow_execution_2 = True if len(nodes_acceptable)>0 else False
                else:
                    allow_execution_2 = True
                allow_execution = allow_execution_1 and allow_execution_2

        if not allow_execution:
            self.execution_state = "aborted"

        if self.conditional_excecution:
          
            conditional_nodes_acceptable = [x[0] for x in self.execution_condition.items() if x in self.upstream_output.items()]
            unconditional_nodes_finished = [x for x in nodes_finished if x not in self.execution_condition.keys()]
            nodes_acceptable = conditional_nodes_acceptable + unconditional_nodes_finished
            
            self.upstream_output = { k:self.upstream_output[k] for k in self.upstream_output.keys() if  k in  nodes_acceptable}

        if verbose : 
            log.info("   (2) [bold yellow]->o[/] [bold yellow]%s[/] received upstream (filter acceptable): %s", 
                     self.node_id, 
                     self.upstream_output, 
                     extra={"markup": True})
       
      
        # If aborted, will not do transform, etc.
        if self.execution_state == "aborted":
            pass
        else:
            # move from report_start to here
            # because we need FILTERED upstream output to set node_desc
            self.set_desc()
            
            if func_start_hook:
                    func_start_hook(self)
            # move end

            self.transform()
            self.__set_dag_output()
            self.execution_state = "finished"
        
    def __str__(self) -> str:
        return self.node_id
    
    def __rshift__(self, other):
        """
        Change `rshift` operand to represent the edge syntax, ie : node_1 >> node_2
        """
        if LangDAG.get_current() == None:
            raise RuntimeError("No LangDAG context is active, please use `>>` syntax within `with LangDAG() as dag:` context")
        
        if isinstance(other, list) or isinstance(other, tuple):
            if any(isinstance(x, Node) for x in other):
                raise LangdagSyntaxError('Please do not put Nodes in a list or tuple when defining edges. This is not supported with `>>` syntax.')

        if isinstance(other, Node):
            LangDAG.get_current().add_edge(self, other)
            if not isinstance(self.downstream_execution_condition_temp, _EmptySentinel):
                self.downstream_execution_condition = merge_dicts(self.downstream_execution_condition, 
                                                                  {other.node_id: {self.node_id:self.downstream_execution_condition_temp} })
                self.downstream_execution_condition_temp = _EmptySentinel() 
            return other
        else:
            self.downstream_execution_condition_temp = other
            return self

    async def arun_node(self, verbose=True, func_start_hook=None) -> None:
        """
        Asynchronously decides how a node executes.
        This is the async counterpart to run_node.
        """
        nodes_finished = [x[0] for x in self.upstream_execution_state.items() if x[1]=="finished"]   

        if self.allow_execution_only_when_all_upstream_nodes_acceptable:
            allow_execution_1 = all([x[1]=="finished" for x in self.upstream_execution_state.items()])
            if allow_execution_1 == False:
                allow_execution = False
            else:
                if self.conditional_excecution:
                    allow_execution_2 = all(
                        self.upstream_output.get(k) == v for k, v in self.execution_condition.items()
                    )
                else:
                    allow_execution_2 = True
                allow_execution = allow_execution_1 and allow_execution_2
            
        else:
            allow_execution_1 = any([x[1]=="finished" for x in self.upstream_execution_state.items()])
            if allow_execution_1 == False:
                allow_execution = False
            else:
                if self.conditional_excecution:
                    conditional_nodes_acceptable = [
                        k for k, v in self.execution_condition.items() 
                        if self.upstream_output.get(k) == v
                    ]
                    unconditional_nodes_finished = [x for x in nodes_finished if x not in self.execution_condition.keys()]
                    nodes_acceptable = conditional_nodes_acceptable + unconditional_nodes_finished
                    allow_execution_2 = True if len(nodes_acceptable)>0 else False
                else:
                    allow_execution_2 = True
                allow_execution = allow_execution_1 and allow_execution_2

        if not allow_execution:
            self.execution_state = "aborted"

        if self.conditional_excecution:
          
            conditional_nodes_acceptable = [
                k for k, v in self.execution_condition.items() 
                if self.upstream_output.get(k) == v
            ]
            unconditional_nodes_finished = [x for x in nodes_finished if x not in self.execution_condition.keys()]
            nodes_acceptable = conditional_nodes_acceptable + unconditional_nodes_finished
            
            self.upstream_output = { k:self.upstream_output[k] for k in self.upstream_output.keys() if  k in  nodes_acceptable}

        if verbose : 
            log.info("   (2) [bold yellow]->o[/] [bold yellow]%s[/] received upstream (filter acceptable): %s", 
                     self.node_id, 
                     self.upstream_output, 
                     extra={"markup": True})
       
      
        # If aborted, will not do transform, etc.
        if self.execution_state == "aborted":
            pass
        else:
            # move from report_start to here
            # because we need FILTERED upstream output to set node_desc
            if inspect.iscoroutinefunction(self.func_desc):
                await self.aset_desc()
            else:
                self.set_desc()
            
            if func_start_hook:
                if inspect.iscoroutinefunction(func_start_hook):
                    await func_start_hook(self)
                else:
                    func_start_hook(self)

            # Await the async transform
            if inspect.iscoroutinefunction(self.func_transform):
                await self.atransform()
            else:
                self.transform()

            self.__set_dag_output() # This remains sync for now
            self.execution_state = "finished"

    async def atransform(self) -> None:
        """
        Asynchronously runs the node's transform function.
        """
        if self.func_transform:
            self.node_output = await self.func_transform(self.prompt,
                                                         self.upstream_output,
                                                         LangDAG.get_current().dag_state)
        return self.node_output

    async def aset_desc(self) -> None:
        """
        Asynchronously generates a dynamic description.
        """
        if self.func_desc:
            self.node_desc = await self.func_desc(self.prompt,
                                                  self.upstream_output,
                                                  LangDAG.get_current().dag_state)
