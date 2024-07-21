from typing import List, Set, Dict, Tuple, Optional, Any, Callable
from rich.tree import Tree

from paradag import DAG, _call_method, _process_vertices
from langdag.processor import SequentialProcessor
import time
from langdag.utils import merge_dicts, show_tree
from langdag.executor import LangExecutor
from langdag.selector import FullSelector, MaxSelector
from langdag.error import LangdagSyntaxError


from rich import print
from rich.traceback import install
from rich.progress import Progress, TimeElapsedColumn
from rich.logging import RichHandler
install(show_locals=False)

import logging

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", 
    format=FORMAT, 
    datefmt="[%X]", 
    handlers=[RichHandler()]
)

log = logging.getLogger("rich")

class Empty:
    pass

class LangDAG(DAG):
    """A DAG for orchestrating large language model workflows

    Example:
        with LangDAG(user_query) as dag:
            ...

    Args:
        dag_input (`Any`, *optional*`): 
            input for a dag, accessible to func_transform in every Node.
    """
    current_dag : "LangDAG" = None
    
    def __init__(self, dag_input : Optional[ str | Any] = None):
        super().__init__()
        self.dag_state = {
                "input": dag_input,
                "specs": {},
                "output": None
                 }
        
        
    def __enter__(self):
        LangDAG.current_dag = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        LangDAG.current_dag = None
    
    def __iadd__(self, other):
        if isinstance(other, list) or isinstance(other, tuple):
            if any(isinstance(x, Node) for x in other):
                er='Please do not put Nodes in a list or tuple when adding nodes. This syntax is not supported.'
                raise LangdagSyntaxError(er)
        if isinstance(other, Node):
            self.add_vertex(other)
            self.dag_state["specs"].update({other.node_id: other.spec})
        else:
            raise LangdagSyntaxError('Please add only `Node` class instance to the DAG')

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
            er='When use `add_conditional_edge(self, left_node, condition, right_node)`, \
                ensure left_node and right_node are both instances of Node.'
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
    
    def __str__(self) -> str:
        for x in self._DAG__data._dagData__graph:
            print(f"Info dict of {x.node_id}:")
            keys_to_exclude = ['_original_instance_dict']
            filtered_dict = {k: v for k, v in x.__dict__.items() if k not in keys_to_exclude}
            print(filtered_dict)
        return ""

# Class
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

        self.downstream_execution_condition_temp = Empty()
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
                                            LangDAG.current_dag.dag_state)

    def transform(self) -> None:
        """
        A method accepts prompt, upstream_output, dag_state and use them to generate a node output
        """
        if self.func_transform:
            self.node_output = self.func_transform(self.prompt, 
                                                   self.upstream_output, 
                                                   LangDAG.current_dag.dag_state)
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
                LangDAG.current_dag.dag_state["output"] =  self.node_output
                LangDAG.current_dag.dag_state["output_by_node_id"] =  self.node_id

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
                    allow_execution_2 =  all( x in self.upstream_output.items() for x in self.execution_condition.items())
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
                    func_start_hook(self.node_id, 
                                    self.node_desc)
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
        if LangDAG.current_dag == None:
            raise RuntimeError("No LangDAG context is active, please use `>>` syntax within `with LangDAG() as dag:` context")
        
        if isinstance(other, list) or isinstance(other, tuple):
            if any(isinstance(x, Node) for x in other):
                raise LangdagSyntaxError('Please do not put Nodes in a list or tuple when defining edges. This is not supported with `>>` syntax.')

        if isinstance(other, Node):
            LangDAG.current_dag.add_edge(self, other)
            if not isinstance(self.downstream_execution_condition_temp, Empty):
                self.downstream_execution_condition = merge_dicts(self.downstream_execution_condition, 
                                                                  {other.node_id: {self.node_id:self.downstream_execution_condition_temp} })
                self.downstream_execution_condition_temp = Empty() 
            return other
        else:
            self.downstream_execution_condition_temp = other
            return self




def __raw_run(dag: LangDAG, 
              selector=FullSelector(), 
              processor=SequentialProcessor(), 
              executor=LangExecutor(), 
              slower: bool | int | float =False, 
              progressbar: bool=True):
    '''
    Rewritten `dag_run` function from `paradag` package.
    Run tasks according to DAG.

    Args:
        dag (`LangDAG`, *required*`): The DAG to run.
        selector (*optional*, defaults to `FullSelector()`): 
            langdag.selector.FullSelector will select all possilbe nodes to run concurrently, 
            langdag.selector.MaxSelector(N) will only select maximum of N instead
        processor (*optional*, defaults to `SequentialProcessor()`): 
            Use langdag.processor.SequentialProcessor for single thread execution (one by one), 
            use langdag.processor.MultiThreadProcessor for multi-thread exection (allow concurrent execution)
        executor (*optional*, defaults to `LangExecutor`): 
            Should use LangExecutor in most cases unless you what to customize your own.
        slower (`Boolean`, *optional*, defaults to False): 
            When set to True, it slow down every node execution by 1 sec; When set to a number N, 
            it slow down every node execution by N sec.
    '''

    indegree_dict = {}
    for vtx in dag.vertices():
        indegree_dict[vtx] = dag.indegree(vtx)

    vertices_final = []
    vertices_running = set()
    vertices_zero_indegree = dag.all_starts()

    # # <Modificaiton> add logic to handle dag input xxxxxxxxxxxxxx cancel
    # for start_node in vertices_zero_indegree:
    #     _call_method(executor, 'init_input_deliver', start_node, {"init_input": dag.dag_state["input"] } ) 
    
    pb_columns = [*Progress.get_default_columns()[:-1], TimeElapsedColumn()]
    ## </> 

    with Progress(*pb_columns) as progress:  # Modification: add progress bar
        task_num = len(dag.vertices()) # Modification: add progress bar
        if progressbar:
            task = progress.add_task("[green]Processing...", total=100) # Modification: add progress bar

        while vertices_zero_indegree:
            if slower:
                if isinstance(slower, int) or isinstance(slower, float):
                    time.sleep(slower)
                else:
                    time.sleep(1)

            vertices_idle = vertices_zero_indegree-vertices_running
            vertices_to_run = selector.select(vertices_running, vertices_idle)
            _call_method(executor, 'report_start', vertices_to_run)

            vertices_running |= set(vertices_to_run)
            _call_method(executor, 'report_running', vertices_running)

            processed_results = _process_vertices(
                vertices_to_run, vertices_running, processor, executor)
            _call_method(executor, 'report_finish', processed_results)

            vertices_processed = [result[0] for result in processed_results]
            vertices_running -= set(vertices_processed)

            vertices_final += vertices_processed
            vertices_zero_indegree -= set(vertices_processed)

            for vtx, result in processed_results:
                for v_to in dag.successors(vtx):
                    _call_method(executor, 'deliver', vtx, v_to, result) #  Modificaiton: add vtx
                    indegree_dict[v_to] -= 1
                    if indegree_dict[v_to] == 0:
                        vertices_zero_indegree.add(v_to)
            if progressbar:
                progress.update(task, advance= 100 * 1/task_num  ) #  Modificaiton: add vtx
        if progressbar:
            progress.update(task, description="[green]Finished", advance=100)
            
    return vertices_final

def run_dag(dag: LangDAG, 
            selector=FullSelector(), 
            processor=SequentialProcessor(), 
            executor=LangExecutor(), 
            verbose: bool=True, 
            slower: bool | int | float =False, 
            progressbar: bool=True):
    '''
    Simply a wrapper around `__raw_run`, modified `dag_run` in paradag.
    It implictly set `func_set_dag_output_when` to terminating nodes, 
    which allows only the not aborted terminating node to set dag output

    Args:
        dag (`LangDAG`, *required*`): The DAG to run.
        selector (*optional*, defaults to `FullSelector()`): 
            When using `MultiThreadProcessor()`, set to `FullSelector()` for unlimited concurrent execution, 
            or use `MaxSelector(max_no)` to limit the maximum number of nodes executing concurrently to `max_no`.
        processor (*optional*, defaults to `SequentialProcessor()`): 
            Use langdag.processor.SequentialProcessor for single thread execution (one by one), use 
            langdag.processor.MultiThreadProcessor for multi-thread exection (allow concurrent execution)
        executor (*optional*, defaults to `LangExecutor`): 
            Should use LangExecutor in most cases unless you what to customize your own.
        verbose (`Boolean`, *optional*, defaults to True): 
            When set to False, it disable verbose logging.
        slower (`Boolean| int| float`, *optional*, defaults to False): 
            When set to True, it slows down every node execution by 1 sec; When set to a number N, 
            it slow down every node execution by N sec.
        progressbar (`Boolean`, *optional*, defaults to True): 
            When set to False, it disable progressbar.
    '''
    LangDAG.current_dag = dag
    
    for vtx in dag.all_terminals():
        vtx.func_set_dag_output_when = lambda p, up, out, state: state != "aborted"
    if isinstance(processor, SequentialProcessor):
        selector = MaxSelector(1)
    if verbose == False:
        executor.verbose = False
    res = __raw_run(dag, selector, processor, executor, slower, progressbar)

    LangDAG.current_dag = None

    return res