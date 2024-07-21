from typing import List, Set, Dict, Tuple, Optional, Any, Callable

import logging
from rich.tree import Tree
from rich.padding import Padding
from rich import print

class Subset(list):
    """
    Subclass of list
    Customized equal method: `Subset(a) == b` will return True if b is a subset of a.
    """
    def __eq__(self, other):
        self_list = self
        if isinstance(other, list) or isinstance(other, set) or isinstance(other, tuple):
            other_list = [x for x in other]
        else:
            other_list = [other]
        return all(x in self_list for x in other_list)
    
    def __str__(self) -> str:
        list_str = list(self)
        return f"Subset({list_str})"
    
    def __repr__(self) -> str:
        list_str = list(self)
        return f"Subset({list_str})"
    
class Superset(list):
    """
    Subclass of list
    Customized equal method: `Superset(a) == b` will return True if b is a superset of a.
    """
    def __eq__(self, other):
        self_list = self
        if isinstance(other, list) or isinstance(other, set) or isinstance(other, tuple):
            other_list = [x for x in other]
        else:
            other_list = [other]
        return all(x in other_list for x in self_list)
    
    def __str__(self) -> str:
        list_str = list(self)
        return f"Superset({list_str})"
    
    def __repr__(self) -> str:
        list_str = list(self)
        return f"Superset({list_str})"

class Emptyset(list):
    """
    Customized equal method: `Emptyset() == b` will return True if b is an empty list, tuple or set.
    """
    def __eq__(self, other):
        if (isinstance(other, list) or isinstance(other, tuple) or isinstance(other, set) ) and len(other)==0:
            # Handle list: when other is a empty list, return True
            return True
        elif other is None:
            return True
        elif isinstance(other, Emptyset):
            return True
        else:
            return False
        
    def __str__(self) -> str:
        list_str = list(self)
        return f"Emptyset({list_str})"
    
    def __repr__(self) -> str:
        list_str = list(self)
        return f"Emptyset({list_str})"
    
class NonEmptyset(list):
    """
    Customized equal method: `NonEmptyset() == b` will return True if b is not an empty list, tuple or set.
    """
    def __eq__(self, other):
        if (isinstance(other, list) or isinstance(other, tuple) or isinstance(other, set) ) and len(other)==0:
            # Handle list: when other is a non-empty list, return True
            return False
        elif other is None:
            return False
        elif isinstance(other, Emptyset):
            return False
        else:
            return True
        
    def __str__(self) -> str:
        list_str = list(self)
        return f"NonEmptyset({list_str})"
    
    def __repr__(self) -> str:
        list_str = list(self)
        return f"NonEmptyset({list_str})"

class PretransformSet():
    """
    Customized equal method:  
    `x == PretransformSet(b, y)` will return True if f(x) == y, 
    ie. x is untransformed y.
    """
    def __init__(self, func, res) -> None:
        self.func = func
        self.data = res
    
    def __str__(self) -> str:
        return f"PretransformSet({self.func.__name__}, {self.data})"

    def __repr__(self) -> str:
        return f"PretransformSet({self.func.__name__}, {self.data})"
    
    def __eq__(self, other) -> bool:
        try:
            if self.func(other) == self.data :
                return True
            else:
                return False
        except Exception as e:
            logging.warning('PretransformSet() Error occured!')
            logging.warning(e)
            return False       

class NotPretransformSet():
    """
    Customized equal method:  
    `x == NotPretransformSet(b, y)` will return True if f(x) != y, 
    ie. x is not pre-transformed y.
    """
    def __init__(self, func, res) -> None:
        self.func = func
        self.data = res
    
    def __str__(self) -> str:
        return f"NotPretransformSet({self.func.__name__}, {self.data})"
    
    def __eq__(self, other) -> bool:
        try:
            if self.func(other) == self.data :
                return False
            else:
                return True
        except Exception as e:
            logging.warning('NotPretransformSet() Error occured!')
            logging.warning(e)
            return True        

def default(upstream_output: Dict):
    """
    Given a dict with single item, return value of this item.
    Will raise an error if input dict has no or more than 1 item.
    """
    up_len = len(upstream_output.items())
    if up_len != 1:
        logging.warning(upstream_output)
        raise Exception(f"Using `default` function: `upstream_output` has {up_len} output, which is not equal to 1.")
    
    default_output = list(upstream_output.items())[0][1]
    return default_output


# Function to merge dictionaries
def merge_dicts(*dicts: Dict[Any, Any]):
    """
    Merging dicts to one.
    """
    merged_dict = {}
    for d in dicts:
        for key, value in d.items():
            if key in merged_dict:
                if isinstance(merged_dict[key], list) and value not in merged_dict[key]:
                    merged_dict[key].append(value)
                elif merged_dict[key] != value:
                    merged_dict[key] = [merged_dict[key], value]
            else:
                merged_dict[key] = value
    return merged_dict



def walk_dag(dag, child_nodes, parent_tree, parent_node=None):
    """
    Autoregressive tree generation for observability tree (dag.inspect_execution)
    """
    child_nodes = sorted(child_nodes, key=lambda x: x.node_id)
    for node in child_nodes:
        sus_nodes = dag.successors(node)
        
        if node.execution_state == "finished":
            node_es = "[green](√)[/green]" 
        elif node.execution_state == "aborted":
            node_es = "[red](X)[/red]" 
        else:
            node_es = "(-)" 

        condition = ""
        output_str = f"\n[red]OUTPUT:[/red] [italic]{node.node_output}[/]" 

        if parent_node:
            if node.node_id in parent_node.downstream_execution_condition.keys():
                if parent_node.node_output == parent_node.downstream_execution_condition[node.node_id][parent_node.node_id]:
                    condition = f"\n[red]CONDITION MET:[/] {parent_node.downstream_execution_condition[node.node_id]}" 
                else:
                    condition = f"\n[red]CONDITION NOT MET:[/] {parent_node.downstream_execution_condition[node.node_id]}" 

        style = "dim" if node.execution_state == "aborted" or "not matched" in condition else ""
        branch = parent_tree.add(
                    " ".join([x for x in [f"[bold blue]{str(node.node_id)}[/]", 
                                          f"[blue](DESC: {str(node.node_desc)})[/blue]", 
                                          node_es, 
                                          condition,  
                                          output_str] if x!=""  ]) ,
                    style=style,
                    guide_style=style,
                )
        walk_dag(dag, sus_nodes, branch, node)


def show_tree(dag):
    """
    Print to console a rich.tree to show DAG execution (dag.inspect_execution)
    """
    root_tree = Tree(
            f"[bold red]DAG INPUT: [italic] {str(dag.dag_state["input"])} [/] [/]",
            guide_style="bold bright_blue",
        )

    walk_dag(dag, dag.all_starts(), root_tree)
    print(Padding(root_tree, (1,1,2,2)))
    return root_tree
