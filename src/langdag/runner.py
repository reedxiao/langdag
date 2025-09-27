from typing import List, Set, Dict, Tuple, Optional, Any, Callable
import time
import warnings
import logging
import asyncio

from paradag import _call_method, _process_vertices
from langdag.processor import SequentialProcessor
from langdag.executor import LangExecutor, AsyncLangExecutor
from langdag.selector import FullSelector, MaxSelector
from langdag.core import LangDAG, Node

from rich.progress import Progress, TimeElapsedColumn

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

def _raw_run(dag: LangDAG, 
              selector=FullSelector(), 
              processor=SequentialProcessor(), 
              executor=LangExecutor(), 
              delay: bool | int | float =False, 
              progressbar: bool=True,
              indegree_dict: Optional[Dict[Node, int]] = None,
              vertices_zero_indegree: Optional[Set[Node]] = None,
              vertices_final: Optional[List[Node]] = None):
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
        delay (`Boolean`, *optional*, defaults to False): 
            When set to True, it slows down every node execution by 1 sec; When set to a number N, 
            it slows down every node execution by N sec.
    '''

    if indegree_dict is None:
        indegree_dict = {vtx: dag.indegree(vtx) for vtx in dag.vertices()}

    if vertices_final is None:
        vertices_final = []
        
    vertices_running = set()

    if vertices_zero_indegree is None:
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
            if delay:
                if isinstance(delay, int) or isinstance(delay, float):
                    time.sleep(delay)
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
            delay: bool | int | float =False, 
            progressbar: bool=True,
            snapshot_on_error_path: Optional[str] = None,
            *,
            slower: Optional[bool | int | float] = None):
    '''
    Simply a wrapper around `_raw_run`, modified `dag_run` in paradag.
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
        delay (`Boolean| int| float`, *optional*, defaults to False): 
            When set to True, it slows down every node execution by 1 sec; When set to a number N, 
            it slow down every node execution by N sec.
        progressbar (`Boolean`, *optional*, defaults to True): 
            When set to False, it disable progressbar.
        snapshot_on_error_path (`str`, *optional*, defaults to `None`):
            If provided, the DAG state will be saved to this path upon any execution error.
    '''
    actual_delay = delay
    if slower is not None:
        warnings.warn(
            "'slower' is deprecated and will be removed in a future version. Use 'delay' instead.",
            DeprecationWarning,
            stacklevel=2
        )
        actual_delay = slower

    executor._emit_event('before_dag_execute', dag)
    try:
        for vtx in dag.all_terminals():
            vtx.func_set_dag_output_when = lambda p, up, out, state: state != "aborted"
        if isinstance(processor, SequentialProcessor):
            selector = MaxSelector(1)
        if verbose == False:
            executor.verbose = False
        executor.dag = dag
        res = _raw_run(dag, selector, processor, executor, actual_delay, progressbar)
        executor._emit_event('after_dag_execute', dag)
    except Exception as e:
        if snapshot_on_error_path:
            log.error(f"Error occurred during DAG execution, snapshotting to {snapshot_on_error_path}...")
            dag.snapshot(snapshot_on_error_path)
        raise e

    return res


def resume_dag(dag: LangDAG, 
            selector=FullSelector(), 
            processor=SequentialProcessor(), 
            executor=LangExecutor(), 
            verbose: bool=True, 
            delay: bool | int | float =False, 
            progressbar: bool=True,
            snapshot_on_error_path: Optional[str] = None,
            *,
            slower: Optional[bool | int | float] = None):
    '''
    Precisely resumes a recovered DAG from the last point of failure.
    '''
    actual_delay = delay
    if slower is not None:
        warnings.warn(
            "'slower' is deprecated and will be removed in a future version. Use 'delay' instead.",
            DeprecationWarning,
            stacklevel=2
        )
        actual_delay = slower

    # 1. Calculate initial indegree for all nodes
    indegree_dict = {vtx: dag.indegree(vtx) for vtx in dag.vertices()}
    
    # 2. Find already finished nodes and nodes ready to run
    vertices_finished = []
    vertices_zero_indegree = set()

    for vtx in dag.vertices():
        if vtx.execution_state == 'finished':
            vertices_finished.append(vtx)
            # 3. Simulate their completion by decrementing successors' indegrees
            for v_to in dag.successors(vtx):
                indegree_dict[v_to] -= 1

    # 4. The new starting set are nodes that now have zero indegree
    for vtx, indegree in indegree_dict.items():
        if indegree == 0 and vtx.execution_state != 'finished':
            vertices_zero_indegree.add(vtx)

    # 5. Run the dag with the reconstructed state
    try:
        if isinstance(processor, SequentialProcessor):
            selector = MaxSelector(1)
        if verbose == False:
            executor.verbose = False
        executor.dag = dag
        res = _raw_run(
            dag, selector, processor, executor, actual_delay, progressbar,
            indegree_dict=indegree_dict,
            vertices_zero_indegree=vertices_zero_indegree,
            vertices_final=vertices_finished
        )
    except Exception as e:
        if snapshot_on_error_path:
            log.error(f"Error occurred during DAG execution, snapshotting to {snapshot_on_error_path}...")
            dag.snapshot(snapshot_on_error_path)
        raise e

    return res


async def _araw_run(dag: LangDAG,
                    selector=FullSelector(),
                    processor=SequentialProcessor(), # Note: Processor is less relevant here
                    executor=AsyncLangExecutor(),
                    delay: bool | int | float = False,
                    progressbar: bool = True,
                    indegree_dict: Optional[Dict[Node, int]] = None,
                    vertices_zero_indegree: Optional[Set[Node]] = None,
                    vertices_final: Optional[List[Node]] = None):
    """
    Asynchronous core DAG execution logic.
    """
    if indegree_dict is None:
        indegree_dict = {vtx: dag.indegree(vtx) for vtx in dag.vertices()}
    if vertices_final is None:
        vertices_final = []
    vertices_running = set()
    if vertices_zero_indegree is None:
        vertices_zero_indegree = dag.all_starts()

    pb_columns = [*Progress.get_default_columns()[:-1], TimeElapsedColumn()]

    with Progress(*pb_columns) as progress:
        task_num = len(dag.vertices())
        if progressbar:
            task = progress.add_task("[green]Processing (async)...", total=100)

        while vertices_zero_indegree or vertices_running:
            if delay:
                await asyncio.sleep(delay if isinstance(delay, (int, float)) else 1)

            vertices_idle = vertices_zero_indegree - vertices_running
            vertices_to_run = selector.select(vertices_running, vertices_idle)

            if not vertices_to_run and not vertices_running:
                break # All done

            _call_method(executor, 'report_start', vertices_to_run)
            vertices_running.update(vertices_to_run)
            vertices_zero_indegree.difference_update(vertices_to_run)

            # Execute tasks concurrently
            tasks = [executor.execute(executor.param(vtx)) for vtx in vertices_to_run]
            processed_results_list = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            processed_results = []
            for i, vtx in enumerate(vertices_to_run):
                result = processed_results_list[i]
                if isinstance(result, Exception):
                    raise result # Propagate exceptions
                processed_results.append((vtx, result))


            _call_method(executor, 'report_finish', processed_results)

            vertices_processed = [result[0] for result in processed_results]
            vertices_running.difference_update(vertices_processed)
            vertices_final.extend(vertices_processed)

            for vtx, result in processed_results:
                for v_to in dag.successors(vtx):
                    _call_method(executor, 'deliver', vtx, v_to, result)
                    indegree_dict[v_to] -= 1
                    if indegree_dict[v_to] == 0:
                        vertices_zero_indegree.add(v_to)
            if progressbar:
                progress.update(task, advance=100 * len(vertices_processed) / task_num)
        if progressbar:
            progress.update(task, description="[green]Finished (async)", completed=100)

    return vertices_final


async def arun_dag(dag: LangDAG,
                   selector=FullSelector(),
                   executor=AsyncLangExecutor(),
                   verbose: bool = True,
                   delay: bool | int | float = False,
                   progressbar: bool = True,
                   snapshot_on_error_path: Optional[str] = None):
    """
    Asynchronously runs a DAG, supporting both async and sync nodes.
    If you have any `async def` nodes, you must use this runner.
    """
    await executor._emit_event_async('before_dag_execute', dag)
    try:
        for vtx in dag.all_terminals():
            vtx.func_set_dag_output_when = lambda p, up, out, state: state != "aborted"
        if not verbose:
            executor.verbose = False
        executor.dag = dag
        res = await _araw_run(dag, selector, None, executor, delay, progressbar)
        await executor._emit_event_async('after_dag_execute', dag)
    except Exception as e:
        if snapshot_on_error_path:
            log.error(f"Error during async DAG execution, snapshotting to {snapshot_on_error_path}...")
            dag.snapshot(snapshot_on_error_path)
        raise e
    return res
