from typing import List, Set, Dict, Tuple, Optional, Any, Callable
import copy
from langdag.utils import merge_dicts
from langdag.error import ConflictConditionsError
from rich import print
from langdag.core import Node, LangDAG
from langdag.plugins.base import Plugin
import asyncio
import inspect

import logging
from rich.logging import RichHandler

FORMAT = "% (message)s"
logging.basicConfig(
    level="INFO", 
    format=FORMAT, 
    datefmt="[%X]", 
    handlers=[RichHandler()]
)

log = logging.getLogger("rich")


class LangExecutor:
    """
    A executor defines how DAG is going to execute.

    Args:
        verbose  (`boolean`, *optional*, defaults to `True`):
            when verbose==True, info will be printed to console
        func_start_hook (`Callable`, *optional*, defaults to `None`):
            A function accepts a node instance and do something customizable before a node execute.
        func_finish_hook (`Callable`, *optional*, defaults to `None`):
            A function accepts a node instance and do something 
            customizable before a node execute.
 """
    def __init__(
            self,
            verbose: bool = True,
            func_start_hook: Optional[Callable[[Node], Any]] = None,
            func_finish_hook: Optional[Callable[[Node], Any]] = None,
            plugins: Optional[List[Plugin]] = None,
        ) -> None:
        self.__upstream_output: Dict = {}
        self.verbose = verbose
        self.func_start_hook = func_start_hook
        self.func_finish_hook= func_finish_hook
        self.dag: Optional["LangDAG"] = None
        self.plugins = plugins or []

    def _emit_event(self, event_name: str, *args, **kwargs):
        for plugin in self.plugins:
            if hasattr(plugin, event_name):
                getattr(plugin, event_name)(*args, **kwargs)

    def param(self, vertex):
        node_itself = vertex
        node_upstream_output = copy.copy(self.__upstream_output.get(vertex, None)) or {}
        return (node_itself, node_upstream_output)

    def execute(self, param):
        token = None
        if self.dag:
            token = LangDAG.set_current(self.dag)
        
        node_itself, node_upstream_output = param
        node_itself.upstream_output = node_upstream_output

        if self.verbose : 
            log.info("   (2) [bold yellow]->o[/] [bold yellow]%s[/] received upstream: %s", 
                     node_itself.node_id, node_upstream_output, 
                     extra={"markup": True})

        try:
            self._emit_event('before_node_execute', node_itself)
            node_itself.run_node(verbose = self.verbose, func_start_hook=self.func_start_hook)
            self._emit_event('on_node_success', node_itself)
        except Exception as e:
            self._emit_event('on_node_error', node_itself, e)
            raise e
        finally:
            self._emit_event('after_node_execute', node_itself)


        if self.verbose : 
            log.info("     (3) [bold yellow]o->[/] [bold yellow]%s[/] output: %s", 
                     node_itself.node_id, 
                     node_itself.node_output, 
                     extra={"markup": True})

        if token:
            LangDAG.reset_current(token)
        return {node_itself.node_id : node_itself.node_output}
    
    def report_start(self, vertices):
        '''Report the start state'''
        for vertex in vertices:
            if self.verbose : 
                log.info("[dim]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/] \n(1) [bold red]%s START[/]", 
                         vertex, 
                         extra={"markup": True})
            # below moved to `execute`
            # if self.func_start_hook:
            #     self.func_start_hook(vertex.node_id, vertex.node_desc)

    def report_finish(self, vertices_result: Tuple):
        for vertex, node_output in vertices_result:
            if self.verbose:
                if vertex.execution_state != "aborted":
                    log.info('       (4) [bold yellow]√[/] [bold yellow]{0}[/] finished: Execution state `{1}`, Output: {2}'.format(vertex.node_id, vertex.execution_state, node_output), extra={"markup": True})
                else:
                    log.info(f'      (4) [bold purple]X {vertex.node_id} aborted![/]', 
                             extra={"markup": True})

            if self.func_finish_hook:
                self.func_finish_hook(vertex)

    def deliver(self, vertex, v_to, result: Dict):
        if v_to.node_id in vertex.downstream_execution_condition.keys():
            v_to.conditional_excecution = True
            if isinstance(vertex.downstream_execution_condition[v_to.node_id], list):
                raise ConflictConditionsError(f'Conflict conditional edges from {vertex.node_id} to {v_to.node_id}', 
                                              vertex.downstream_execution_condition[v_to.node_id])
            v_to.execution_condition = merge_dicts(v_to.execution_condition, 
                                                   vertex.downstream_execution_condition[v_to.node_id])
        
        v_to.upstream_execution_state.update({vertex.node_id: vertex.execution_state})
        
        if result != {vertex.node_id: None}:
            if self.__upstream_output.get(v_to, None):
                self.__upstream_output[v_to].update(result)
            else:
                self.__upstream_output[v_to] = result


class AsyncLangExecutor(LangExecutor):
    """
    An executor that handles both synchronous and asynchronous node execution
    for use with `arun_dag`.
    """
    async def _emit_event_async(self, event_name: str, *args, **kwargs):
        for plugin in self.plugins:
            if hasattr(plugin, event_name):
                method = getattr(plugin, event_name)
                if inspect.iscoroutinefunction(method):
                    await method(*args, **kwargs)
                else:
                    method(*args, **kwargs)

    async def execute(self, param):
        """
        Asynchronously executes a node's transform function.

        If the function is a coroutine, it is awaited directly.
        If it is a regular function, it is run in a separate thread
        to avoid blocking the asyncio event loop.
        """
        token = None
        if self.dag:
            token = LangDAG.set_current(self.dag)

        node_itself, node_upstream_output = param
        node_itself.upstream_output = node_upstream_output

        if self.verbose:
            log.info("   (2) [bold yellow]->o[/] [bold yellow]%s[/] received upstream: %s",
                     node_itself.node_id, node_upstream_output,
                     extra={"markup": True})

        try:
            await self._emit_event_async('before_node_execute', node_itself)
            
            # This is the core async logic
            if inspect.iscoroutinefunction(node_itself.func_transform):
                await node_itself.arun_node(verbose=self.verbose, func_start_hook=self.func_start_hook)
            else:
                await asyncio.to_thread(node_itself.run_node, verbose=self.verbose, func_start_hook=self.func_start_hook)
            
            await self._emit_event_async('on_node_success', node_itself)
        except Exception as e:
            await self._emit_event_async('on_node_error', node_itself, e)
            raise e
        finally:
            await self._emit_event_async('after_node_execute', node_itself)


        if self.verbose:
            log.info("     (3) [bold yellow]o->[/] [bold yellow]%s[/] output: %s",
                     node_itself.node_id,
                     node_itself.node_output,
                     extra={"markup": True})

        if token:
            LangDAG.reset_current(token)
        return {node_itself.node_id: node_itself.node_output}
