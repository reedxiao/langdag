# LangDAG

![inspect exec](./docs/inspect_exec.png)

<div align="center">
  <br />
  <p align="center">
    Build powerful and observable LLM agent workflows with Directed Acyclic Graphs (DAGs).
  </p>
  <p align="center">
    <a href="https://pypi.org/project/langdag/">
      <img alt="PyPI" src="https://img.shields.io/pypi/v/langdag.svg?color=blue">
    </a>
    <a href="./LICENSE.txt">
      <img alt="License" src="https://img.shields.io/pypi/l/langdag.svg?color=blue">
    </a>
  </p>
</div>

## Introduction

> **Note:** LangDAG is currently experimental and under active development.

**LangDAG** is a Python-based orchestration framework for building complex agentic workflows for Large Language Models (LLMs) using Directed Acyclic Graphs (DAGs).

Drawing inspiration from established data orchestration tools like Airflow, LangDAG applies the power and simplicity of DAGs to the domain of LLM agents. The result is a framework that is both expressive and robust.

**Key Features:**

- **Framework Agnostic:** Prioritizes plain functions, avoiding dependencies on specific LLM frameworks.
- **Intuitive Syntax:** A clean and intuitive syntax for defining complex workflows.
- **Stateful Execution:** A shared state is accessible by all nodes within a DAG.
- **Concurrent Execution:** Automatically identifies and executes independent tasks in parallel.
- **Asynchronous Operations:** Native support for `async` operations, ideal for I/O-bound tasks.
- **Conditional Routing:** Dynamically route workflows using conditional edges based on node outputs.
- **Extensible Plugin System:** Easily add custom logic for logging, monitoring, and observability.
- **Lifecycle Hooks:** Customize behavior by injecting logic at critical points in the execution lifecycle.
- **Enhanced Observability:** Visualize the execution flow with a console-based tree diagram for easier debugging.
- **Modular Architecture:** A consistent and reusable structure for all nodes.

## Motivation

While simple LLM agent workflows are straightforward to build, their complexity grows exponentially as more steps are added. This often leads to several challenges:

- **Code Navigation:** Traversing nested `if/else` blocks and `for` loops to modify logic becomes cumbersome.
- **State Tracking:** Manually logging intermediate states and tracking conditional branches is error-prone.
- **Debugging:** Understanding the execution path and how an input triggered a specific outcome is difficult.
- **Reusability:** Reusing workflow components often devolves into copying and pasting code.
- **Extensibility:** Adding hooks or callbacks requires intrusive modifications to existing code.
- **Performance:** Manually implementing concurrency to speed up execution is complex and time-consuming.
- **Framework Lock-in:** Inflexible frameworks can limit the ability to adapt and extend workflows.

LangDAG is designed to address these challenges, offering a structured and scalable solution for building and managing sophisticated LLM agent workflows.

### A Hybrid Approach: DAGs for Orchestration, Tool-Calling for Execution

While LangDAG provides a powerful way to structure complex workflows, some tasks require more flexibility than a predefined graph can offer. For example, an agent might need to perform an unknown number of steps to fulfill a request, like reading a file, modifying its content, and then renaming it.

To address this, LangDAG promotes a powerful **hybrid design philosophy**:

1.  **High-Level Orchestration with a DAG**: Use the `LangDAG` to define the major, predictable stages of your agent's workflow. This provides a clear, observable structure. A typical pattern might be:
    `analyze_intent` >> `execute_task` >> `compose_response`

2.  **Flexible Execution with a Tool-Calling Node**: Encapsulate the complex, dynamic parts of the workflow within a single, powerful node. This "executor" node contains its own LLM-driven loop that can intelligently call a set of predefined tools (e.g., file I/O, web search) as many times as needed to complete the task.

This approach combines the best of both worlds:
-   **Structure & Observability**: The DAG gives you a clear overview of the agent's high-level process.
-   **Flexibility & Autonomy**: The tool-calling node gives the agent the freedom to reason and act dynamically to solve complex problems.

This is analogous to project management: the DAG represents the project milestones, while the tool-calling node is like an autonomous team member who has the skills (tools) and intelligence to figure out the detailed steps required to hit those milestones.

## 📑 Contents

1. **[💬 Concepts](#-Concepts)**

2. **[🔧 Setup](#-setup)**

3. **[💻 Usage](#-usage)**

4. **[📕 API Reference](#-api-reference)**

## 💬 Concepts

A few core concepts are helpful to understand before getting started.

### Node

A **node** (or *vertex*) is the fundamental building block of a DAG. In LangDAG, each node is an instance of the `Node` class, defined by a unique `node_id` and an optional `prompt`.

The `prompt` acts as a static, predefined input for the node, which is particularly useful in LLM applications.

Each node can receive data from upstream nodes and combine it with its own `prompt`. This combined input is then processed by a `transforming function` (`func_transform`), which produces an output that is passed to downstream nodes.

### Edge

An **edge** is a directional connection from an upstream node to a downstream node. To prevent infinite loops, cycles are not allowed.

In LLM agent workflows, complex tasks are often broken down into a sequence of steps. An edge, such as `A >> B`, defines this sequence, indicating that `Node_A` must execute before `Node_B`. The output of `Node_A` is then passed to `Node_B` as its **upstream output**.

It's important to note that a node only has access to the outputs of its immediate parents. For example, in a chain `A >> B >> C`, Node C cannot access the output of Node A unless an explicit edge `A >> C` is defined.

By default, a node will only execute after all of its upstream dependencies have successfully completed.

### Conditional Edge

LangDAG also supports **conditional edges**, which allow you to dynamically control the execution path based on the output of a node.

For example, the following syntax defines a conditional edge:

```python
A >> 1 >> B
```

This means that Node B will only execute if the output of Node A is `1`.

### Node Input and Output

As mentioned, nodes receive a dictionary of outputs from their upstream parents. For a `Node_B` with two parents, `Node_A1` and `Node_A2`, the input would be:

```python
{"node_A1": Any, "node_A2": Any}
```

The keys of this dictionary are the `node_id`s of the parent nodes.

The output of `Node_B` is also a dictionary, with its own `node_id` as the key:

```python
{"node_B": Any}
```

### DAG (Directed Acyclic Graph)

In LangDAG, a **DAG** is a collection of nodes and edges that define a complete workflow. The `LangDAG` class orchestrates the execution of these nodes according to the defined dependencies.

### DAG State (`dag_state`)

While nodes can pass data to their immediate children, they cannot access the state of nodes that are not directly connected to them. To solve this, LangDAG provides a shared **DAG State** (`dag_state`), which is a dictionary accessible to all nodes in the DAG.

The `dag_state` is initialized with the following reserved keys:

```python
{
    "id": dag_id,        # Optional ID provided when the DAG is created
    "input": dag_input,  # Optional input provided when the DAG is created
    "specs": {},         # Stores the specifications of all nodes
    "output": None       # The final output of the DAG
}
```

You can store any additional data in the `dag_state` during a node's execution, and it will be available to all subsequent nodes.

### Execution Behavior

A node can be in one of three states: `initialized`, `finished`, or `aborted`.

- A node is **acceptable** if it has `finished` and its output meets the condition of the edge connecting to the downstream node.
- By default, a node will only execute if **all** of its upstream parents are "acceptable."
- This behavior can be changed to allow a node to execute if **any** of its upstream parents are "acceptable."

If a node is not executed, its state is marked as `aborted`.

### Putting It All Together

- A **node** is a single step in a workflow.
- A **DAG** organizes these steps and their dependencies.
- **Edges** connect nodes and define the flow of data.
- **Conditional edges** allow for dynamic routing.
- The **DAG State** provides a shared memory space for all nodes.
- The **execution behavior** can be customized to control how nodes are triggered.



## 🔧 Setup

### Installation

Install LangDAG using pip:

```sh
$ pip install langdag
```

From Python 3.12, you will need to create & activate a virtual environment before using the command.

### Import

Then import it:

```python
from langdag import Node
```

To save some time, though not necessary, you may import all of these before try out the examples.

```python
from langdag import Node, LangDAG, run_dag
from langdag.processor import MultiThreadProcessor, SequentialProcessor
from langdag.selector import MaxSelector
from langdag.executor import LangExecutor
from langdag.utils import default, Empty, NotEmpty, ContainsAll, SubsetOf
from langdag.decorator import make_node
```

## 💻 Usage

### Defining Nodes

To create a node in LangDAG, use the `Node` class. Here’s an example:

```python
node_1 = Node(
    node_id="node_1",
    prompt="SOME PROMPT...",
    func_transform=lambda prompt, upstream_output, dag_state: ...(YOUR LOGIC HERE),
)
```

**Main Parameters**:

- **node_id**: *(required)*, a unique identifier for each node instance.

- **prompt**: *(optional, defaults to `None`)*, a predefined prompt for the node.

- **func_transform**: *(optional)*, a function that takes `prompt`, `upstream_output`, and `dag_state` as inputs and generates the output of the current node. If not defined, the output will be `None`. You can use a Python `lambda function` for simplicity or a regular function for more flexibility. Please note that upstream output in LangDAG refers to the outputs of the upstream nodes that have explicit connections to the node. For example, In `NodeA >> NodeB >> NodeC`, the output of NodeA will not be accessible to NodeC unless we explicit add another `NodeA >> NodeC`. 


Here’s an example node_2 is generating an answer based on city name extracted by node_1 from the use query.

```python
# Placeholder function for demonstration
def get_weather(city: str) -> str:
    return "sunny"

node_2 = Node(
    node_id="node_2",
    prompt="The weather in {city} is {weather}.",
    func_transform=lambda prompt, upstream_output, dag_state: 
        prompt.format(
            city=upstream_output['node_1'], 
            weather=get_weather(upstream_output['node_1'])
        )
)
```

In this case, the output might be `The weather in NY is sunny.`.


**Execution Order**: When a node executes, `func_transform` runs first

After execution, the node output can be accessed using `node.node_output`.

Additionally, when making function / tool calling agents, you may want to add `spec` to the node by 
setting node.spec attribute or use `node.add_spec()` method after creating a node. For example,

```python
node_2 = Node(
    node_id="node_2",
    prompt="The weather in #CITY is #WEATHER.",
    spec = {"type": "function", "function": {}},
    func_transform=lambda prompt, upstream_output, dag_state: 
        prompt.replace('#CITY', upstream_output['node_1'])
              .replace('#WEATHER', get_weather(upstream_output['node_1'])),
)
```

or 

```python
node_2 = Node(
    node_id="node_2",
    prompt="The weather in {city} is {weather}.",
    func_transform=lambda prompt, upstream_output, dag_state: 
        prompt.format(city=upstream_output['node_1'], 
                      weather=get_weather(upstream_output['node_1']))
)

node_2.add_spec({"type": "function", "function": {}})
```

After adding a spec to a `node`, you can acess the spec with `node.spec`, or you can alse get a list of specs of all nodes (if spec available) in a DAG (will be explained later) `dag` by `dag.get_all_specs()`.


### Define Nodes with Decorators

> `@make_node()`

You can use the `@make_node()` decorator above a transforming function (ie `func_transform`) to create a node from that function. This method is useful for creating nodes where the `node_id` is the function (ie `func_transform`) name, and when the function is hard to defined with a simple lambda function.

For example, the following two methods of creating nodes are equivalent:

```python
node_2 = Node(
    node_id="node_2",
    prompt="The weather in {city} is {weather}.",
    func_transform=lambda prompt, upstream_output, dag_state: 
        prompt.format(city=upstream_output['node_1'], 
                      weather=get_weather(upstream_output['node_1']))
)
```

and 

```python
from langdag.decorator import make_node

@make_node(prompt="The weather in {city} is {weather}.")
def node_2(prompt, upstream_output, dag_state): 
    res = prompt.format(city=upstream_output['node_1'], weather=get_weather(upstream_output['node_1']))
    return res
```

Though the `@make_node()` decorator provide a different way to create a node by directly associating the function with the node's transformation logic, the `@make_node()` decorator has the same functionality as the `Node()` class. It accepts the same parameters as `Node()`, except it uses the decorated function as `func_transform`, and the `node_id` defaults to the name of the decorated function if not explicitly set (the `node_id` parameter can also be manually set to an id that is different from the function name).

> `@make_node(spec = {...})`

Use spec parameter in `@make_node()` decorator to add function / tool spec to this node.
This is optional, but will be helpful if you are working on function calling or tool calling, and 
want to define function / tool spec on a Node. After adding a spec to a `node`, you can acess the spec with `node.spec`, and you can also get a list of specs of all nodes (if spec available) in a DAG `dag` by `dag.get_all_specs()`.

### Empowering Nodes with a `Toolbox`

To support the hybrid design philosophy, LangDAG provides a `Toolbox` class. This allows you to register a collection of Python functions as "tools" that a tool-calling node can execute.

**Creating and Using a Toolbox:**

1.  **Instantiate a Toolbox:**
    ```python
    from langdag.decorator import Toolbox
    toolbox = Toolbox()
    ```

2.  **Register Functions as Tools:**
    Use the `@toolbox.add_tool()` decorator on any function you want to make available to your agent.

    ```python
    @toolbox.add_tool(auto_spec=True)
    def read_file(filepath: str):
        """
        Reads the content of a specified file.
        filepath (str): The path to the file to read.
        """
        # ... implementation ...
    ```

**Tool Specification (`spec`):**

For an LLM to know how to use your functions, it needs a specification (a JSON schema). `Toolbox` gives you two options:

-   **`auto_spec=True` (Recommended)**: Automatically generates a spec from your function's signature and docstring. Just add type hints and a clear docstring, and LangDAG handles the rest.
-   **`spec={...}`**: Manually provide a complete, OpenAI-compatible JSON schema for full control.

3.  **Using the Toolbox in a Node:**
    Inside a tool-calling node, you can pass the list of all tool specs to your LLM and use `toolbox.call_tool_by_name()` to execute the functions the LLM chooses.

    ```python
    # Get all specs for the LLM
    tools = toolbox.get_all_specs()

    # Execute a function chosen by the LLM
    result = toolbox.call_tool_by_name("read_file", filepath="/path/to/file.txt")
    ```

The `Toolbox` makes it easy to create a library of capabilities that your agent can dynamically use to solve a wide range of problems.

### Default Upstream Output

If a node has only one upstream node, and you want to access its output directly in `func_transform`, you can use the `default` function from `langdag`.

```python
from langdag.utils import default

node_2 = Node(
    node_id="node_2",
    prompt="The weather in {city} is {weather}.",
    func_transform=lambda prompt, upstream_output, dag_state: 
        prompt.format(city=default(upstream_output), 
                      weather=get_weather(default(upstream_output)))
)
```

The `default` function accept a dict with a single item, otherwise it raise an error.

To improve reusability of a Node, we encourage using `default` instead of using upstream `node_id`. If there are multiple upstream outputs, use `list(upstream_output.values())` to get them as a whole. 


### Define and Run a DAG (Syntax #1)

A Directed Acyclic Graph (DAG) represents a workflow with multiple steps and paths, unlike simple straight-line step-by-step workflows.

To add nodes to a DAG and connect them, use the following syntax:

```python
from langdag import Node, LangDAG

user_question = "Tell me more about SF."

with LangDAG(dag_input=user_question) as dag:
    dag += node_1
    dag += node_2
    dag += node_3
    dag += node_4
    dag += node_5
    dag += node_6

    node_1 >> node_2
    node_2 >> node_3 >> node_6
    node_2 >> node_4 >> node_6
    node_2 >> node_5 >> node_6
```

or you can 
This code defines a DAG and connects nodes as shown:

```text
╭─────────────────────────────────────────────────────────────────╮
│                                                                 │
│     ┌──────┐    ┌──────┐   ┌────────┐       ┌──────┐            │
│     │node_1│ ──▷│node_2├──▷│ node_3 ├──────▷│node_6│            │
│     └──────┘    └──┬───┘   └────────┘       └───▲──┘            │
│                    │       ┌────────┐           │               │
│                    ├──────▷│ node_4 ├───────────┤               │
│                    │       └────────┘           │               │
│                    │       ┌────────┐           │               │
│                    └──────▶│ node_5 ├───────────┘               │
│                            └────────┘                           │
│                                                                 │
╰─────────────────────────────────────────────────────────────────╯
```

Example use case:

1. **node_1**: Translate user question to English.
2. **node_2**: Extracts a English city name (with LLM).
3. **node_3**: Checks the city's introduction.
4. **node_4**: Gets the city's best places to visit.
5. **node_5**: Checks the city's weather.
6. **node_6**: Combines all the information into a friendly response.

To execute the DAG, use the `run_dag` function:

```python
from langdag.processor import MultiThreadProcessor
user_question = "Tell me more about SF."
with LangDAG(dag_input=user_question) as dag:
    dag += node_1
    dag += node_2
    dag += node_3
    dag += node_4
    dag += node_5
    dag += node_6

    node_1 >> node_2
    node_2 >> node_3 >> node_6
    node_2 >> node_4 >> node_6
    node_2 >> node_5 >> node_6

    run_dag(dag)  # <--- THIS LINE
    # run_dag(dag, processor=MultiThreadProcessor())  # Or run concurrently

    print(dag.dag_state["output"])
```

**Note**: The `run_dag` function can be used either within or not within the `with LangDAG()` context.

You can also run the DAG concurrently using `MultiThreadProcessor`. The final output will be accessible via `dag.dag_state["output"]`.


### Define and Run a DAG without Context Manager  (Syntax #2)

For the above example, though it is recommended to use `with` context manager and simplified `+=` and `>>` syntax, you can also define the DAG without these.

```python
from langdag.processor import MultiThreadProcessor
user_question = "Tell me more about SF."

dag = LangDAG(dag_input=user_question)
dag.add_node(node_1, node_2, node_3, node_4, node_5, node_6 )
dag.add_edge(node_1, node_2)
dag.add_edge(node_2, node_3)
dag.add_edge(node_2, node_4)
dag.add_edge(node_2, node_5)
dag.add_edge(node_3, node_6)
dag.add_edge(node_4, node_6)
dag.add_edge(node_5, node_6)

run_dag(dag)

print(dag.dag_state["output"])

```


### DAG Execution Observability

To improve observability and inspect the execution of a DAG, you can use the following method:

```python
dag.inspect_execution()
```

This prints a tree-structure diagram. For example:

```sh
DAG INPUT: hello222
┗━━ node_1, desc1, (√), ## <- means node_execution_state=`finished` 
    OUTPUT: False
    ┣━━ node_3, node_desc_of_node_3, (X), ## <- means node_execution_state=`aborted` 
    ┃   OUTPUT: None, Condition not matched: {'node_1': True} ## output of node_1 is False but condition require True
    ┗━━ node_2, node_desc_of_node_2, (√), 
        OUTPUT: False
        ┗━━ node_3, node_desc_of_node_3, (X), 
            OUTPUT: None, Condition not matched: {'node_2': 1}
```

In this diagram:

- (√) indicates that the node execution state is finished.
- (X) indicates that the node execution state is aborted.
- The output of each node is displayed along with conditions that were met or not met. (conditon explain later)

This visualization helps you understand the execution flow and identify any issues or unmet conditions in your DAG.

### DAG Input or `dag_input`

As demonstrated in the previous example, you can use the dag_input parameter when instantiating a node to define an input for all nodes in the DAG:

```python
with LangDAG(dag_input="SOMETHING") as dag:
    dag += node_1
    ...
```

All nodes can access the DAG input in the func_transform method of the node class by using dag_state["input"].


### Find Starts & terminals in a DAG

To find all the starting and terminating nodes, use

```python
print(dag.all_starts())
print(dag.all_terminals())
```

### Cycles in a Graph

Cycles are not allowed in a DAG as they create infinite loops. For example:

```python
with LangDAG() as dag:
    dag += node_1
    dag += node_2
    dag += node_3_1
    dag += node_3_2
    dag += node_4
    dag += node_5

    node_1 >> node_2
    node_2 >> node_3_1 >> node_4 >> node_1
    node_2 >> node_3_2

    run_dag(dag)
```

This will cause an error:

```python
paradag.error.DAGCycleError: Cycle if add edge from "node_4" to "node_1"
```

### Conditional Edges

LangDAG supports conditional execution with **Conditional Edges**. Let us compare two example to see the difference conditional edges make.

Without **Conditional Edges**:

```python
# without conditional edges
with LangDAG("some_input") as dag:
    dag += node_input_clean
    dag += node_check_if_equation
    dag += node_eval
    dag += node_just_answer

    node_input_clean >> node_check_if_equation
    node_check_if_equation >> node_eval # node_eval need a internal logic to tell `if there is eq` first
    node_check_if_equation >> node_just_answer # node_just_answer need a internal logic to tell `if there is eq` first

    run_dag(dag)
```

With **Conditional Edges**:

```python
# with conditional edges
with LangDAG("some_input") as dag:
    dag += node_input_clean
    dag += node_check_if_equation
    dag += node_eval
    dag += node_just_answer

    node_input_clean >> node_check_if_equation
    node_check_if_equation >> True >> node_eval # node_eval does not need a internal logic to tell `if there is eq` first, if condition not met node_eval will be aborted 
    node_check_if_equation >> False >> node_just_answer # node_just_answer does not need a internal logic to tell `if there is eq` first, if condition not met node_just_answer will be aborted 

    run_dag(dag)
```

**Note**: Conditional edges should be unique between two nodes and should only be used as follows:

```python
node_1 >> [condition] >> node_2
```

**Note**: Conditional edges use == in Python to determine if the condition matches the upstream output. Consequently, the following boolean comparisons, for example, will also match:

```python
True == 1  # returns True
False == 0  # returns True
{1:1} == {True:True} # returns True
```

This 'equal' property is also utilized in special condition defining, as we will see in *"Conditions with Special Classes"* later.


**Note**: Adding conditional edges without context manager (Syntax B), please use `add_conditional_edge`, for example:

```python
 dag.add_conditional_edge(node_1, True, node_2)
```

### Multiple Conditions and Upstream Nodes

We define *"Exectuion behavior"* as:

- For a node, an upstream node is *"acceptable"* if it is *finished* and condition met(if condition edge exist).
- For a node, an upstream node is not *"acceptable"* if it is *not finished* or it is *finished* but condition not met.

By default, a node will execute if all upstream nodes are *"acceptable"*, otherwise it will not be executed (ie, it will be aborted).

Consider the following example:

```python
with LangDAG("my input") as dag:
    dag += node_1
    dag += node_2
    dag += node_3
    dag += node_5

    node_1 >> 1 >> node_5
    node_2 >> True >> node_5
    node_3 >> 3 >> node_4 >> node_5

    run_dag(dag)
```

In this case, by default, `node_5` will execute only if 
- `node_1` is finished (as a starting node, `node_1` will always finished) and outputs `1` 
- **and** `node_2` is finished and outputs `True` (as a starting node, `node_2` will always finished) 
- **and** `node_4` is finished (ie., `node_3` outputs `3` and the condition `3` is met, and `node_4` is not aborted).

This behavior can be customized using the following node methods:

- `node.exec_if_any_upstream_acceptable()`

For example, to make `node_5` execute if **any** upstream node is **acceptable**, use the `exec_if_any_upstream_acceptable()` method on the node instance:

```python
with LangDAG("my input") as dag:
    dag += node_1
    dag += node_2
    dag += node_3
    dag += node_5
    node_5.exec_if_any_upstream_acceptable()

    node_1 >> 1 >> node_5
    node_2 >> True >> node_5
    node_3 >> 3 >> node_4 >> node_5

    run_dag(dag)
```

Alternatively:

```python
with LangDAG("my input") as dag:
    dag += node_1
    dag += node_2
    dag += node_3
    dag += node_5
    node_5.exec_if_any_upstream_acceptable()

    node_1 >> 1 >> node_5
    node_2 >> True >> node_5
    node_3 >> 3 >> node_4 >> node_5

    run_dag(dag)
```

With this new behavior, `node_5` will execute if either of node_1, node_2, node_4 is **acceptable**, that is when 
- [`node_1` is finished (as a starting node, `node_1` will always finished) and outputs `1` ]
- **or** [`node_2` is finished and outputs `True` (as a starting node, `node_2` will always finished) ]
- **or** [`node_4` is finished (ie., `node_3` outputs `3` and the condition `3` is met, and `node_4` is not aborted).]


### Conditions with Special Classes

While standard equality checks (`output == condition`) are useful, LangDAG provides a set of special condition classes for more advanced and readable comparisons.

To use them, import them from `langdag.utils` and place them in the condition part of an edge:

```python
from langdag.utils import ContainsAll, InstanceOf, Check
# ...
node_A >> ContainsAll([1, 2]) >> node_B
node_C >> InstanceOf(dict) >> node_D
```

Here are the available special classes:

- **`>> ContainsAll([...]) >>`**
  - **Checks:** If the node's output is a superset of the given list.
  - **Example:** `ContainsAll([1, 2])` is met by an output of `[1, 2, 3]`.

- **`>> SubsetOf([...]) >>`**
  - **Checks:** If the node's output is a subset of the given list.
  - **Example:** `SubsetOf([1, 2, 3])` is met by an output of `[1, 2]`.

- **`>> Empty() >>`**
  - **Checks:** If the node's output is "empty" (e.g., `None`, `False`, `0`, `""`, `[]`, `{}`).
  - **Example:** `Empty()` is met by an output of `[]`.

- **`>> NotEmpty() >>`**
  - **Checks:** If the node's output is *not* "empty".
  - **Example:** `NotEmpty()` is met by an output of `[1]`.

- **`>> EmptyDict() >>`**
  - **Checks:** If the node's output is an empty dictionary (`{}`).
  - **Example:** `EmptyDict()` is met by an output of `{}`.

- **`>> NotEmptyDict() >>`**
  - **Checks:** If the node's output is a non-empty dictionary.
  - **Example:** `NotEmptyDict()` is met by an output of `{'key': 'value'}`.

- **`>> InstanceOf(type) >>`**
  - **Checks:** If the node's output is an instance of the given type/class.
  - **Example:** `InstanceOf(dict)` is met by an output of `{'key': 'value'}`.

- **`>> Check(func, expected_result) >>`**
  - **Checks:** If the result of `func(output)` equals the `expected_result`. This is useful for complex checks without adding extra nodes.
  - **Example:** `Check(lambda x: x['status'], "success")` is met by an output of `{'status': 'success', 'data': [...]}`.

- **`>> CheckNot(func, unexpected_result) >>`**
  - **Checks:** If the result of `func(output)` does *not* equal the `unexpected_result`. 
  - **Example:** `CheckNot(lambda x: x['status'], "error")` is met by an output of `{'status': 'success', 'data': [...]}`.

These classes allow you to build complex and highly readable conditional logic directly into your DAG structure.
### Node `execution_state`

A node can be in one of three possible execution states (all represented as strings):

- `"initialized"`: The node is defined but not yet executed.
- `"finished"`: The node has executed successfully.
- `"aborted"`: The node was aborted due to unmet conditions.

### Setting DAG Output

For terminating nodes, their outputs are saved to `dag.dag_state["output"]` as the final DAG output if their `execution_state` is not `"aborted"`. In most LLM agent use cases, there should be only one terminating node with "finished" state after execution, so `dag.dag_state["output"]` will be set once.

However, in DAGs with multiple terminating nodes, the final output may be set multiple times in the order of node execution. This can add complexity and should be used cautiously.

### Concurrent Execution

To enable concurrent execution of the DAG, use `run_dag` as shown below:

```python
with LangDAG() as dag:
    ...
    run_dag(dag, processor=MultiThreadProcessor(), executor=LangExecutor())  # <--- THIS LINE
```

The default setting maximizes concurrent task execution. You can control this by setting the `selector` parameter to `MaxSelector(N)`, where `N` is the maximum number of nodes that can run concurrently. For example, `MaxSelector(4)` allows up to 4 nodes to run concurrently:

```python
with LangDAG() as dag:
    ...
    run_dag(
        dag,
        processor=MultiThreadProcessor(),
        selector=MaxSelector(4),
        executor=LangExecutor()
    )
```

### Asynchronous Execution

LangDAG also supports asynchronous execution, which is useful for I/O-bound tasks. To use this feature, you need to define async nodes and use `arun_dag` to run the DAG.

**Defining Async Nodes:**

Async nodes are defined similarly to sync nodes, but they use async functions for transformation and description.

```python
import asyncio

async def a_transform(prompt, upstream_output, dag_state):
    await asyncio.sleep(1)
    return "async result"

node_async = Node(
    node_id="node_async",
    func_transform=a_transform
)
```

**Running the DAG Asynchronously:**

To run the DAG asynchronously, use the `arun_dag` function.

```python
import asyncio

async def main():
    with LangDAG() as dag:
        dag += node_async
        await arun_dag(dag)
    print(dag.dag_state["output"])

asyncio.run(main())
```

`arun_dag` uses `AsyncLangExecutor` by default to handle both sync and async nodes.


**Example with FastAPI:**

Here's how you can use an async DAG within a FastAPI application:

```python
# main.py
import asyncio
from fastapi import FastAPI
from langdag import Node, LangDAG, arun_dag

# 1. Define an async node
async def a_transform(prompt, upstream_output, dag_state):
    # Simulate an async I/O operation
    await asyncio.sleep(1)
    return f"Input was: {dag_state['input']}"

node_async = Node(
    node_id="node_async",
    func_transform=a_transform
)

# 2. Create a FastAPI app
app = FastAPI()

# 3. Define an endpoint that uses the DAG
@app.post("/process")
async def process_data(data: dict):
    user_input = data.get("input")

    with LangDAG(dag_input=user_input) as dag:
        dag += node_async
        # Run the DAG asynchronously
        await arun_dag(dag, progressbar=False, verbose=False)

    # Return the result from the DAG's state
    return {"result": dag.dag_state["output"]}

# To run this example:
# 1. Install necessary packages: pip install fastapi "uvicorn[standard]"
# 2. Save the code as main.py
# 3. Run the server: uvicorn main:app --reload
# 4. Send a POST request to http://127.0.0.1:8000/process with a JSON body like: {"input": "hello world"}
```

### Snapshot and Recovery

To handle interruptions and make workflows more resilient, LangDAG supports snapshotting the state of a DAG during execution. If a run fails, you can recover the DAG from the snapshot and resume it from where it left off.

**Automatic Snapshots on Failure (Opt-in):**
You can configure `run_dag` and `resume_dag` to automatically save a snapshot upon failure by providing a file path to the `snapshot_on_error_path` parameter. This is an opt-in feature.

```python
run_dag(dag, snapshot_on_error_path="my_dag_snapshot.dill")
```
If an error occurs, the state of `dag` will be saved to `my_dag_snapshot.dill`. This is safe for concurrent runs, as you can provide a unique path for each run.

**Manual Snapshot and Recovery:**
You can also manually save and recover a DAG at any point.

```python
# Manually save a snapshot
dag.snapshot("my_snapshot.dill")

# Recover the DAG from the snapshot
from langdag import LangDAG
recovered_dag = LangDAG.recover("my_snapshot.dill")
```

**Resuming a Recovered DAG:**
To resume a recovered DAG from the point of failure, use the `resume_dag` function. It intelligently calculates the correct starting nodes and continues the execution.

```python
from langdag import resume_dag

# Assume recovered_dag is loaded from a snapshot
resume_dag(recovered_dag)

print(recovered_dag.dag_state["output"])
```

### Node Reset

When instantiated, a node has an internal state. To view this state, simply print the node:

```python
print(node_1)
```

After a DAG run, a node's state may change. Nodes are reusable, but you often do not want to carry over the state from one DAG run to another. Reset the node's internal state before reusing it in a new DAG:

```python
node_1.reset()
```

To reset all nodes in a previous DAG, use:

```python
dag.reset_all_nodes()
```


### Run DAG Silently

When running a DAG with `run_dag`, LangDAG will print useful logs. 

If you like to run the DAG without standard output (for example in production), set the `verbose` parameter of `LangExecutor` to `False`:

```python
with LangDAG() as dag:
    ...
    run_dag(
        dag,
        executor=LangExecutor(verbose=False) ## <---- Here
    )
```

or set  `verbose=False` directly in `run_dag`, which will do the above job for you.

```python
with LangDAG() as dag:
    ...
    run_dag(
        dag,
        verbose=False ## <---- Here
    )
```

Similarly, to run the DAG without progress bar, set the `progressbar` parameter of `run_dag` to `False`:

```python
with LangDAG() as dag:
    ...
    run_dag(
        dag,
        progressbar=False ## <---- Here
    )
```

### 🔌 Plugin System

LangDAG features a powerful, event-driven plugin system that allows you to hook into the core execution lifecycle. This makes it easy to add custom logic for logging, advanced monitoring, external integrations, and more, without modifying the core framework.

**How It Works:**

The system is built around a `Plugin` base class. You can create your own plugin by inheriting from this class and overriding the methods for the events you want to handle.

The available event hooks are:
- `before_dag_execute(dag)`
- `after_dag_execute(dag)`
- `before_node_execute(node)`
- `after_node_execute(node)`
- `on_node_success(node)`
- `on_node_error(node, error)`

**Creating a Custom Plugin:**

It's as simple as creating a class that inherits from `langdag.plugins.base.Plugin` and implementing the methods you need.

```python
from langdag.plugins.base import Plugin

class MyLoggingPlugin(Plugin):
    def before_node_execute(self, node):
        print(f"🚀 Starting execution of node: {node.node_id}")

    def on_node_success(self, node):
        print(f"✅ Node {node.node_id} finished successfully.")
        print(f"   Output: {node.node_output}")
```

**Using a Plugin:**

To use your plugin, simply instantiate it and pass it to the `LangExecutor` when you run your DAG.

```python
from langdag.executor import LangExecutor

my_plugin = MyLoggingPlugin()
executor = LangExecutor(plugins=[my_plugin])

run_dag(dag, executor=executor)
```

**Example: Integrating with Langfuse for Advanced Observability**

The plugin system makes it trivial to integrate with powerful third-party tools. For example, you can add detailed, trace-level observability to your entire workflow using [Langfuse](https://langfuse.com/).

Here’s how a `LangfusePlugin` could look:

```python
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
            name="my-agent-trace",
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

```

Now, you can get rich, interactive traces of your DAGs just by adding the plugin to the executor:

```python
# Add your Langfuse credentials
langfuse_plugin = LangfusePlugin(
    public_key="pk-lf-...",
    secret_key="sk-lf-...",
    host="https://cloud.langfuse.com"
)

# Pass the plugin to the executor
executor = LangExecutor(plugins=[langfuse_plugin])
run_dag(dag, executor=executor)
```
This will produce a detailed trace in Langfuse, giving you unparalleled insight into your agent's execution flow.

### Extending LangDAG: Plugins vs. Hooks

LangDAG offers two primary mechanisms to extend its functionality: the full-featured **Plugin System** and simple **Lifecycle Hooks**.

- **Plugins** are powerful, stateful, and class-based. They provide a structured way to handle complex logic and integrate with external systems by giving you access to the complete DAG and node lifecycle, including error handling.
- **Hooks** (`func_start_hook`, `func_finish_hook`) are lightweight, stateless functions. They are perfect for simple, one-off actions that don't require managing state or complex logic.

**When to Use Which:**

| Use Case                               | Recommendation  | Why?                                                                                             |
| -------------------------------------- | --------------- | ------------------------------------------------------------------------------------------------ |
| Quick debugging or logging             | **Hooks**       | Simple `lambda` functions are perfect for quick, temporary logging without creating a new class.   |
| Simple, stateless notifications        | **Hooks**       | If you just need to know when a node starts or finishes, a hook is the most direct way.           |
| **Stateful operations** (e.g., timing) | **Plugins**     | A plugin instance can store state (like a start time) between `before` and `after` events.       |
| **Complex or multi-step logic**        | **Plugins**     | The class structure of plugins is much cleaner for organizing logic than complex hook functions. |
| **Separating concerns**                | **Plugins**     | You can attach multiple, independent plugins (e.g., one for logging, one for metrics).           |
| **Reusable extensions** (e.g., Langfuse) | **Plugins**     | The plugin system is designed for creating robust, shareable extensions.                         |

In short, **start with hooks for simplicity, and graduate to plugins for power and scalability.**

**Lifecycle Hooks Example:**

You can set the `func_start_hook` and `func_finish_hook` parameters when instantiating a `LangExecutor`.

- `func_start_hook` runs before node execution. It takes a function with one required positional parameter: `node`.
- `func_finish_hook` runs after node execution finishes. It takes a function with one required positional parameter: `node`.

```python
myCustomExecutor = LangExecutor(
    verbose=False,
    func_start_hook=lambda node: 
        print(f"----UI Update---- Starting: `{node.node_desc}`"),
    func_finish_hook=lambda node: 
        print(f"----UI Update---- Finished: `{node.node_desc}` with state `{node.execution_state}`")
)

run_dag(dag, executor=myCustomExecutor)
```

### Node Description

You can add a description to a Node by setting the `node_desc` parameter or the `func_desc` when creating a node.

```python
node_1 = Node(
    node_id="node_1", 
    prompt="...",
    node_desc="THIS IS DESC",
    func_desc=lambda prompt, upstream_output, dag_state: 
        f"THIS IS A DYNAMIC DESC FROM {prompt}",
    func_transform=...
)
```

`node_desc` is a static description, while `func_desc` dynamically creates a description from `prompt`, `upstream_output`, and `dag_state`. If both are set, `node_desc` will be overridden by the value `func_desc` return.

Though optional, node descriptions are beneficial. For example, before executing a node, you may want to send a status message using a node hook function. This message can inform the user about the current action, such as "Getting weather..." or "Getting weather for: New York on 2024-01-01...".

Example:

```python
node_1 = Node(
    node_id="node_get_weather", 
    prompt="Weather for %s on %s is %s",
    func_desc=lambda prompt, upstream_output, dag_state: 
        f"Getting weather for: {upstream_output['node_0']} on {date.today().strftime('%d/%m/%Y')}",
    func_transform=lambda prompt, upstream_output, dag_state: 
        prompt % (
          upstream_output['node_0'], 
          date.today().strftime("%d/%m/%Y"), 
          get_weather(
                      upstream_output['node_0'], 
                      date.today()
                      )
        )
)
```

Setting a hook:

```python
myCustomExecutor = LangExecutor(
    verbose=False,
    func_start_hook=lambda node_id, node_desc: 
        print(f"----FAKE---- UI showing: starting `{node_desc}`"),
    func_finish_hook=lambda node_id, node_desc, execution_state, node_output: 
        print(f"---FAKE----- UI showing: finished `{node_desc}`")
)
```


## 📕 API Reference

### `Node`

The `Node` class represents a single unit of work in a DAG.

**Parameters:**

- **`node_id`** (`Any`): A unique identifier for the node.
- **`node_desc`** (`Any`, optional): A static description for the node.
- **`prompt`** (`Any`, optional): A predefined input for the node.
- **`spec`** (`Dict | Any`, optional): A specification for the node, useful for tool-calling scenarios.
- **`func_desc`** (`Callable`, optional): A function that dynamically generates a description for the node.
- **`func_transform`** (`Callable`, optional): A synchronous or asynchronous function that defines the node's execution logic.
- **`func_set_dag_output_when`** (`Callable`, optional): A function that determines if the node's output should be set as the final output of the DAG.

**Methods:**

- **`reset()`**: Resets the node to its initial state.
- **`get_info()`**: Returns a dictionary of the node's attributes.
- **`add_spec(spec_dict)`**: Adds a specification to the node.
- **`exec_if_any_upstream_acceptable()`**: Configures the node to execute if any of its upstream dependencies are met.

### `LangDAG`

The `LangDAG` class defines the structure of the workflow.

**Parameters:**

- **`dag_input`** (`Any`, optional): An initial input that is accessible to all nodes in the DAG.
- **`dag_id`** (`str`, optional): A unique identifier for the DAG.

**Methods:**

- **`all_starts()`**: Returns a list of all starting nodes.
- **`all_terminals()`**: Returns a list of all terminal nodes.
- **`reset_all_nodes()`**: Resets all nodes in the DAG to their initial state.
- **`inspect_execution()`**: Prints a tree diagram of the execution flow to the console.
- **`snapshot(path)`**: Saves the current state of the DAG to a file.
- **`recover(path)`**: A static method that loads a DAG from a snapshot.
- **`get_node(node_id)`**: Returns the node instance with the given node_id from the DAG. Returns None if no node with the given id is found.

### `LangExecutor`

The `LangExecutor` class handles the execution of synchronous workflows.

**Parameters:**

- **`verbose`** (`bool`, optional): Toggles the display of execution logs.
- **`func_start_hook`** (`Callable`, optional): A function to be executed before a node starts.
- **`func_finish_hook`** (`Callable`, optional): A function to be executed after a node finishes.
- **`plugins`** (`List[Plugin]`, optional): A list of plugin instances to extend functionality.

### `AsyncLangExecutor`

The `AsyncLangExecutor` class handles the execution of asynchronous workflows.

**Parameters:**

- **`verbose`** (`bool`, optional): Toggles the display of execution logs.
- **`func_start_hook`** (`Callable`, optional): A synchronous or asynchronous function to be executed before a node starts.
- **`func_finish_hook`** (`Callable`, optional): A synchronous or asynchronous function to be executed after a node finishes.
- **`plugins`** (`List[Plugin]`, optional): A list of plugin instances to extend functionality.

### `run_dag()`

Executes a DAG.

**Parameters:**

- **`dag`** (`LangDAG`): The DAG to execute.
- **`processor`** (optional): The processor to use for execution (`SequentialProcessor` or `MultiThreadProcessor`).
- **`selector`** (optional): The selector to use for concurrent execution (`FullSelector` or `MaxSelector`).
- **`executor`** (optional): The executor to use for execution (`LangExecutor`).
- **`verbose`** (`bool`, optional): Toggles the display of execution logs.
- **`delay`** (`float`, optional): A delay in seconds to add between node executions.
- **`progressbar`** (`bool`, optional): Toggles the display of a progress bar.
- **`snapshot_on_error_path`** (`str`, optional): The file path to save a snapshot to in case of an error.

### `arun_dag()`

Asynchronously executes a DAG. It accepts the same parameters as `run_dag`, but uses `AsyncLangExecutor` by default.

### `resume_dag()`

Resumes the execution of a recovered DAG. It accepts the same parameters as `run_dag`.

### `default()`

A utility function that retrieves the value from a single-item dictionary.

## Customization with `paradag`

LangDAG is built on top of `paradag`, which allows for advanced customization of processors, selectors, and executors.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue to discuss your ideas.

## License

This project is licensed under the MIT License.
