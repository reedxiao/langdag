# This is an adapted exmaple to do function calling with `langdang` package.
# The original code is from OpenAI
# https://cookbook.openai.com/examples/how_to_call_functions_with_chat_models

# Note:
# Since I do not have an OpenAI payment account, this example is only tested 
# with openai compatible completion api from Alibaba Cloud

# Last updated: Jul 21, 2024 by reedxiao.

from langdag import Node, LangDAG, run_dag
from langdag.processor import MultiThreadProcessor, SequentialProcessor
from langdag.selector import MaxSelector
from langdag.executor import LangExecutor
from langdag.utils import default, Emptyset, NonEmptyset, PretransformSet, Subset, Superset
from langdag.decorator import make_node

from rich import print
from openai import OpenAI
import json

client = OpenAI(
        api_key=  # use_your own key ,  
        # base_url="https://dashscope.aliyuncs.com/compatible-mode/v1", # Pls remove this if not using AliCloud
        )   
model_name = "qwen-turbo"

spec_get_current_weather = {
                                "type": "function",
                                "function": {
                                    "name": "get_current_weather",
                                    "description": "Get the current weather in a given location",
                                    "parameters": {
                                        "type": "object",
                                        "properties": {
                                            "location": {
                                                "type": "string",
                                                "description": "The city and state, ie. San Francisco, CA",
                                            },
                                            "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]},
                                        },
                                        "required": ["location"],
                                    },
                                },
                            }

spec_evaluate_expression = {
    "type": "function",
    "function": {
        "name": "evaluate_expression",
        "description": "Evaluate a simple mathematical expression",
        "parameters": {
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "The mathematical expression to evaluate, ie. '2 + 2 * 2'",
                }
            },
            "required": ["expression"],
        },
    },
}

@make_node()
def llm_resp(prompt, upstream, dag_state):
    messages = dag_state.get("input")
    tools = [x for x in list(dag_state["specs"].values()) if x is not None]
    response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            tools=tools,
        )
    medium_message = response.to_dict()['choices'][0]['message']
    dag_state["medium_message"] = medium_message

    return medium_message


@make_node()
def tools_to_call(prompt, upstream, dag_state):
    dag_state["tool_calls"] = default(upstream)

    tool_calls = default(upstream)['tool_calls']
    return tool_calls


@make_node(spec=spec_get_current_weather, 
           func_desc=lambda prompt, upstream_output, upstream_input: 
           f"Getting weather for: {json.loads([x for x in default(upstream_output) 
                                               if x['function']["name"]=="get_current_weather"][0]['function']['arguments'])["location"]} on today")
def get_current_weather(prompt, upstream, dag_state):
    tool = [x for x in default(upstream) if x['function']["name"]=="get_current_weather"][0]
    args = json.loads(tool['function']['arguments'])
    location = args["location"]
    unit= args.get("unit") or "fahrenheit"

    res = ''
    """Get the current weather in a given location"""
    if "tokyo" in location.lower():
        res= json.dumps({"location": "Tokyo", "temperature": "10", "unit": unit})
    elif "san francisco" in location.lower():
        res= json.dumps({"location": "San Francisco", "temperature": "72", "unit": unit})
    elif "paris" in location.lower():
        res= json.dumps({"location": "Paris", "temperature": "22", "unit": unit})
    else:
        res= json.dumps({"location": location, "temperature": "unknown"})

    return {
                    "tool_call_id": tool['id'],
                    "role": "tool",
                    "name": "get_current_weather",
                    "content": res,
                }


@make_node(spec=spec_evaluate_expression)
def evaluate_expression(prompt, upstream, dag_state):
    args = json.loads(default(upstream)[0]['function']['arguments'])
    expression = args["expression"]

    """Evaluate a simple mathematical expression"""
    try:
        # Use eval to evaluate the mathematical expression
        result = str(eval(expression))
    except Exception as e:
        result = str(e)
    
    return {
                "tool_call_id": default(upstream)[0]['id'],
                "role": "tool",
                "name": "evaluate_expression",
                "content": result,
                }

@make_node()
def llm_resp_given_tool(prompt, upstream, dag_state):
    messages = dag_state.get("input") + [dag_state.get("tool_calls")] + list(upstream.values())
    # print(messages)
    response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            # tools=tools,
        )
    
    res = response.to_dict()['choices'][0]['message']
    # dag_state.update({"new_messages": [res]}) 
    return res


@make_node()
def end_conv(prompt, upstream, dag_state):    
    return dag_state["input"] + [default(upstream)]



# ===============================================


def single_round_ans_with_tool(messages):
    
    with LangDAG(messages) as dag:
        dag += llm_resp
        dag += tools_to_call
        dag += get_current_weather
        dag += evaluate_expression
        dag += llm_resp_given_tool
        dag += end_conv
        
        f1 = lambda resp: resp.get("tool_calls")
        llm_resp >> PretransformSet(f1, Emptyset()) >> end_conv

        llm_resp >> PretransformSet(f1, NonEmptyset()) >> tools_to_call
        # dag.add_edge(llm_resp, tools_to_call)
        
        f2 = lambda tool_calls: [x.get("function").get("name") for x in tool_calls]
        tools_to_call >> PretransformSet(f2, Superset(["get_current_weather"])) >> get_current_weather
        tools_to_call >> PretransformSet(f2, Superset(["evaluate_expression"])) >> evaluate_expression

        get_current_weather >> llm_resp_given_tool
        evaluate_expression >> llm_resp_given_tool

        llm_resp_given_tool >> end_conv

        end_conv.exec_if_any_upstream_acceptable()
        llm_resp_given_tool.exec_if_any_upstream_acceptable()

        def func_start_hook(node_id, node_desc):
            if node_desc:
                print(f"----FAKE---- UI showing: starting `{node_desc}`")
        def func_finish_hook(node_id, node_desc, execution_state, node_output):
            if node_desc:
                print(f"----FAKE---- UI showing: finished `{node_desc}`")

        myCustomExecutor = LangExecutor(
                                        # verbose=False,
                                        func_start_hook=func_start_hook,
                                        func_finish_hook=func_finish_hook
                                    )
        
        run_dag(dag, executor=myCustomExecutor)
        dag.inspect_execution()
        print(dag.dag_state["output"])
        dag.reset_all_nodes()
        return dag.dag_state["output"]


# q = "What is the weather like in SF."
messages = [{"role": "system", 
             "content": "You are a helpful AI assistant. Background system will provide usefull info such as function calls. DO NOT mention any background tool or function calls IN ANY SITUATION."}]
for i in range(10):
    messages.append({"role": "user", "content": input("enter\n")})
    messages = single_round_ans_with_tool(messages)
    