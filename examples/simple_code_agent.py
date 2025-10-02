# Welcome to the LangDAG Masterclass!
#
# This script showcases an advanced coding agent built with `langdag`, demonstrating
# a powerful hybrid architecture. A high-level DAG orchestrates the main workflow,
# while a specialized node uses an iterative, LLM-driven tool-calling loop to
# handle complex, multi-step tasks with flexibility.
#
# --- Key LangDAG Features Demonstrated ---
#
# 1.  **Hybrid Architecture**: A DAG defines the major stages (intent analysis,
#     execution, response), while a tool-calling loop within a single node
#     provides the flexibility for detailed, dynamic execution.
#
# 2.  **Toolbox for Dynamic Capabilities**: The `Toolbox` class is used to
#     register a set of functions (e.g., file I/O) as tools, which the LLM can
#     then invoke dynamically.
#
# 3.  **Stateful, Iterative Execution**: The `execute_tool_calls` node maintains
#     its own internal conversation state, allowing it to perform a series of
#     tool calls until the user's request is fully resolved.
#
# 4.  **Simplified High-Level DAG**: By encapsulating complexity within the
#     tool-calling node, the main DAG remains clean, readable, and easy to manage.
#
# 5.  **Convergent Edges (`exec_if_any_upstream_acceptable`)**: The `compose_final_response`
#     node elegantly runs regardless of which path (successful tool execution,
#     simple chat) was taken.
#
# Last updated: Sep 20, 2024 by a LangDAG enthusiast.

import os
import json
import re
from typing import List, Dict
from dotenv import load_dotenv
from openai import OpenAI
from rich import print
from rich.panel import Panel

from langdag import LangDAG, run_dag
from langdag.utils import Check
from langdag.decorator import Toolbox, make_node

load_dotenv()

# --- OpenAI Client Setup ---
client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)
model_name = "deepseek/deepseek-chat-v3.1:free"

# --- Toolbox Setup ---
# We create a toolbox to register functions that the LLM can call.
toolbox = Toolbox()

@toolbox.add_tool(spec={
    "type": "function",
    "function": {
        "name": "list_files",
        "description": "List all files and directories in a given directory.",
        "parameters": {
            "type": "object",
            "properties": {
                "directory": {"type": "string", "description": "The directory to list."}
            },
            "required": ["directory"],
        },
    },
})
def list_files(directory="."):
    """Lists files in a directory."""
    try:
        return json.dumps({"files": os.listdir(directory)})
    except Exception as e:
        return json.dumps({"error": str(e)})

@toolbox.add_tool(spec={
    "type": "function",
    "function": {
        "name": "read_file",
        "description": "Read the content of a specified file.",
        "parameters": {
            "type": "object",
            "properties": {
                "filepath": {"type": "string", "description": "The path to the file to read."}
            },
            "required": ["filepath"],
        },
    },
})
def read_file(filepath):
    """Reads content from a file."""
    filepath = filepath[1:] if filepath.startswith('@') else filepath
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except Exception as e:
        return json.dumps({"error": str(e)})

@toolbox.add_tool(spec={
    "type": "function",
    "function": {
        "name": "write_file",
        "description": "Write content to a specified file. This will create the file if it doesn't exist or overwrite it if it does.",
        "parameters": {
            "type": "object",
            "properties": {
                "filepath": {"type": "string", "description": "The path to the file to write to."},
                "content": {"type": "string", "description": "The content to write to the file."}
            },
            "required": ["filepath", "content"],
        },
    },
})
def write_file(filepath, content):
    """Writes content to a file."""
    filepath = filepath[1:] if filepath.startswith('@') else filepath

    try:
        if '/' in filepath:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            f.write(content)
        return f"Successfully wrote to {filepath}"
    except Exception as e:
        return json.dumps({"error": str(e)})

@toolbox.add_tool(spec={
    "type": "function",
    "function": {
        "name": "rename_file",
        "description": "Rename a file or directory.",
        "parameters": {
            "type": "object",
            "properties": {
                "old_filepath": {"type": "string", "description": "The original path of the file/directory."},
                "new_filepath": {"type": "string", "description": "The new path for the file/directory."}
            },
            "required": ["old_filepath", "new_filepath"],
        },
    },
})
def rename_file(old_filepath, new_filepath):
    """Renames a file."""
    old_filepath = old_filepath[1:] if old_filepath.startswith('@') else old_filepath
    new_filepath = new_filepath[1:] if new_filepath.startswith('@') else new_filepath

    try:
        os.rename(old_filepath, new_filepath)
        return f"Successfully renamed {old_filepath} to {new_filepath}"
    except Exception as e:
        return json.dumps({"error": str(e)})


# --- System Prompts ---

INTENT_ANALYSIS_PROMPT = """
You are an intent classification expert. Analyze the user's request and determine if it requires using tools (like writing code, file operations) or if it's a simple 'chat' request (a question, a conversation).
Respond with a single JSON object: `{{"intent": "tool_use"}}` or `{{"intent": "chat"}}`.

User Request: "{user_request}"
"""

TOOL_EXECUTION_SYSTEM_PROMPT = """
You are a senior software engineer. Your task is to fulfill the user's request by calling the available tools.
- Analyze the user's request and any provided file context.
- Decide which tool(s) to call in what order.
- You can call multiple tools in parallel if needed.
- Once you have all the information you need and the task is complete, respond directly to the user with a summary of what you have done.
- If you encounter an error, report it and stop.

User Request: "{user_request}"
{file_context}
"""

RESPONSE_COMPOSITION_PROMPT = """
You are a helpful AI assistant. Compose a friendly, user-facing response based on the internal agent status.
Explain what you did and what the result is. Return the response directly.

Agent Status: {agent_status}
User Request: "{user_request}"
Internal Notes: {internal_notes}
"""

# --- LangDAG Nodes ---

@make_node()
def analyze_intent(prompt, upstream, dag_state):
    """Classifies the user's intent as 'tool_use' or 'chat'."""
    user_request = dag_state["input"]["user_request"]
    print(Panel("Analyzing user intent...", title="[bold blue]Agent Status[/bold blue]", expand=False))
    
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "system", "content": INTENT_ANALYSIS_PROMPT.format(user_request=user_request)}],
        response_format={"type": "json_object"},
    )
    intent = json.loads(response.choices[0].message.content)
    dag_state["intent"] = intent.get("intent", "chat")
    print(f"           [bold]Intent:[/bold] {dag_state['intent']}")
    return dag_state["intent"]

@make_node()
def execute_tool_calls(prompt, upstream, dag_state):
    """Orchestrates the tool-calling loop to fulfill the user's request."""
    print(Panel("Starting tool execution loop...", title="[bold blue]Agent Status[/bold blue]", expand=False))
    
    user_request = dag_state["input"]["user_request"]
    file_context = dag_state["input"].get("file_context", "")
    
    messages = [
        {"role": "system", "content": TOOL_EXECUTION_SYSTEM_PROMPT.format(user_request=user_request, file_context=file_context)},
        {"role": "user", "content": user_request}
    ]
    
    tools = toolbox.get_all_specs()
    
    for i in range(10): # Max 10 iterations to prevent infinite loops
        print(Panel(f"Tool-calling Iteration {i+1}", title="[bold blue]Agent Status[/bold blue]", expand=False))
        
        response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            tools=tools,
            tool_choice="auto"
        )
        
        response_message = response.choices[0].message
        tool_calls = response_message.tool_calls
        
        if not tool_calls:
            dag_state["internal_notes"] = response_message.content
            return response_message.content

        messages.append(response_message)
        
        for tool_call in tool_calls:
            function_name = tool_call.function.name
            function_args = json.loads(tool_call.function.arguments)
            
            print(f"           [bold]Tool Call:[/bold] {function_name}({', '.join(f'{k}={v}' for k, v in function_args.items())})")
            
            function_response = toolbox.call_tool_by_name(function_name, **function_args)
            
            print(f"           [bold]Tool Result:[/bold] {function_response}")
            
            messages.append({
                "tool_call_id": tool_call.id,
                "role": "tool",
                "name": function_name,
                "content": function_response,
            })
            
    final_summary = "Reached max iterations in tool-calling loop."
    dag_state["internal_notes"] = final_summary
    return final_summary


@make_node()
def compose_final_response(prompt, upstream, dag_state):
    """Composes the final, user-facing response based on the agent's journey."""
    print(Panel("Composing final response...", title="[bold blue]Agent Status[/bold blue]", expand=False))
    
    status = "Completed"
    if dag_state.get("intent") == "chat":
        status = "Chatting"

    internal_notes = dag_state.get("internal_notes", "No internal notes.")

    # If the upstream result is from the tool execution, it's already a complete response.
    if dag_state.get("intent") == "tool_use":
        return internal_notes

    # Otherwise, we compose a new one (for chat or errors).
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "system", "content": RESPONSE_COMPOSITION_PROMPT.format(
            agent_status=status,
            user_request=dag_state["input"]["user_request"],
            internal_notes=internal_notes
        )}],
    )
    return response.choices[0].message.content

# --- DAG Definition and Execution ---

def run_agent_turn(user_request: str, history: List[Dict], file_contents: Dict[str, str] = None):
    """Constructs and runs the LangDAG for a single conversational turn."""
    file_context_str = ""
    if file_contents:
        for path, content in file_contents.items():
            file_context_str += f"\n--- Content of {path} ---\n{content}\n--- End of {path}---\n"

    dag_input = {
        "user_request": user_request, 
        "file_context": file_context_str,
    }

    with LangDAG(dag_input) as dag:
        dag += [analyze_intent, execute_tool_calls, compose_final_response]

        analyze_intent >> Check(lambda intent: intent == "tool_use", True) >> execute_tool_calls
        analyze_intent >> Check(lambda intent: intent == "chat", True) >> compose_final_response

        execute_tool_calls >> compose_final_response
        
        compose_final_response.exec_if_any_upstream_acceptable()

        print(Panel(f"User Request: \"{user_request}\"", title="[bold green]New Turn[/bold green]"))
        run_dag(dag)
        return dag.dag_state["output"]

# --- Main Interactive Loop ---

if __name__ == "__main__":
    print(Panel("Advanced LangDAG Coding Agent", subtitle="Type 'exit' to quit."))
    conversation_history = []
    while True:
        user_input = input("\nEnter your request:\n> ")
        if user_input.lower() == 'exit': break
        
        file_references = re.findall(r'@(\S+)', user_input)
        file_contents = {}
        for file_path in file_references:
            try:
                with open(file_path, 'r') as f:
                    file_contents[file_path] = f.read()
                print(Panel(f"Read content from '{file_path}'", title="[bold blue]File Context[/bold blue]", expand=False))
            except FileNotFoundError:
                print(Panel(f"Error: File not found at '{file_path}'", title="[bold red]File Error[/bold red]", expand=False))
                file_contents[file_path] = f"Error: File not found at '{file_path}'"

        conversation_history.append({"role": "user", "content": user_input})
        agent_response = run_agent_turn(user_input, conversation_history, file_contents)
        conversation_history.append({"role": "assistant", "content": agent_response})
        
        print(Panel(agent_response, title="[bold magenta]Agent's Response[/bold magenta]"))

        # Optional: Limit history size
        if len(conversation_history) > 6:
            conversation_history = conversation_history[-6:]
