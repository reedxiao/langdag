# examples/fastapi_concurrent_streaming_example.py
#
# --- LangDAG Advanced Example: Concurrent, Chained Streaming with FastAPI & Langfuse ---
#
# This script builds upon the basic streaming example to demonstrate a more
# complex, concurrent workflow with integrated observability using Langfuse.
#
# --- Key Concepts Illustrated ---
#
# 1.  **Concurrent Node Execution**: The DAG is structured so that two nodes
#     (the "pro" and "con" argument generators) run in parallel.
#
# 2.  **Fan-in Dependency**: A final node (the "synthesizer") depends on the
#     outputs of *both* concurrent nodes, and will only run after both have
#     completed. The DAG structure is: a >> [b, c], [b, c] >> e.
#
# 3.  **Real-time, Multi-Node Concurrent Streaming**:
#     -   An `asyncio.Queue` is shared across all nodes via the DAG's state.
#     -   Node `a` runs first, streaming its output.
#     -   Nodes `b` and `c` start concurrently, and their streamed outputs may
#         be interleaved as they are sent to the client.
#     -   Node `e` runs last, streaming the final, synthesized response.
#
# 4.  **Observability with Langfuse**:
#     -   The `Langfuse` plugin is added to the `LangDAG` context.
#     -   This automatically captures the entire execution trace, including the
#         concurrent structure, inputs, outputs, and timings of each node.
#     -   You can view the trace in your Langfuse project to debug and analyze
#         the workflow.
#
# 5.  **FastAPI Integration**: The setup remains similar to the previous example,
#     using a `StreamingResponse` and a background task to run the DAG, ensuring
#     the server remains non-blocking.

import os
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn

from langdag import LangDAG, Node, arun_dag
from langdag.plugins import LangfusePlugin

# --- Setup ---
load_dotenv()

# Configure the OpenAI client to use OpenRouter
# Make sure you have OPENAI_API_KEY set in your .env file
client = AsyncOpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)
# You can use a free model for this example
model_name = "deepseek/deepseek-chat-v3.1:free" 

# --- Langfuse Plugin Setup ---
# Make sure you have LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY in your .env
# file to enable observability. If not set, the plugin will be disabled.
langfuse_plugin = LangfusePlugin(
    # dag_name="concurrent-streaming-example",
    # user_id="example-user"
)

# --- FastAPI App Definition ---
app = FastAPI(
    title="LangDAG Concurrent Streaming Example",
    description="An example of a complex, concurrent, and observable DAG with real-time streaming in FastAPI.",
)

class UserRequest(BaseModel):
    prompt: str

# --- LangDAG Node Definitions ---

# Node A: Generates a central topic and passes it to the next two nodes.
async def generate_topic(prompt: str, upstream_output: dict, dag_state: dict) -> str:
    """Generates a topic, streams it, and returns the full text."""
    print("Node A (Topic Generator): Starting...")
    queue = dag_state['input']['queue']
    user_prompt = dag_state['input']['prompt']
    
    await queue.put("--- Starting Topic Generation ---\n")
    full_response = ""
    messages = [
        {"role": "system", "content": "You are a topic generator. Based on the user's input, create a single, interesting, and debatable topic statement."},
        {"role": "user", "content": user_prompt}
    ]
    
    stream = await client.chat.completions.create(model=model_name, messages=messages, stream=True)
    
    async for chunk in stream:
        content = chunk.choices[0].delta.content
        if content:
            full_response += content
            await queue.put(content)
    
    await queue.put("\n\n")
    print(f"Node A: Finished. Topic: '{full_response}'")
    return full_response

# Node B: Takes the topic and argues for it.
async def pro_argument(prompt: str, upstream_output: dict, dag_state: dict) -> str:
    """Generates a 'pro' argument for the topic, streams it, and returns the full text."""
    print("Node B (Pro Argument): Starting...")
    topic = upstream_output['topic_generator']
    queue = dag_state['input']['queue']
    
    await queue.put("--- Generating Pro Argument ---\n")
    full_response = ""
    messages = [
        {"role": "system", "content": "You are a debater. Write a strong argument in favor of the following topic."},
        {"role": "user", "content": topic}
    ]
    
    stream = await client.chat.completions.create(model=model_name, messages=messages, stream=True)
    
    async for chunk in stream:
        content = chunk.choices[0].delta.content
        if content:
            full_response += content
            await queue.put(content)

    await queue.put("\n\n")
    print(f"Node B: Finished.")
    return full_response

# Node C: Takes the topic and argues against it.
async def con_argument(prompt: str, upstream_output: dict, dag_state: dict) -> str:
    """Generates a 'con' argument for the topic, streams it, and returns the full text."""
    print("Node C (Con Argument): Starting...")
    topic = upstream_output['topic_generator']
    queue = dag_state['input']['queue']
    
    await queue.put("--- Generating Con Argument ---\n")
    full_response = ""
    messages = [
        {"role": "system", "content": "You are a debater. Write a strong argument against the following topic."},
        {"role": "user", "content": topic}
    ]
    
    stream = await client.chat.completions.create(model=model_name, messages=messages, stream=True)
    
    async for chunk in stream:
        content = chunk.choices[0].delta.content
        if content:
            full_response += content
            await queue.put(content)
            
    await queue.put("\n\n")
    print(f"Node C: Finished.")
    return full_response

# Node E: Takes arguments from both B and C and synthesizes them.
async def synthesize_conclusion(prompt: str, upstream_output: dict, dag_state: dict) -> None:
    """Takes both pro and con arguments and generates a balanced conclusion."""
    print("Node E (Synthesizer): Starting...")
    pro_arg = upstream_output['pro_arg_node']
    con_arg = upstream_output['con_arg_node']
    queue = dag_state['input']['queue']
    
    await queue.put("--- Synthesizing Conclusion ---\n")
    synthesis_prompt = f"Here are two opposing viewpoints on a topic.\n\nArgument For:\n{pro_arg}\n\nArgument Against:\n{con_arg}\n\nPlease provide a balanced, synthesized conclusion that considers both perspectives."
    
    messages = [
        {"role": "system", "content": "You are a thoughtful analyst. Synthesize the provided arguments into a final conclusion."},
        {"role": "user", "content": synthesis_prompt}
    ]
    
    stream = await client.chat.completions.create(model=model_name, messages=messages, stream=True)
    
    async for chunk in stream:
        content = chunk.choices[0].delta.content
        if content:
            await queue.put(content)
    
    print("Node E: Finished.")
    return None

# Create Node instances
node_a = Node(node_id="topic_generator", func_transform=generate_topic)
node_b = Node(node_id="pro_arg_node", func_transform=pro_argument)
node_c = Node(node_id="con_arg_node", func_transform=con_argument)
node_e = Node(node_id="synthesizer", func_transform=synthesize_conclusion)

# --- FastAPI Endpoint ---

@app.post("/stream_concurrent_chain")
async def stream_concurrent_chain_endpoint(request: UserRequest):
    """
    This endpoint runs a four-step async LangDAG with concurrent nodes (B and C)
    and streams the responses from all nodes back to the client in real-time,
    while logging the trace to Langfuse.
    """
    
    async def response_generator():
        queue = asyncio.Queue()

        async def run_dag_in_background():
            try:
                # Use the LangDAG context manager with the Langfuse plugin
                with LangDAG(
                    dag_input={"prompt": request.prompt, "queue": queue},
                    # plugins=[langfuse_plugin]
                ) as dag:
                    # Define the graph structure: a -> (b, c) -> e
                    dag += [node_a, node_b, node_c, node_e]
                    node_a >> node_b
                    node_a >>  node_c
                    node_b >> node_e
                    node_c >> node_e

                    print("\n--- Starting Concurrent Asynchronous DAG Execution ---")
                    await arun_dag(dag, progressbar=False, verbose=False)
                    print("--- Concurrent Asynchronous DAG Execution Finished ---\n")
            finally:
                await queue.put(None) # Signal the end of the stream

        dag_task = asyncio.create_task(run_dag_in_background())

        while True:
            item = await queue.get()
            if item is None:
                break
            yield item
            await asyncio.sleep(0.01)

        await dag_task

    return StreamingResponse(response_generator(), media_type="text/plain")

# --- Main Execution Block ---

if __name__ == "__main__":
    print("Starting FastAPI server for LangDAG concurrent streaming example...")
    print("Navigate to http://127.0.0.1:8000/docs to see the API documentation.")
    # To run this example:
    # 1. Make sure you have an .env file with your OPENAI_API_KEY.
    # 2. For observability, also add LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY.
    # 3. Run this script: `python examples/fastapi_concurrent_streaming_example.py`
    # 4. Send a POST request to http://127.0.0.1:8000/stream_concurrent_chain with a JSON body like:
    #    {"prompt": "The impact of artificial intelligence on creative professions."}
    #    You can use a tool like `curl`:
       # curl -N -X POST "http://127.0.0.1:8000/stream_concurrent_chain" \
       #      -H "Content-Type: application/json" \
       #      -d '{"prompt": "The case for and against universal basic income."}'
    
    uvicorn.run(app, host="127.0.0.1", port=8000)
