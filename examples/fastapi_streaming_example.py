# examples/fastapi_streaming_example.py
#
# --- LangDAG Advanced Example: Chained Streaming with FastAPI ---
#
# This script demonstrates how to integrate LangDAG with a FastAPI web server
# to create a powerful, chained, and fully asynchronous streaming endpoint
# where tokens from multiple, sequential nodes are streamed to the client in
# real-time.
#
# --- Key Concepts Illustrated ---
#
# 1.  **Asynchronous DAGs (`arun_dag`)**: The entire workflow is non-blocking,
#     making it ideal for I/O-bound tasks like API calls in a web server context.
#
# 2.  **Chained LLM Calls**: The output of the first LLM call (`node_one`) is
#     used as the direct input for the second LLM call (`node_two`), creating a
#     sequential, dependent workflow.
#
# 3.  **Real-time, Multi-Node Streaming**:
#     -   This example uses an `asyncio.Queue` passed into the DAG's state.
#     -   Each node's transform function can `put` results onto this queue as
#         they are generated.
#     -   **Node 1 (Summarizer)**: It initiates a streaming API call, puts each
#         token onto the shared queue for the client, and *also* accumulates the
#         full response to pass as output to the next node.
#     -   **Node 2 (Expander)**: It takes the complete text from Node 1, starts a
#         second streaming call, and puts each new token onto the same queue.
#
# 4.  **FastAPI StreamingResponse with Background Tasks**: The FastAPI endpoint
#     defines an `async def` generator. This generator starts the `arun_dag`
#     call in a background `asyncio.Task`. It then immediately starts listening
#     to the shared queue, yielding each token it receives to the client. This
#     allows the client to receive tokens from the very first node as soon as
#     they are available.

import os
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn

from langdag import LangDAG, Node, arun_dag
from langdag.utils import default

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

# --- FastAPI App Definition ---
app = FastAPI(
    title="LangDAG FastAPI Streaming Example",
    description="An example of using LangDAG with FastAPI to handle chained, real-time streaming from multiple LLM calls.",
)

class UserRequest(BaseModel):
    prompt: str

# --- LangDAG Node Definitions ---

# Node 1: Takes the initial prompt, streams its response, and passes the full text to the next node.
async def get_initial_summary(prompt: str, upstream_output: dict, dag_state: dict) -> str:
    """
    Makes the first LLM call, streams its output to a queue for the client,
    and also accumulates the full response to pass to the next node.
    """
    print("Node 1: Starting initial summary call...")
    
    # Extract prompt and queue from the DAG's input state
    user_prompt = dag_state['input']['prompt']
    queue = dag_state['input']['queue']

    full_response = ""
    messages = [
        {"role": "system", "content": "You are a helpful assistant. Summarize the user's text in a single, concise sentence."},
        {"role": "user", "content": f"Please summarize this: {user_prompt}"}
    ]
    
    # Create the async stream from the API.
    stream = await client.chat.completions.create(
        model=model_name,
        messages=messages,
        stream=True,
    )
    
    # Consume the stream, putting each chunk into the queue and building the full response.
    async for chunk in stream:
        content = chunk.choices[0].delta.content
        if content:
            full_response += content
            await queue.put(content)
            
    print(f"Node 1: Finished. Full response: '{full_response}'")
    # The final output of this node is the complete string, for the next node.
    return full_response

node_one = Node(
    node_id="summarizer",
    func_transform=get_initial_summary
)

# Node 2: Takes the text from Node 1 and makes a second, final streaming call.
async def expand_on_summary(prompt: str, upstream_output: dict, dag_state: dict) -> None:
    """
    Takes the summary from the previous node, makes a new LLM call, and streams
    its output to the queue for the client.
    """
    # `default(upstream_output)` safely gets the output from the single upstream node.
    summary_text = default(upstream_output)
    queue = dag_state['input']['queue'] # Get the queue from the DAG state
    print(f"Node 2: Starting expansion call with summary: '{summary_text}'")
    
    messages = [
        {"role": "system", "content": "You are a creative writer. Expand the following sentence into a short, interesting paragraph."},
        {"role": "user", "content": summary_text}
    ]
    
    # Create the stream object.
    stream = await client.chat.completions.create(
        model=model_name,
        messages=messages,
        stream=True,
    )
    
    print("Node 2: Streaming response to client via queue.")
    # Stream the response chunk by chunk into the queue.
    async for chunk in stream:
        content = chunk.choices[0].delta.content
        if content:
            await queue.put(content)
    
    # This node is the last one, so it doesn't need to return anything for the DAG.
    return None

node_two = Node(
    node_id="expander",
    func_transform=expand_on_summary
)

# --- FastAPI Endpoint ---

@app.post("/stream_chain")
async def stream_chain_endpoint(request: UserRequest):
    """
    This endpoint takes a user prompt, runs it through a two-step async LangDAG,
    and streams the responses from *both* nodes back to the client in real-time.
    """
    
    async def response_generator():
        """
        An async generator that runs the DAG in a background task while
        simultaneously yielding results from a queue that the DAG nodes
        populate during their execution.
        """
        queue = asyncio.Queue()

        async def run_dag_in_background():
            """Defines and runs the DAG, feeding results into the shared queue."""
            try:
                # The DAG's input is a dictionary containing the prompt and the queue
                with LangDAG(dag_input={"prompt": request.prompt, "queue": queue}) as dag:
                    dag += [node_one, node_two]
                    node_one >> node_two

                    print("\n--- Starting Asynchronous DAG Execution ---")
                    await arun_dag(dag, progressbar=False, verbose=False)
                    print("--- Asynchronous DAG Execution Finished ---\n")
            finally:
                # Signal the end of the stream
                await queue.put(None)

        # Start the DAG execution in a background task
        dag_task = asyncio.create_task(run_dag_in_background())

        # Yield results from the queue as they arrive
        while True:
            item = await queue.get()
            if item is None:  # End of stream signal
                break
            yield item
            await asyncio.sleep(0.01) # Small sleep for cooperative multitasking

        # Await the background task to ensure it completes and to raise any exceptions
        await dag_task

    # Return a StreamingResponse that uses our async generator.
    return StreamingResponse(response_generator(), media_type="text/plain")

# --- Main Execution Block ---

if __name__ == "__main__":
    print("Starting FastAPI server for LangDAG streaming example...")
    print("Navigate to http://127.0.0.1:8000/docs to see the API documentation.")
    # To run this example:
    # 1. Make sure you have an .env file with your OPENAI_API_KEY.
    # 2. Run this script: `python examples/fastapi_streaming_example.py`
    # 3. Send a POST request to http://127.0.0.1:8000/stream_chain with a JSON body like:
    #    {"prompt": "LangDAG is a Python library for building complex LLM workflows."}
    #    You can use a tool like `curl`:
    #    curl -N -X POST "http://127.0.0.1:8000/stream_chain" \
    #         -H "Content-Type: application/json" \
    #         -d '{"prompt": "LangDAG is a Python library for building complex LLM workflows with Directed Acyclic Graphs."}'
    
    uvicorn.run(app, host="127.0.0.1", port=8000)