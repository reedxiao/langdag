# Install the Python Requests library:
# `pip install requests`

import requests
import json
import time

systemPrompt =  """
You have access to the following tools:
[
    {
        "name": "get_location",
        "description": "get detail info of a specific location",
        "parameters": {
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "The city and state, ie. San Francisco, CA"
                }
            }
        },
        "required": [
            "location"
        ]
    },
    {
        "name": "get_math_expression_result",
        "description": "get result info of a given math expression",
        "parameters": {
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "The math expression, ie. 1+2*3, sin(a), 2**3, etc., in Python."
                }
            }
        },
        "required": [
            "expression"
        ]
    }
]

Give priority to use a tool for zero or more times, respond with a JSON object with the following structure: 
{
	"tool": <name of the called tool given above>,
	"tool_input": <parameters for the tool matching the above JSON schema>
}
If no matched tool, just respond directly
"""




def get_llm_response_old(prompt):
    # cURL
    # POST https://api.deepseek.com/chat/completions

    try:
        response = requests.post(
            url="https://api.deepseek.com/chat/completions",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer sk-08279429eaf6423d8513e818cf6b3327",
                "Cookie": "HWWAFSESID=af47cfafb51933c3ab; HWWAFSESTIME=1715604220127",
            },
            data=json.dumps({
                "model": "deepseek-chat",
                "stream": False,
                "messages": [
                    {
                        "content": systemPrompt,
                        "role": "system"
                    },
                    {
                        "content": prompt,
                        "role": "user"
                    }
                ]
            })
        )
        print('Response HTTP Status Code: {status_code}'.format(
            status_code=response.status_code))
        json_obj = json.loads(response.content.decode('utf-8'))
        resp = json_obj['choices'][0]['message']['content']

        print('Response HTTP Response Body: {content}'.format(
            content=resp))
        return resp
    except requests.exceptions.RequestException:
        print('HTTP Request failed')






def get_llm_response(prompt):
    # cURL
    # POST https://api.deepseek.com/chat/completions

    try:
        resp = "好美丽"
        time.sleep(2)
        return resp
    except requests.exceptions.RequestException:
        print('HTTP Request failed')


