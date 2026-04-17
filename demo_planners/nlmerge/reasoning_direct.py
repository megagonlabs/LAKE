

from demo_planners.linear_planner.planner import data_infos,additions
from demo_planners.utils import *


# prompt = "Explain the significance of the number 42 in popular culture."



# Stream the response

# user_question=''

def run_planning(user_question, previous_rounds_text=[],tools_list=['JOIN_2', 'SELECT', 'NL2LLM', 'ROWWISE_NL2LLM', 'NL2SQL', 'COUNT']):
    system_prompt="""You are an expert in planning tools adjancement from a task, a list of tools, and available data. 
    Your goal is to create a plan that uses the available tools and data to address the user's query.
    Your response should be a JSON. It should contain the tool to use with their inputs and attributes. Inputs is a list, that can contain one or multiple tools - represented as dictionary, or be empty. 
    Before answering, you should think about how the question can be answered with the available data.
    **Don't take shortcut and query in database unexisting data**
    In your reasoning, think and see if another path can provide a quicker or less expensive way to answer (optimization).


    Data information:
    {data}

    Here is an example of a plan for the main task "{example_task}":
    {example_plan}

    **IT IS VERY IMPORTANT TO PAY ATTENTION TO THE TOOLS' REQUIREMENTS BEFORE USING THEM**
    Tools:
    {available_tools}

    The task is provided in the user prompt.
    """
    # **DO NOT RETURN ANYTHING ELSE THAN A JSON, DO NOT WRAP IT**
    # """
    available_tools=get_tool_description(tools_list, level=['basic', 'linking'], type='themergeone')
    example_task="What jobs are available for data scientists in the bay area?"
    example_plan_l={frozenset(['SELECT', 'ROWWISE_NL2LLM', 'NL2SQL']):"""{
      "tool": "SELECT",
      "inputs": [
        {
          "tool": "ROWWISE_NL2LLM",
          "inputs": [
            {
              "tool": "NL2SQL",
              "inputs": [],
              "attributes": {
                "question": "Select 'unique_job_id', 'job_title', 'location' from 'jobs' where 'job_title' like '%Data Scientist%'",
                "protocol": "postgres",
                "database": "postgres",
                "collection": "public",
                "source": "default"
              }
            }
          ],
          "attributes": {
            "query": "is the location in the bay area?",
            "context": "",
            "attr_names": ["isInBayArea"]
          }
        }
      ],
      "attributes": {
        "operand_key": "isInBayArea",
        "operand": "=",
        "operand_val": true,
        "approximate_match": false,
        "eps": 0.0
      }
    }
    """,frozenset(['SMARTNL2SQL','ROWWISE_NL2LLM']):"""{
      "tool": "SMARTNL2SQL",
      "inputs": [
        {
          "tool": "ROWWISE_NL2LLM",
          "inputs": [
            {
              "tool": "SMARTNL2SQL",
              "inputs": [],
              "attributes": {
                "question": "Select 'unique_job_id', 'job_title', 'location' from 'jobs' where 'job_title' like '%Data Scientist%'",
                'runOn':'database'
              }
            }
          ],
          "attributes": {
            "query": "is the location in the bay area?",
            "context": "",
            "attr_names": ["isInBayArea"]
          }
        }
      ],
      "attributes": {
        "question": "Select elements where 'isInBayArea' is true",
        'runOn':'input'
      }
    }
    """}
    example_plan=''
    for key,val in example_plan_l.items():
      good=True
      for elt in key:
            if not elt in tools_list:
                  good=False
                  break  
      if good:
            example_plan=val
            break
    if example_plan=='':
      raise Exception("No example plan found with the provided tools")
    system_prompt=system_prompt.format(available_tools=available_tools,data=data_infos,example_task=example_task,example_plan=example_plan)
    return get_answer_gpt_advanced(system_prompt, [user_question]+previous_rounds_text)
    

