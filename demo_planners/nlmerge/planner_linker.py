import copy
import json
import sys
from typing import List, Dict, Any, Callable, Optional

###### Blue
from blue.operators.nl2llm_operator import *
from blue.operators.nl2sql_operator import *
from blue.operators.join_operator import *

from demo_planners.utils import *
from demo_planners.linear_planner.planner import data_infos,additions

initial_prompt_old="""You are an expert in splitting tasks into smaller subtasks to solve a main task. Your goal is to create a plan that uses the available tools and data to address the user's query. The plan should follow a structured process, including reformulating the query, checking data availability, validating tool usage, ensuring tool inputs are correct, and aligning with the user's intent. Each step should be clear, precise, and complete, ensuring tools can operate without further clarification.

**Your response should be a JSON list of dictionaries, each with:**
- TASK: The type of task (e.g., REFORMULATE, DATA CHECK, TOOL CHECK, TOOL INPUT CHECK, USER INTENT ALIGNMENT).
- REASONING: Explain why this step is necessary and how it contributes to the solution. For each task, explicitly answer the following questions (as applicable, based on the example):
  - For REFORMULATE: Why is reformulation needed, and how does it clarify the query?
  - For DATA CHECK: 
    - Do we have the required data in the database?
    - Does it include the specific information needed?
    - Is it in the correct format? **Data in database can be in a different granularity than what is asked in the query, e.g., city vs region. In such cases, note the mismatch and include a step to transform the data.**
    - Is it usable without additional operations?
    - Do we need any other information to provide a comprehensive answer?
  - For TOOL CHECK: 
    - What is the correct tool to use?
    - Why is this tool appropriate for the task?
  - For TOOL INPUT CHECK: 
    - Can the tool operate given what is provided to it?
    - Is it usable without additional operations?
  - TOOL INPUT TO ATTRIBUTES:
    - Convert the textual description to the dictionary format required by the tool for its attributes.
    **DO NOT USE ' INSIDE VALUES FOR ATTRIBUTES DICTIONARY WITHOUT ESCAPING THEM**
  - For USER INTENT ALIGNMENT: 
    - Are we answering the user's original question?
    - Are any additional steps needed to fully address the intent?
    **See USER INTENT ALIGNMENT as a mandatory verification step. Ideally the first user intent alignment should show no issue. If there is an issue, add the necessary steps to fix it as in the example below, and repeat the verification until there is no issue.**
    **CHECK THE ABSENCE OF INSTRUCTIONS IN NATURAL LANGUAGE BETWEEN ## IN THIS STEP. IF THERE IS, PLANNING NEEDS TO CONTINUE**
- OUTPUT: The specific output of this step, such as a reformulated query, a tool call with precise instructions, or a confirmation of user intent alignment.

Until you finished converting the natural language text into a runnable action, leave the description between #, as shown in the example.
TOOLS should be called using TOOL([inputs],attributes). Inputs can be either empty lists [[]] or results from a tool. For instance, EX_TOOL3([EX_TOOL1([[]],{{}}),EX_TOOL2([[]],{{}})],attributes)
**A TOOL PARAMETERS CAN NEVER BE ANYTHING ELSE THAN A LIST AND A DICTIONARY. IT NEEDS TO HAVE BOTH AND NOTHING MORE, NOTHING LESS**

**Use the provided data information and tools to create the plan. If additional data is needed, include a step to acquire it using an appropriate tool.**

Data information:
{data}

Here is an example of a plan for the main task "{example_task}":
{example_plan}

**IT IS VERY IMPORTANT TO PAY ATTENTION TO THE TOOLS' REQUIREMENTS BEFORE USING THEM**
Tools:
{available_tools}

Main task: {task}
{error_mitigation}

**Guidelines:**
- Ensure each step validates data availability and format compatibility with the query.
- Explicitly answer the specified questions in the REASONING field for each task, following the example structure.
- Use tools only when their input requirements are met, explicitly checking in TOOL INPUT CHECK steps.
- Address granularity mismatches (e.g., city vs. region) by including steps to transform data.
- Nest tool calls correctly to pass data between steps.
- Align the final output with the user's intent, confirming in USER INTENT ALIGNMENT steps.
"""

initial_prompt_new="""You are an expert in splitting tasks into smaller subtasks to solve a main task. Your goal is to create a plan that uses the available tools and data to address the user's query. The plan should follow a structured process, starting with checking data availability, then reformulating the query if needed, validating tool usage from outer to inner operations, ensuring tool inputs are correct, and aligning with the user's intent. Each step should be clear, precise, and complete, ensuring tools can operate without further clarification. Ensure no part of the original task is lost during the steps.

**Your response should be a JSON list of dictionaries, each with:**
- TASK: The type of task (e.g., DATA CHECK, REFORMULATE, TOOL CHECK, TOOL INPUT CHECK, USER INTENT ALIGNMENT).
- REASONING: Explain why this step is necessary and how it contributes to the solution. For each task, explicitly answer the following questions (as applicable, based on the example):
  - For DATA CHECK: 
    - Do we have the required data in the database?
    - Does it include the specific information needed?
    - Is it in the correct format? **Data in database can be in a different granularity than what is asked in the query, e.g., city vs region. In such cases, note the mismatch and include a step to transform the data.**
    - Is it usable without additional operations?
    - Do we need any other information to provide a comprehensive answer?
  - For REFORMULATE: Why is reformulation needed, and how does it clarify the query without losing any details?
  - For TOOL CHECK: 
    - What is the correct tool to use?
    - Why is this tool appropriate for the task?
    **Plan tools from outer (high-level) operations to inner (data-fetching) ones to build the nested structure.**
  - For TOOL INPUT CHECK: 
    - Can the tool operate given what is provided to it?
    - Is it usable without additional operations?
  - TOOL INPUT TO ATTRIBUTES:
    - Convert the textual description to the dictionary format required by the tool for its attributes.
    **DO NOT USE ' INSIDE VALUES FOR ATTRIBUTES DICTIONARY WITHOUT ESCAPING THEM**
  - For USER INTENT ALIGNMENT: 
    - Are we answering the user's original question?
    - Are any additional steps needed to fully address the intent?
    **See USER INTENT ALIGNMENT as a mandatory verification step. Ideally the first user intent alignment should show no issue. If there is an issue, add the necessary steps to fix it as in the example below, and repeat the verification until there is no issue.**
    **CHECK THE ABSENCE OF INSTRUCTIONS IN NATURAL LANGUAGE BETWEEN ## IN THIS STEP. IF THERE IS, PLANNING NEEDS TO CONTINUE**
- OUTPUT: The specific output of this step, such as a reformulated query, a tool call with precise instructions, or a confirmation of user intent alignment.

Until you finished converting the natural language text into a runnable action, leave the description between #, as shown in the example.
TOOLS should be called using TOOL([inputs],attributes). Inputs can be either empty lists [[]] or results from a tool. For instance, EX_TOOL3([EX_TOOL1([[]],{{}}),EX_TOOL2([[]],{{}})],attributes)
**A TOOL PARAMETERS CAN NEVER BE ANYTHING ELSE THAN A LIST AND A DICTIONARY. IT NEEDS TO HAVE BOTH AND NOTHING MORE, NOTHING LESS**

**Use the provided data information and tools to create the plan. If additional data is needed, include a step to acquire it using an appropriate tool.**

Data information:
{data}

Here is an example of a plan for the main task "{example_task}":
{example_plan}

**IT IS VERY IMPORTANT TO PAY ATTENTION TO THE TOOLS' REQUIREMENTS BEFORE USING THEM**
Tools:
{available_tools}

Main task: {task}

**Guidelines:**
- Start with DATA CHECK to ground the plan in available resources.
- Ensure each step validates data availability and format compatibility with the query.
- Explicitly answer the specified questions in the REASONING field for each task, following the example structure.
- Use tools only when their input requirements are met, explicitly checking in TOOL INPUT CHECK steps.
- Address granularity mismatches (e.g., city vs. region) by including steps to transform data.
- Nest tool calls correctly to pass data between steps, building from outer to inner.
- Align the final output with the user's intent, confirming in USER INTENT ALIGNMENT steps.
- Preserve all aspects of the original query throughout the plan.

{error_mitigation}
"""

example_old={'data_scientist_jobs':{
    'task': "What jobs are available for data scientists in the bay area?",
    'plan': {frozenset(['SELECT', 'ROWWISE_NL2LLM', 'NL2SQL']):"""[
        {
            "TASK": "REFORMULATE",
            "REASONING": "The original query is a question. Reformulating it into a statement will make it clearer and more direct for processing.",
            "OUTPUT": "#Find available jobs for data scientists in the Bay Area.#"
        },
        {
            "TASK": "DATA CHECK",
            "REASONING": "Do we have a job listing in database? Yes, we do. Does it include information on jobs? Yes, it has a field 'job_title' as shown in data structure. Is it in the correct format? An exact match might be too restrictive here. I will keep this in mind. We should look for jobs like data scientist. Is it usable without additional operations? Yes. Does it include location information in the same table? Yes, it has a field 'location'. Is it in a correct format? No, the granularity is different: it contains city while I want a region. The information should be converted. Is it usable without additional operations? No. Do we have need any other information to provide a comprehensive answer? No, we should have sufficient data to find relevant job openings for data scientists in the Bay Area.",
            "OUTPUT": "#Find available jobs for title like data scientists in find locations that are in the Bay Area#"
        },
        {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We don't have any data yet, and we want to gather it from database. We can use NL2SQL to convert the natural language query into a SQL query. Why is this tool appropriate for the task? NL2SQL can directly query the database to retrieve job titles and their locations based on a natural language input, which aligns with our need to extract job data.",
            "OUTPUT": "NL2SQL([[]],#available jobs for titles like data scientists and their location#) # in find locations that are in the Bay Area#"
        },
        {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - NL2SQL operates on the database directly without input data. We should give it an empty input as input data. Is it usable without additional operations? No, it can use the data scientist title, but the location information is not in the correct format. We need to convert the location information to match the database format. We remove it from the parameters and handle it separately.",
            "OUTPUT": "NL2SQL([[]],#available jobs for titles like data scientists and their location#) # in find locations that are in the Bay Area#"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. We need the attribute question. We make sure to give the correct names as shown in data. We will give it the value ``Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'"''. For the protocol, we use postgres. For database, we use postgres. For collection, we use public. The context can be empty as we don't have additional details to provide.",
            "OUTPUT": "NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})# in find locations that are in the Bay Area#"
        },
        {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We have data from NL2SQL, and we want to transform it to determine which locations are in the Bay Area. We perform a row-wise LLM with ROWWISE_NL2LLM. Why is this tool appropriate for the task? ROWWISE_NL2LLM can evaluate each location row to check if it belongs to the Bay Area, adding a True/False column for filtering.",
            "OUTPUT": "ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})], #Is the location in the Bay Area? Add a column with True/False#)"
        },
        {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - ROWWISE_NL2LLM operates directly on the NL2SQL result, which contains job titles and locations. Is it usable without additional operations? Yes, the NL2SQL output provides the necessary data (job titles and locations) for ROWWISE_NL2LLM to process each row.",
            "OUTPUT": "ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})], #Is the location in the Bay Area? Add a column with True/False#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. We give as query 'is the location in the bay area?' The context can be empty as we don't have additional details to provide. For attr_names, we give the list ['isInBayArea'] as we want to add a column with True/False indicating if the location is in the Bay Area.",
            "OUTPUT": "ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})], {'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})"
        },
        {
            "TASK": "USER INTENT ALIGNMENT",
            "REASONING": "Are we answering the user's original question? We are finding jobs for data scientists and specifying if their locations are in the Bay Area or not. Are any additional steps needed to fully address the intent? Yes, we need to filter the results to only include jobs that are in the Bay Area to fully align with the user's request.",
            "OUTPUT": "ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']}) # and keep the one in bay area#"
        },
        {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We have data from ROWWISE_NL2LLM with a True/False column indicating Bay Area locations, and we want to filter for jobs where the location is in the Bay Area. We perform a SELECT operation. Why is this tool appropriate for the task? SELECT can filter rows based on the True/False column to keep only the relevant jobs.",
            "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#in bay area is True#)"
        },
        {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - SELECT operates directly on the ROWWISE_NL2LLM result, which includes the True/False column for filtering. Is it usable without additional operations? Yes, the data is in the correct format for SELECT to filter based on the condition.",
            "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. For operand_key, we give isInBayArea as it is the column we want to filter on. For operand, we use = as we want to select rows where the value is True. For operand_val, we use True as we want to keep rows where the location is in the Bay Area. approximate_match and eps are not needed for this boolean comparison, so we can leave them as default (False and 0.0 respectively)."
            "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['in bay area']})],{'operand_key':'isInBayArea','operand':'=','operand_val':True,'approximate_match':False,'eps':0.0})"
        },
        {
            "TASK": "USER INTENT ALIGNMENT",
            "REASONING": "Are we answering the user's original question? Yes, we are finding jobs for data scientists and filtering the results to only include those in the Bay Area. Are any additional steps needed to fully address the intent? No, the SELECT operation ensures we only return jobs in the Bay Area, fully addressing the user's intent.",
            "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['in bay area']})],{'operand_key':'isInBayArea','operand':'=','operand_val':True,'approximate_match':False,'eps':0.0})"
        }
    ]    """,frozenset(['SMARTNL2SQL','ROWWISE_NL2LLM']):"""[{
        {
            "TASK": "REFORMULATE",
            "REASONING": "The original query is a question. Reformulating it into a statement will make it clearer and more direct for processing.",
            "OUTPUT": "#Find available jobs for data scientists in the Bay Area.#"
        },
        {
            "TASK": "DATA CHECK",
            "REASONING": "Do we have a job listing in database? Yes, we do. Does it include information on jobs? Yes, it has a field 'job_title' as shown in data structure. Is it in the correct format? An exact match might be too restrictive here. I will keep this in mind. We should look for jobs like data scientist. Is it usable without additional operations? Yes. Does it include location information in the same table? Yes, it has a field 'location'. Is it in a correct format? No, the granularity is different: it contains city while I want a region. The information should be converted. Is it usable without additional operations? No. Do we have need any other information to provide a comprehensive answer? No, we should have sufficient data to find relevant job openings for data scientists in the Bay Area.",
            "OUTPUT": "#Find available jobs for title like data scientists in find locations that are in the Bay Area#"
        },
        {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We don't have any data yet, and we want to gather it from database. We can use SMARTNL2SQL to convert the natural language query into a SQL query. Why is this tool appropriate for the task? SMARTNL2SQL can directly query the database to retrieve job titles and their locations based on a natural language input, which aligns with our need to extract job data.",
            "OUTPUT": "SMARTNL2SQL([[]],#available jobs for titles like data scientists and their location#) # in find locations that are in the Bay Area#"
        },
        {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - SMARTNL2SQL operates on the database directly without input data. We should give it an empty input as input data. Is it usable without additional operations? No, it can use the data scientist title, but the location information is not in the correct format. We need to convert the location information to match the database format. We remove it from the parameters and handle it separately.",
            "OUTPUT": "SMARTNL2SQL([[]],#available jobs for titles like data scientists and their location#) # in find locations that are in the Bay Area#"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. We need the attribute question. We make sure to give the correct names as shown in data. We will give it the value ``Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'"''. The context can be empty as we don't have additional details to provide.",
            "OUTPUT": "SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})# in find locations that are in the Bay Area#"
        },
        {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We have data from SMARTNL2SQL, and we want to transform it to determine which locations are in the Bay Area. We perform a row-wise LLM with ROWWISE_NL2LLM. Why is this tool appropriate for the task? ROWWISE_NL2LLM can evaluate each location row to check if it belongs to the Bay Area, adding a True/False column for filtering.",
            "OUTPUT": "ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})], #Is the location in the Bay Area? Add a column with True/False#)"
        },
        {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - ROWWISE_NL2LLM operates directly on the NL2SQL result, which contains job titles and locations. Is it usable without additional operations? Yes, the SMARTNL2SQL output provides the necessary data (job titles and locations) for ROWWISE_NL2LLM to process each row.",
            "OUTPUT": "ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})], #Is the location in the Bay Area? Add a column with True/False#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. We give as query 'is the location in the bay area?' The context can be empty as we don't have additional details to provide. For attr_names, we give the list ['isInBayArea'] as we want to add a column with True/False indicating if the location is in the Bay Area.",
            "OUTPUT": "ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})], {'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})"
        },
        {
            "TASK": "USER INTENT ALIGNMENT",
            "REASONING": "Are we answering the user's original question? We are finding jobs for data scientists and specifying if their locations are in the Bay Area or not. Are any additional steps needed to fully address the intent? Yes, we need to filter the results to only include jobs that are in the Bay Area to fully align with the user's request.",
            "OUTPUT": "ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']}) # and keep the one in bay area#"
        },
        {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We have data from ROWWISE_NL2LLM with a True/False column indicating Bay Area locations, and we want to filter for jobs where the location is in the Bay Area. We perform a SMARTNL2SQL operation. Why is this tool appropriate for the task? SMARTNL2SQL can filter rows based on the True/False column to keep only the relevant jobs.",
            "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#in bay area is True#)"
        },
        {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - SMARTNL2SQL operates directly on the ROWWISE_NL2LLM result, which includes the True/False column for filtering. Is it usable without additional operations? Yes, the data is in the correct format for SMARTNL2SQL to filter based on the condition.",
            "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. We simply need the attribute question. We give it the value 'Only keep items where \\\\'isInBayArea\\\\' is true'.",
            "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['in bay area']})],{'question':'Only keep items where \\\\'isInBayArea\\\\' is true', 'runOn':'input'})"
        },
        {
            "TASK": "USER INTENT ALIGNMENT",
            "REASONING": "Are we answering the user's original question? Yes, we are finding jobs for data scientists and filtering the results to only include those in the Bay Area. Are any additional steps needed to fully address the intent? No, the SMARTNL2SQL operation ensures we only return jobs in the Bay Area, fully addressing the user's intent.",
            "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['in bay area']})],{'question':'Only keep items where \\\\'isInBayArea\\\\' is true', 'runOn':'input'})"
        }
    ]"""
}}}

example_new = {'data_scientist_jobs':{
    'task': "What jobs are available for data scientists in the bay area?",
    'plan': {frozenset(['SELECT', 'ROWWISE_NL2LLM', 'NL2SQL']):"""[
        {
          "TASK": "DATA CHECK",
          "REASONING": "Before reformulating or planning tools, we need to assess data availability to ensure we can address the query. Do we have the required data in the database? Yes, we have a job listings database. Does it include the specific information needed? Yes, it has fields like 'job_title' for job types and 'location' for where the jobs are based. Is it in the correct format? For job titles, yes, we can match similar to 'data scientist'. For locations, there is a mismatch in granularity: the database uses cities, but the query asks for the 'Bay Area' region, so we note this and will need a transformation step later. Is it usable without additional operations? Partially - job titles can be queried directly, but locations require mapping or evaluation to determine if they fall within the Bay Area. Do we need any other information to provide a comprehensive answer? No, the core data (job IDs, titles, locations) should suffice once filtered appropriately.",
          "OUTPUT": "#Find available jobs for titles like data scientists in locations that are in the Bay Area#"
        },
        {
          "TASK": "REFORMULATE",
          "REASONING": "With data availability confirmed, reformulating the query into a clear statement ensures no part of the original task is lost and makes it more actionable for tool planning. Why is reformulation needed? The original is a question; turning it into a directive statement clarifies the goal without losing details like job type or location. How does it clarify the query? It specifies the need to find and filter jobs precisely, preserving the focus on 'data scientists' and 'Bay Area'.",
          "OUTPUT": "#Find available jobs for data scientists in the Bay Area.#"
        },
        {
          "TASK": "TOOL CHECK",
          "REASONING": "Now that data is checked and query reformulated, we identify the outer/high-level operation first: filtering jobs by title and then by location. What is the correct tool to use? We start with the outermost need - ultimately, we need to select rows where location is in Bay Area after evaluating them, so the outer tool could be SELECT for filtering. Why is this tool appropriate for the task? SELECT can filter based on a condition, but since the condition requires evaluation (is location in Bay Area?), we need inner tools to add that condition first.",
          "OUTPUT": "SELECT(#input_from_inner_tools#,#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL CHECK",
          "REASONING": "Moving inward: before SELECT, we need to add a column for whether location is in Bay Area. What is the correct tool to use? ROWWISE_NL2LLM to evaluate each row's location. Why is this tool appropriate for the task? It can process row-by-row with a natural language query to add a True/False attribute, ensuring we don't lose the location evaluation step.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM(#input_from_inner_tool#,#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL CHECK",
          "REASONING": "Further inward: before ROWWISE_NL2LLM, we need the base data of jobs with titles like data scientist and their locations. What is the correct tool to use? NL2SQL to query the database for relevant jobs. Why is this tool appropriate for the task? It translates natural language to SQL to fetch the initial data without losing the job title filter.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],#available jobs for titles like data scientists and their location#)],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT CHECK",
          "REASONING": "Starting from the innermost tool: for NL2SQL. Can the tool operate given what is provided to it? Yes, it can query the database with a natural language question and empty input data. Is it usable without additional operations? Yes, but we'll specify attributes next to ensure precision.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],#available jobs for titles like data scientists and their location#)],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
          "REASONING": "Convert NL2SQL inputs to dictionary format. We need the attribute 'question' as a SQL-like query: 'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\''. For 'protocol', 'database', 'collection': 'postgres', 'postgres', 'public'. Context is empty.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT CHECK",
          "REASONING": "For ROWWISE_NL2LLM: Can the tool operate given what is provided to it? Yes, it takes the output from NL2SQL as input data. Is it usable without additional operations? Yes, the data will have locations to evaluate.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
          "REASONING": "Convert ROWWISE_NL2LLM inputs to dictionary. 'query': 'is the location in the bay area?', 'context': '', 'attr_names': ['isInBayArea'].",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT CHECK",
          "REASONING": "For SELECT: Can the tool operate given what is provided to it? Yes, it takes the output from ROWWISE_NL2LLM, which includes the new column. Is it usable without additional operations? Yes, we can filter on the True/False column.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
          "REASONING": "Convert SELECT inputs to dictionary. 'operand_key': 'isInBayArea', 'operand': '=', 'operand_val': True, 'approximate_match': False, 'eps': 0.0.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],{'operand_key':'isInBayArea','operand':'=','operand_val':True,'approximate_match':False,'eps':0.0})"
        },
        {
          "TASK": "USER INTENT ALIGNMENT",
          "REASONING": "Are we answering the user's original question? Yes, by querying data scientist jobs, evaluating locations for Bay Area, and filtering accordingly, we provide available jobs in the specified area without losing any part of the query. Are any additional steps needed to fully address the intent? No, this nested structure covers all aspects.",
          "OUTPUT": "SELECT([ROWWISE_NL2LLM([NL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'protocol':'postgres','database':'postgres','collection':'public','source':'postgres_example'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],{'operand_key':'isInBayArea','operand':'=','operand_val':True,'approximate_match':False,'eps':0.0})"
        }
    ]    """,frozenset(['SMARTNL2SQL','ROWWISE_NL2LLM']):"""[{
        {
          "TASK": "DATA CHECK",
          "REASONING": "Before reformulating or planning tools, we need to assess data availability to ensure we can address the query. Do we have the required data in the database? Yes, we have a job listings database. Does it include the specific information needed? Yes, it has fields like 'job_title' for job types and 'location' for where the jobs are based. Is it in the correct format? For job titles, yes, we can match similar to 'data scientist'. For locations, there is a mismatch in granularity: the database uses cities, but the query asks for the 'Bay Area' region, so we note this and will need a transformation step later. Is it usable without additional operations? Partially - job titles can be queried directly, but locations require mapping or evaluation to determine if they fall within the Bay Area. Do we need any other information to provide a comprehensive answer? No, the core data (job IDs, titles, locations) should suffice once filtered appropriately.",
          "OUTPUT": "#Find available jobs for titles like data scientists in locations that are in the Bay Area#"
        },
        {
          "TASK": "REFORMULATE",
          "REASONING": "With data availability confirmed, reformulating the query into a clear statement ensures no part of the original task is lost and makes it more actionable for tool planning. Why is reformulation needed? The original is a question; turning it into a directive statement clarifies the goal without losing details like job type or location. How does it clarify the query? It specifies the need to find and filter jobs precisely, preserving the focus on 'data scientists' and 'Bay Area'.",
          "OUTPUT": "#Find available jobs for data scientists in the Bay Area.#"
        },
        {
          "TASK": "TOOL CHECK",
          "REASONING": "Now that data is checked and query reformulated, we identify the outer/high-level operation first: filtering jobs by title and then by location. What is the correct tool to use? We start with the outermost need - ultimately, we need to select rows where location is in Bay Area after evaluating them, so the outer tool could be SMARTNL2SQL for filtering. Why is this tool appropriate for the task? SMARTNL2SQL can filter based on a condition, but since the condition requires evaluation (is location in Bay Area?), we need inner tools to add that condition first.",
          "OUTPUT": "SMARTNL2SQL(#input_from_inner_tools#,#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL CHECK",
          "REASONING": "Moving inward: before SMARTNL2SQL, we need to add a column for whether location is in Bay Area. What is the correct tool to use? ROWWISE_NL2LLM to evaluate each row's location. Why is this tool appropriate for the task? It can process row-by-row with a natural language query to add a True/False attribute, ensuring we don't lose the location evaluation step.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM(#input_from_inner_tool#,#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL CHECK",
          "REASONING": "Further inward: before ROWWISE_NL2LLM, we need the base data of jobs with titles like data scientist and their locations. What is the correct tool to use? SMARTNL2SQL to query the database for relevant jobs. Why is this tool appropriate for the task? It translates natural language to SQL to fetch the initial data without losing the job title filter.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],#available jobs for titles like data scientists and their location#)],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT CHECK",
          "REASONING": "Starting from the innermost tool: for SMARTNL2SQL. Can the tool operate given what is provided to it? Yes, it can query the database with a natural language question and empty input data. Is it usable without additional operations? Yes, but we'll specify attributes next to ensure precision.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],#available jobs for titles like data scientists and their location#)],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
          "REASONING": "Convert SMARTNL2SQL inputs to dictionary format. We need the attribute 'question' as a SQL-like query: 'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\''. For 'protocol', 'database', 'collection': 'postgres', 'postgres', 'public'. Context is empty.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT CHECK",
          "REASONING": "For ROWWISE_NL2LLM: Can the tool operate given what is provided to it? Yes, it takes the output from SMARTNL2SQL as input data. Is it usable without additional operations? Yes, the data will have locations to evaluate.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],#Is the location in the Bay Area? Add a column with True/False#)],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
          "REASONING": "Convert ROWWISE_NL2LLM inputs to dictionary. 'query': 'is the location in the bay area?', 'context': '', 'attr_names': ['isInBayArea'].",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT CHECK",
          "REASONING": "For SMARTNL2SQL: Can the tool operate given what is provided to it? Yes, it takes the output from ROWWISE_NL2LLM, which includes the new column. Is it usable without additional operations? Yes, we can filter on the True/False column.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],#keep where in bay area is True#)"
        },
        {
          "TASK": "TOOL INPUT TO ATTRIBUTES",
          "REASONING": "Convert SMARTNL2SQL inputs to dictionary. We need to add the question only keep items where \\\\'isInBayArea\\\\' is true. Context is empty.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],{'question':'Only keep items where \\\\'isInBayArea\\\\' is true', 'runOn':'input'})"
        },
        {
          "TASK": "USER INTENT ALIGNMENT",
          "REASONING": "Are we answering the user's original question? Yes, by querying data scientist jobs, evaluating locations for Bay Area, and filtering accordingly, we provide available jobs in the specified area without losing any part of the query. Are any additional steps needed to fully address the intent? No, this nested structure covers all aspects.",
          "OUTPUT": "SMARTNL2SQL([ROWWISE_NL2LLM([SMARTNL2SQL([[]],{'question':'Select \\\\'unique_job_id\\\\', \\\\'job_title\\\\', \\\\'location\\\\' from \\\\'jobs\\\\' where \\\\'job_title\\\\' like \\\\'%Data Scientist%\\\\'', 'runOn':'database'})],{'query':'is the location in the bay area?','context':'','attr_names':['isInBayArea']})],{'question':'Only keep items where \\\\'isInBayArea\\\\' is true', 'runOn':'input'})"
        }
      ]"""
}}}

# print(example_new['data_scientist_jobs']['plan'])
def get_plan(task, error_mitigation='', special_task={}, method='old',tools_list=['JOIN_2', 'SELECT', 'NL2LLM', 'ROWWISE_NL2LLM', 'NL2SQL', 'COUNT']):
    addition = ''
    if 'addition' in special_task.keys():
        addition = additions[special_task['addition']]
    
    if method == 'old':
        initial_prompt = initial_prompt_old
        example_task = example_old['data_scientist_jobs']['task']
        # example_plan = example_old['data_scientist_jobs']['plan']
        example_plan=''
        for key,val in example_old['data_scientist_jobs']['plan'].items():
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
    elif method == 'new':
        initial_prompt = initial_prompt_new
        example_task = example_new['data_scientist_jobs']['task']
        # example_plan = example_new['data_scientist_jobs']['plan']
        example_plan=''
        for key,val in example_new['data_scientist_jobs']['plan'].items():
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
    else:
        raise ValueError("Invalid method. Choose 'old' or 'new'.")
    
    prompt = initial_prompt.format(
        data=data_infos,
        addition=addition,
        example_task=example_task,
        example_plan=example_plan,
        available_tools=get_tool_description(tools_list, level=['basic', 'linking'], type='themergeone'),
        task=task,
        error_mitigation=error_mitigation
    )
    # logging.critical("$$$$$$$$$$$$$$Planner Linker Prompt:\n" + prompt)
    return standard_NL2LLM_agent(prompt, ["TASK", "REASONING", "OUTPUT"])

def get_plan_text(plan):
    return '\n'.join([f"{elt['TASK']}({elt['OUTPUT']})" for elt in plan[0]])

if __name__ == "__main__":
    # Example usage with old method
    plan_old = get_plan("Are jobs requiring a study field different from the company’s main purpose (e.g., a job in a tech company requiring a non-tech study field) typically paid more or less than jobs from companies in the same field?", method='old')
    print("Old Method Plan:")
    print(json.dumps(plan_old, indent=2))
    
    # Example usage with new method
    plan_new = get_plan("Are jobs requiring a study field different from the company’s main purpose (e.g., a job in a tech company requiring a non-tech study field) typically paid more or less than jobs from companies in the same field?", method='new')
    print("\nNew Method Plan:")
    print(json.dumps(plan_new, indent=2))