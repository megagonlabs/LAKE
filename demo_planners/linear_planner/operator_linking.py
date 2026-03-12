###### Formats
import copy
import json
import sys
from typing import List, Dict, Any, Callable, Optional
import logging
###### Blue
from blue.operators.nl2llm_operator import *
from blue.operators.nl2sql_operator import *
from blue.operators.join_operator import *
from demo_planners.utils import *
from demo_planners.linear_planner.error_tackling import *
import threading


# input_data = [[]]
# attributes = {
#     # "question": "what are the 10 best candidates with fluency in python and java?",
#     # "question": "Count the number of job postings that don't have a maximum salary specified.",  #-->5690
#     #"question": "Give me 5 job postings that have a maximum salary specified.",  #-->5690
#     "question": "Find all unique job title.",  #--> long list
    
#     # "question": "Count the number of different companies in job postings.", #-->5195
#     #"question": "Give me 5 different companies in job posting.",
#     # "question": "what is the most frequently advertised manager role in jurong?",
#     # # "question": "what are the top 10 project manager jobs in jurong with a minimum salary of 4000?",
#     "protocol": "postgres",
#     "database": "postgres",
#     "collection": "public",
#     "case_insensitive": True,
#     "additional_requirements": "",
#     "context": "",#This is a job database with information about job postings, skills, companies, and salaries",
#     # schema will be fetched automatically if not provided
# }

# print(f"=== NL2SQL attributes ===")
# print(attributes)

# # just used to get the default properties
# nl2sql_operator = NL2SQLOperator()
# properties = nl2sql_operator.properties
# print(f"=== NL2SQL PROPERTIES ===")
# properties['service_url'] = 'ws://localhost:8001'  # update this to your service url
# print(properties)

# # call the function
# # Option 1: directly call the nl2sql_operator_function
# result = nl2sql_operator_function(input_data, attributes, properties)
# print("=== NL2SQL RESULT (Option 1)===")
# print(result)
# Option 2: use the function method
# result = nl2sql_operator.function(input_data, attributes, properties)
# print("=== NL2SQL RESULT (Option 2)===")
# print(result)





# ## calling example

# # Test data - natural language query
# input_data = [[]]  # empty input data for query type data operator
# attributes = {
#     "query": "What are the top 5 programming languages in 2024?",
#     "context": "Focus on popularity and job market demand",
#     # "attr_names": ["language", "popularity_rank", "description"],
#     "attr_names": ["language", "year"],
# }
# print(f"=== NL2LLM attributes ===")
# print(attributes)

# # just used to get the default properties
# nl2llm_operator = NL2LLMOperator()
# properties = nl2llm_operator.properties
# print(f"=== NL2LLM PROPERTIES ===")
# print(properties)
# properties['service_url'] = 'ws://localhost:8001'  # update this to your service url

# # call the function
# # Option 1: directly call the nl2llm_operator_function
# result = nl2llm_operator_function(input_data, attributes, properties)
# print("=== NL2LLM RESULT (Option 1)===")
# print(result)
# # Option 2: use the function method
# attributes['attr_names'] = ["language", "popularity_rank", "description", "latest_release_date"]
# result = nl2llm_operator.function(input_data, attributes, properties)
# print("=== NL2LLM RESULT (Option 2)===")
# print(result)





#Can you infer the max salary for the job posts missing it, based on the other job posts?
#(Maybe we can decide to have a DeepThink operator that decides if a certain query requires more thinking)
#One plan could be to find the range foe every job title of salary, to rank them
#Then we need again the iterate operator, to go through every ...
##==> maybe I should start with something simpler....





#WOULDN T WE NEED A WHILE OPERATOR FOR THAT?
# Or an iterate operator, that gives every row individually to some agent and take the updated version to update the overall input
#Other option is to have a nl2llm that operates rowwise, but then what about other agents that need to do the same thing?







#Among job post for people who should have proficiency in at least one programming language, what is the maximum salary to be expected?
#First step is to build a mapping of job title to programming languages
#NL2SQL(Find all unique job title.)"question": "Find all unique job title."





# input_data = [
#     [{"job_id": 1, "name": "name A", "salary": 100000}, {"job_id": 2, "name": "name B", "location": "state B", "salary": 200000}],
#     [{"job_id": 2, "location": "city B"}, {"job_id": 3, "location": "city C"}],
#     [{"id": 1, "title": "title A"}, {"id": 4, "title": "title D"}, {"id": 2, "title": "title B"}],
# ]

# # #### using tool class
# # attributes = {"join_on": [["job_id"], ["job_id"], ["id"]], "join_type": "inner", "join_suffix": ["_employee", "_geometry", "_job_content"], "keep_keys": "both"}
# # join_operator = JoinOperator()
# # result = join_operator.execute(input_data, attributes)  # this assume the Tool class define the execute method

# #### using function directly

# ## test keep_keys = "left"
# attributes = {"join_on": [["job_id"], ["job_id"], ["id"]], "join_type": "inner", "join_suffix": ["_employee", "_geometry", "_job_content"], "keep_keys": "left"}
# result = join_operator_function(input_data, attributes)
# print("=== JOIN RESULT ===")
# print(result)

# ## test keep_keys = "both"
# attributes = {"join_on": [["job_id"], ["job_id"], ["id"]], "join_type": "inner", "join_suffix": ["_employee", "_geometry", "_job_content"], "keep_keys": "both"}
# result = join_operator_function(input_data, attributes)
# print("=== JOIN RESULT ===")
# print(result)
# 0/0









# print(standard_NL2LLM_agent("Give number up to 5 and their multiplication by 2.", ['number', 'multiplied_by_2']))

# print(standard_NL2LLM_agent("""A planner gave the following plan to solve the following task:
#                             Find the maximum salary among people who should have proficiency in at least one programming language.
#                             The tasks are as follows:
#                             0. START
#                             1. NL2SQL("Find all unique job title in postgres database and public collection.")
#                             2. ROWWISE_NL2LLM("This job title requires proficiency in any programming languages? True or False?")
#                             3. NL2SQL("Find all job postings.")  
#                             4. Join("Join the job postings with the job titles that require proficiency in programming languages.")
#                             5. SELECT("Keep only the job postings with proficiency in programming languages.")
#                             6. NL2SQL("Find the maximum salary among these job postings.")

#                             Your task is to provide the logic to connect the different steps of the plan.
#                             We are currently doing step 1->2, connecting NL2SQL with ROWWISE_NL2LLM.
#                             Here are the definitions of the operators, and the expected input, attributes and output formats:
#                             NL2SQL 
#                             Convert a natural language question into a SQL query, and return the results.
#                             input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
#                             output:  [ [ {<key>:<value>, .. }, {...} ] ]
#                             attributes:
#                             -question: what we want to obtain from the query
#                             -protocol: one of ["postgres", "mysql"]
#                             -database
#                             -collection
#                             -context : can be empty, used to provide additionnal details

#                             ROWWISE_NL2LLM 
#                             Given a natural language query, fetch open-domain knowledge from LLM.

#                             input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
#                             output:  [ [ {<key>:<value>, .. }, {...} ] ]
#                             attributes:
#                             -query:what we want to obtain from the query
#                             -context: can be empty, used to provide additionnal details
#                             -attr_names: a list of string, each string being keys you want in the output

#                             If the tasks is not doable because of a missing detail in the planner plan, start your answer with ABORT with a description of the issue.
#                             Give the connection from NL2SQL to ROWWISE_NL2LLM : we want to connect each element from the input of ROWWISE_NL2LLM to an element from the output of NL2SQL or a hardcoded string.
#                             Give in every dict for key 'INPUT_SOURCE' the hardcoded string or the output of the first module, and for key 'INPUT_KEY'  the input of the second module.
#                             For dict and nested properties, link them with '->' (e.g. 'attributes->query').
#                             The INPUT_SOURCE be replaced by a hardcoded string if it is the right thing to do - distinguish them with # surrounding them.

#                             Here is a toy example of the expected output format on a different task:
#                             In this task, the plan is the following:
#                             1. MULTIPLY("Multiply input numbers by 2.")
#                             2. MULTIPLY("Multiply input numbers by 3.")
#                             The description of MULTIPLY is the following:
#                             MULTIPLY
#                             Multiply each input number by a given factor.
#                             input: [ [ {<key>:<value>, .. }, {...} ],  [...]
#                             output:  [ [ {<key>:<value>, .. }, {...} ] ]
#                             attributes:
#                             -factor: the factor to multiply by
#                             The connection between the two modules is the following:
#                             [
#                                 {
#                                     "INPUT_SOURCE": "output",
#                                     "INPUT_KEY": "input"
#                                 },
#                                 {
#                                     "INPUT_SOURCE": "#3#",
#                                     "INPUT_KEY": "factor"
#                                 }
#                             ]
#                             There cannot be any duplicate of INPUT_KEY.
#                             """, [ "INPUT_KEY","INPUT_SOURCE"]))


# print(standard_NL2LLM_agent("""A planner has generated the following plan to solve the task:
# Find the maximum salary among people who should have proficiency in at least one programming language.

# The steps of the plan are as follows:
# 0. START("")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The task requires finding all unique job titles.', 'INPUT_SOURCE': '#Find all unique job title in postgres database and public collection.#', 'INPUT_KEY': 'attributes->question'}, {'INPUT_JUSTIFICATION': 'The task specifies using the postgres protocol.', 'INPUT_SOURCE': '#postgres#', 'INPUT_KEY': 'attributes->protocol'}, {'INPUT_JUSTIFICATION': 'The task specifies using the postgres database.', 'INPUT_SOURCE': '#postgres#', 'INPUT_KEY': 'attributes->database'}, {'INPUT_JUSTIFICATION': 'The task specifies using the public collection.', 'INPUT_SOURCE': '#public#', 'INPUT_KEY': 'attributes->collection'}, {'INPUT_JUSTIFICATION': 'The task does not provide additional context.', 'INPUT_SOURCE': '# #', 'INPUT_KEY': 'attributes->context'}, {'INPUT_JUSTIFICATION': 'This is the first step, so the input is empty.', 'INPUT_SOURCE': '[[]]', 'INPUT_KEY': 'input'}]]

# 1. NL2SQL("Find all unique job title in postgres database and public collection.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the NL2SQL step provides the job titles to be checked for programming language proficiency.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The query is to determine if the job title requires proficiency in any programming languages.', 'INPUT_SOURCE': '#This job title requires proficiency in any programming languages? True or False?#', 'INPUT_KEY': 'attributes->query'}, {'INPUT_JUSTIFICATION': 'No additional context is provided for this step.', 'INPUT_SOURCE': '# #', 'INPUT_KEY': 'attributes->context'}, {'INPUT_JUSTIFICATION': 'The task requires the output to include whether the job title requires programming language proficiency.', 'INPUT_SOURCE': "#['job_title', 'requires_proficiency']#", 'INPUT_KEY': 'attributes->attr_names'}]]

# 2. ROWWISE_NL2LLM("This job title requires proficiency in any programming languages? True or False?")
#  Your answer for connexion at this stage:[[{'INPUT_KEY': 'input', 'INPUT_SOURCE': '[[]]'}, {'INPUT_KEY': 'attributes->question', 'INPUT_SOURCE': '#Find all job postings.#'}, {'INPUT_KEY': 'attributes->protocol', 'INPUT_SOURCE': '#postgres#'}, {'INPUT_KEY': 'attributes->database', 'INPUT_SOURCE': '#postgres#'}, {'INPUT_KEY': 'attributes->collection', 'INPUT_SOURCE': '#public#'}, {'INPUT_KEY': 'attributes->context', 'INPUT_SOURCE': '# #'}]]

# 3. NL2SQL("Find all job postings.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the NL2SQL step provides the job postings to be appended.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The task requires appending job titles that require proficiency in programming languages.', 'INPUT_SOURCE': '$STEP2$->output', 'INPUT_KEY': 'attributes->new_element'}]]

# 4. APPEND("Append the job postings to the job titles that require proficiency in programming languages.")
# 5. JOIN("Join the job postings with the job titles that require proficiency in programming languages.")
# 6. SELECT("Keep only the job postings with proficiency in programming languages.")
# 7. NL2LLM("Find the column that corresponds to the maximum salary.")
# 8. SELECT("Find the maximum salary among these job postings.")

# Your job is to define the logic that connects the different steps in the plan.

# We are currently transitioning from step 4 to step 5, specifically connecting:
# - Step 4: APPEND
# - Step 5: JOIN

# Below are the descriptions of the relevant operators, including their expected input, attributes, and output formats:
# APPEND
# Given an input data, consisting of a list of data elements, append a new data element to the input data.
# input: [ [ {<key>:<value>, .. }, {...} ] ]
# output:  [ [ {<key>:<value>, .. }, {...} ]  ]
# attributes:
# new_element: the element to append to the input data, in the same format as the input data elements.


# JOIN 
# Given an input data,  comprising two or more lists of data, return n-way join 

# input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
# output:  [ [ {<key>:<value>, .. }, {...} ] ]
# attributes:
# -join_on: **it must be list of list of string**, each of the inner list contains the column name we want to perform the join on for each current input. If the join is performed on columns with the same name, the column name should be repeated in each list of list.
# -join_type: type str, type of join in 'inner', 'left', 'right', 'outer'
# -join_suffix: type list of str, each element (str) refers to the additional suffix to add to a data source/group field names to avoid conflicts. The default suffix is ‘_ds{i}’ for data source/group i in the input data.
# -keep_keys: type str, one value in ‘left’, ‘both’. 
# --‘left’: keep only the join index of the first data group in the input data
# --‘both’: keep all join indices with suffixes.

# Instructions:
# - If the task cannot be completed due to missing or incomplete information in the plan, begin your response with **ABORT** followed by a clear explanation of the issue.
# - Otherwise, provide the connection logic from **APPEND** to **JOIN**.
# - Give for:
#   - `"INPUT_JUSTIFICATION"`: a brief explanation/justification of why you use such source for such key.
#   - `"INPUT_SOURCE"`: the output of the previous step or a hardcoded string (surrounded with `#`, e.g. `"#value#"`).
#   - `"INPUT_KEY"`: the input field of the next step.
# - If the input/output field is nested (e.g., within a dictionary), represent it using `->` notation (e.g., `"attributes->query"`).
# - Each `INPUT_KEY` should appear only once in your response.

# Example:
# For a simple task with the following plan:
# 1. MULTIPLY ("Multiply input numbers by 2.")
# 2. MULTIPLY ("Multiply input numbers by 3.")

# Operator description for MULTIPLY:
# MULTIPLY
# Multiply each input number by a given factor.
# input: [ [ {<key>:<value>, .. }, {...} ],  [...]
# output:  [ [ {<key>:<value>, .. }, {...} ] ]
# attributes:
# -factor: the factor to multiply by

# The connection would look like:
# [
#     {
#         "INPUT_JUSTIFICATION": "The output from last step is the one to be multiplied.",
#         "INPUT_SOURCE": "output",
#         "INPUT_KEY": "input"
#     },
#     {
#         "INPUT_JUSTIFICATION": "The task requires a factor of 3.",
#         "INPUT_SOURCE": "#3#",
#         "INPUT_KEY": "factor"
#     }
# ]

# Now, provide the connection between **APPEND** and **JOIN**. Keep in mind that the current task is : JOIN("Join the job postings with the job titles that require proficiency in programming languages.").
# Each `INPUT_KEY` should appear only once in your entire response.
# You MUST provide an INPUT_SOURCE for every attributes in **JOIN**, as well as for the input. The input can be empty (INPUT_SOURCE:'[[]]')
# In Special cases, you can need to gather information from an older step. In that case, add '$STEPi$->' with i the index of the step you want to refer to, at the beginning of INPUT_SOURCE.
# """, [ "INPUT_KEY","INPUT_SOURCE"]))


# print(standard_NL2LLM_agent("""A planner has generated the following plan to solve the task:
# Find the maximum salary among people who should have proficiency in at least one programming language.

# The steps of the plan are as follows:
# 0. START("")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The task requires finding all unique job titles.', 'INPUT_SOURCE': '#Find all unique job title in postgres database and public collection.#', 'INPUT_KEY': 'attributes->question'}, {'INPUT_JUSTIFICATION': 'The task specifies using the postgres protocol.', 'INPUT_SOURCE': '#postgres#', 'INPUT_KEY': 'attributes->protocol'}, {'INPUT_JUSTIFICATION': 'The task specifies using the postgres database.', 'INPUT_SOURCE': '#postgres#', 'INPUT_KEY': 'attributes->database'}, {'INPUT_JUSTIFICATION': 'The task specifies using the public collection.', 'INPUT_SOURCE': '#public#', 'INPUT_KEY': 'attributes->collection'}, {'INPUT_JUSTIFICATION': 'The task does not provide additional context.', 'INPUT_SOURCE': '# #', 'INPUT_KEY': 'attributes->context'}, {'INPUT_JUSTIFICATION': 'This is the first step, so the input is empty.', 'INPUT_SOURCE': '[[]]', 'INPUT_KEY': 'input'}]]

# 1. NL2SQL("Find all unique job title in postgres database and public collection.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the NL2SQL step provides the job titles to be checked for programming language proficiency.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The query is to determine if the job title requires proficiency in any programming languages.', 'INPUT_SOURCE': '#This job title requires proficiency in any programming languages? True or False?#', 'INPUT_KEY': 'attributes->query'}, {'INPUT_JUSTIFICATION': 'No additional context is provided for this step.', 'INPUT_SOURCE': '# #', 'INPUT_KEY': 'attributes->context'}, {'INPUT_JUSTIFICATION': 'The task requires the output to include whether the job title requires programming language proficiency.', 'INPUT_SOURCE': "#['job_title', 'requires_proficiency']#", 'INPUT_KEY': 'attributes->attr_names'}]]

# 2. ROWWISE_NL2LLM("This job title requires proficiency in any programming languages? True or False?")
#  Your answer for connexion at this stage:[[{'INPUT_KEY': 'input', 'INPUT_SOURCE': '[[]]'}, {'INPUT_KEY': 'attributes->question', 'INPUT_SOURCE': '#Find all job postings.#'}, {'INPUT_KEY': 'attributes->protocol', 'INPUT_SOURCE': '#postgres#'}, {'INPUT_KEY': 'attributes->database', 'INPUT_SOURCE': '#postgres#'}, {'INPUT_KEY': 'attributes->collection', 'INPUT_SOURCE': '#public#'}, {'INPUT_KEY': 'attributes->context', 'INPUT_SOURCE': '# #'}]]

# 3. NL2SQL("Find all job postings.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the NL2SQL step provides the job postings to be appended.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The task requires appending job titles that require proficiency in programming languages.', 'INPUT_SOURCE': '$STEP2$->output', 'INPUT_KEY': 'attributes->new_element'}]]

# 4. APPEND("Append the job postings to the job titles that require proficiency in programming languages.")
#  Your answer for connexion at this stage:[[{'INPUT_KEY': 'input', 'INPUT_SOURCE': 'output'}, {'INPUT_KEY': 'attributes->join_on', 'INPUT_SOURCE': "#['job_title']#"}, {'INPUT_KEY': 'attributes->join_type', 'INPUT_SOURCE': '#inner#'}, {'INPUT_KEY': 'attributes->join_suffix', 'INPUT_SOURCE': "#['_ds1', '_ds2']#"}, {'INPUT_KEY': 'attributes->keep_keys', 'INPUT_SOURCE': '#both#'}]]

# 5. JOIN("Join the job postings with the job titles that require proficiency in programming languages.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the JOIN step provides the data to be filtered for job postings with proficiency in programming languages.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The task requires filtering job postings based on programming language proficiency.', 'INPUT_SOURCE': '#requires_proficiency#', 'INPUT_KEY': 'attributes->operand_key'}, {'INPUT_JUSTIFICATION': 'The task requires selecting job postings where proficiency is required.', 'INPUT_SOURCE': '#=#', 'INPUT_KEY': 'attributes->operand'}, {'INPUT_JUSTIFICATION': 'The task requires selecting job postings where proficiency is required.', 'INPUT_SOURCE': '#True#', 'INPUT_KEY': 'attributes->operand_val'}]]

# 6. SELECT("Keep only the job postings with proficiency in programming languages.")
# 7. NL2LLM("Find the column that corresponds to the maximum salary.")
# 8. SELECT("Find the maximum salary among these job postings.")

# Your job is to define the logic that connects the different steps in the plan.

# We are currently transitioning from step 6 to step 7, specifically connecting:
# - Step 6: SELECT
# - Step 7: NL2LLM

# Below are the descriptions of the relevant operators, including their expected input, attributes, and output formats:
# SELECT 
# Given an input data, consisting of a list of data elements,  filter data elements based on a specified condition (record-wise).

# input: [ [ {<key>:<value>, .. }, {...} ] ]
# output:  [ [ {<key>:<value>, .. }, {...} ]  ]
# attributes:
# operand_key: type str, the key to filter the data records
# operand: type str, comparison operator: =, !=, >, >=, <, <=, max, min, in, not in, like, not like
# operand_val: type Any, value to compare with (not needed for max, min)
# approximate_match: type bool, whether to use epsilon tolerance for numeric comparison.
# eps: type float, epsilon tolerance for numeric comparison

# NL2LLM 
# Given a natural language query, fetch open-domain knowledge from LLM.

# input: [ [ {<key>:<value>, .. }, {...} ] ] (**leave empty**, if needed, provide elements in the context)
# output:  [ [ {<key>:<value>, .. }, {...} ] ]
# attributes:
# -query:what we want to obtain from the query
# -context: provide here any information that is needed to answer the query
# -attr_names: a list of string, each string being keys you want in the output (must not be empty)

# Instructions:
# - If the task cannot be completed due to missing or incomplete information in the plan, begin your response with **ABORT** followed by a clear explanation of the issue.
# - Otherwise, provide the connection logic from **SELECT** to **NL2LLM**.
# - Give for:
#   - `"INPUT_JUSTIFICATION"`: a brief explanation/justification of why you use such source for such key.
#   - `"INPUT_SOURCE"`: the output of the previous step or a hardcoded string (surrounded with `#`, e.g. `"#value#"`).
#   - `"INPUT_KEY"`: the input field of the next step.
# - If the input/output field is nested (e.g., within a dictionary), represent it using `->` notation (e.g., `"attributes->query"`).
# - Each `INPUT_KEY` should appear only once in your response.

# Example:
# For a simple task with the following plan:
# 1. MULTIPLY ("Multiply input numbers by 2.")
# 2. MULTIPLY ("Multiply input numbers by 3.")

# Operator description for MULTIPLY:
# MULTIPLY
# Multiply each input number by a given factor.
# input: [ [ {<key>:<value>, .. }, {...} ],  [...]
# output:  [ [ {<key>:<value>, .. }, {...} ] ]
# attributes:
# -factor: the factor to multiply by

# The connection would look like:
# [
#     {
#         "INPUT_JUSTIFICATION": "The output from last step is the one to be multiplied.",
#         "INPUT_SOURCE": "output",
#         "INPUT_KEY": "input"
#     },
#     {
#         "INPUT_JUSTIFICATION": "The task requires a factor of 3.",
#         "INPUT_SOURCE": "#3#",
#         "INPUT_KEY": "factor"
#     }
# ]

# Now, provide the connection between **SELECT** and **NL2LLM**. Keep in mind that the current task is : NL2LLM("Find the column that corresponds to the maximum salary.").
# Each `INPUT_KEY` should appear only once in your entire response.
# You MUST provide an INPUT_SOURCE for every attributes in **NL2LLM**, as well as for the input. The input can be empty (INPUT_SOURCE:'[[]]')
# In Special cases, you can need to gather information from an older step. In that case, add '$STEPi$->' with i the index of the step you want to refer to, at the beginning of INPUT_SOURCE.
# """, [ "INPUT_KEY","INPUT_SOURCE"]))



def prompt_correction(prompt: str,type_corr:str, current_output:str) -> str:
    str_base=''
    if type_corr == "OUTPUT_STEP":
        str_base="For the following prompt: '''"
        str_base+= prompt
        str_base+="'''\nYou provided the output:"
        str_base+=current_output
        str_base+="Include in each source the step number it comes from, using the format $STEPi$->output, where i is the step number. If the source is a hardcoded string, use the format #value#.\nGive an answer in the same format."
    return str_base

def run_prompt_correction(prompt: str, type_corr: str, current_output: str) -> str:
    attributes_output = ["INPUT_KEY", "INPUT_SOURCE"]
    return standard_NL2LLM_agent(prompt_correction(prompt, type_corr, current_output), attributes_output)


# print(standard_NL2LLM_agent("""For the following prompt: '''
# A planner has generated the following plan to solve the task:
# Find the maximum salary among people who should have proficiency in at least one programming language.

# The steps of the plan are as follows:
# 0. START("")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The task requires finding all unique job titles.', 'INPUT_SOURCE': '#Find all unique job title in postgres database and public collection.#', 'INPUT_KEY': 'attributes->question'}, {'INPUT_JUSTIFICATION': 'The task specifies using the postgres protocol.', 'INPUT_SOURCE': '#postgres#', 'INPUT_KEY': 'attributes->protocol'}, {'INPUT_JUSTIFICATION': 'The task specifies using the postgres database.', 'INPUT_SOURCE': '#postgres#', 'INPUT_KEY': 'attributes->database'}, {'INPUT_JUSTIFICATION': 'The task specifies using the public collection.', 'INPUT_SOURCE': '#public#', 'INPUT_KEY': 'attributes->collection'}, {'INPUT_JUSTIFICATION': 'The task does not require additional context.', 'INPUT_SOURCE': '# #', 'INPUT_KEY': 'attributes->context'}, {'INPUT_JUSTIFICATION': 'This is the first step, so the input is empty.', 'INPUT_SOURCE': '#[]#', 'INPUT_KEY': 'input'}]]

# 1. NL2SQL("Find all unique job title in postgres database and public collection.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the NL2SQL step provides the job titles to be checked for programming language proficiency.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The task requires checking if the job title requires proficiency in any programming languages.', 'INPUT_SOURCE': '#This job title requires proficiency in any programming languages? True or False?#', 'INPUT_KEY': 'attributes->query'}, {'INPUT_JUSTIFICATION': 'No additional context is provided or required for this step.', 'INPUT_SOURCE': '# #', 'INPUT_KEY': 'attributes->context'}, {'INPUT_JUSTIFICATION': 'The task requires the output to include whether the job title requires programming language proficiency.', 'INPUT_SOURCE': "#['job_title', 'requires_proficiency']#", 'INPUT_KEY': 'attributes->attr_names'}]]

# 2. ROWWISE_NL2LLM("This job title requires proficiency in any programming languages? True or False?")
#  Your answer for connexion at this stage:[[{'INPUT_KEY': 'input', 'INPUT_SOURCE': '#[]#'}, {'INPUT_KEY': 'attributes->question', 'INPUT_SOURCE': '#Find all job postings.#'}, {'INPUT_KEY': 'attributes->protocol', 'INPUT_SOURCE': '#postgres#'}, {'INPUT_KEY': 'attributes->database', 'INPUT_SOURCE': '#postgres#'}, {'INPUT_KEY': 'attributes->collection', 'INPUT_SOURCE': '#public#'}, {'INPUT_KEY': 'attributes->context', 'INPUT_SOURCE': '# #'}]]

# 3. NL2SQL("Find all job postings.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the NL2SQL step provides the job postings to be appended.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The task requires appending job postings to job titles that require proficiency in programming languages.', 'INPUT_SOURCE': '$STEP2$->output', 'INPUT_KEY': 'attributes->new_element'}]]

# 4. APPEND("Append the job postings to the job titles that require proficiency in programming languages.")
#  Your answer for connexion at this stage:[[{'INPUT_KEY': 'input', 'INPUT_SOURCE': 'output'}, {'INPUT_KEY': 'attributes->join_on', 'INPUT_SOURCE': "#[['job_title'], ['job_title']]#"}, {'INPUT_KEY': 'attributes->join_type', 'INPUT_SOURCE': '#inner#'}, {'INPUT_KEY': 'attributes->join_suffix', 'INPUT_SOURCE': "#['_ds1', '_ds2']#"}, {'INPUT_KEY': 'attributes->keep_keys', 'INPUT_SOURCE': '#both#'}]]

# 5. JOIN("Join the job postings with the job titles that require proficiency in programming languages.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the JOIN step provides the data to be filtered.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The task requires filtering job postings that require proficiency in programming languages.', 'INPUT_SOURCE': '#requires_proficiency#', 'INPUT_KEY': 'attributes->operand_key'}, {'INPUT_JUSTIFICATION': 'The task requires selecting records where proficiency is required.', 'INPUT_SOURCE': '#=#', 'INPUT_KEY': 'attributes->operand'}, {'INPUT_JUSTIFICATION': 'The value to compare against is True, indicating proficiency is required.', 'INPUT_SOURCE': '#True#', 'INPUT_KEY': 'attributes->operand_val'}]]

# 6. SELECT("Keep only the job postings with proficiency in programming languages.")
#  Your answer for connexion at this stage:[[{'INPUT_JUSTIFICATION': 'The output from the SELECT step provides the filtered job postings to be used as context.', 'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_JUSTIFICATION': 'The task requires finding the column that corresponds to the maximum salary.', 'INPUT_SOURCE': '#Find the column that corresponds to the maximum salary.#', 'INPUT_KEY': 'attributes->query'}, {'INPUT_JUSTIFICATION': 'The task requires the output to include the column name for the maximum salary.', 'INPUT_SOURCE': "#['column_name']#", 'INPUT_KEY': 'attributes->attr_names'}]]

# 7. NL2LLM("Find the column that corresponds to the maximum salary.")
# 8. SELECT("Find the maximum salary among these job postings.")

# Your job is to define the logic that connects the different steps in the plan.

# We are currently transitioning from step 7 to step 8, specifically connecting:
# - Step 7: NL2LLM
# - Step 8: SELECT

# Below are the descriptions of the relevant operators, including their expected input, attributes, and output formats:
# NL2LLM 
# Given a natural language query, fetch open-domain knowledge from LLM using the input as context.

# input: [ [ {<key>:<value>, .. }, {...} ] ] 
# output:  [ [ {<key>:<value>, .. }, {...} ] ]
# attributes:
# -query:what we want to obtain from the query
# -attr_names: a list of string, each string being keys you want in the output (must not be empty)

# SELECT 
# Given an input data, consisting of a list of data elements,  filter data elements based on a specified condition (record-wise).

# input: [ [ {<key>:<value>, .. }, {...} ] ]
# output:  [ [ {<key>:<value>, .. }, {...} ]  ]
# attributes:
# operand_key: type str, the key to filter the data records
# operand: type str, comparison operator: =, !=, >, >=, <, <=, max, min, in, not in, like, not like
# operand_val: type Any, value to compare with (not needed for max, min)
# approximate_match: type bool, whether to use epsilon tolerance for numeric comparison.
# eps: type float, epsilon tolerance for numeric comparison

# Instructions:
# - If the task cannot be completed due to missing or incomplete information in the plan, begin your response with **ABORT** followed by a clear explanation of the issue.
# - Otherwise, provide the connection logic from **NL2LLM** to **SELECT**.
# - Give for:
#   - `"INPUT_JUSTIFICATION"`: a brief explanation/justification of why you use such source for such key.
#   - `"INPUT_SOURCE"`: the output of the previous step or a hardcoded string (surrounded with `#`, e.g. `"#value#"`).
#   - `"INPUT_KEY"`: the input field of the next step.
# - If the input/output field is nested (e.g., within a dictionary), represent it using `->` notation (e.g., `"attributes->query"`).
# - Each `INPUT_KEY` should appear only once in your response.

# Example:
# For a simple task with the following plan:
# 1. MULTIPLY ("Multiply input numbers by 2.")
# 2. MULTIPLY ("Multiply input numbers by 3.")

# Operator description for MULTIPLY:
# MULTIPLY
# Multiply each input number by a given factor.
# input: [ [ {<key>:<value>, .. }, {...} ],  [...]
# output:  [ [ {<key>:<value>, .. }, {...} ] ]
# attributes:
# -factor: the factor to multiply by

# The connection would look like:
# [
#     {
#         "INPUT_JUSTIFICATION": "The output from last step is the one to be multiplied.",
#         "INPUT_SOURCE": "output",
#         "INPUT_KEY": "input"
#     },
#     {
#         "INPUT_JUSTIFICATION": "The task requires a factor of 3.",
#         "INPUT_SOURCE": "#3#",
#         "INPUT_KEY": "factor"
#     }
# ]

# Now, provide the connection between **NL2LLM** and **SELECT**. Keep in mind that the current task is : SELECT("Find the maximum salary among these job postings.").
# Each `INPUT_KEY` should appear only once in your entire response.
# You MUST provide an INPUT_SOURCE for every attributes in **SELECT**, as well as for the input. The input can be empty (INPUT_SOURCE:'#[[]]#')
# In Special cases, you can need to gather information from an older step. In that case, add '$STEPi$->' with i the index of the step you want to refer to, at the beginning of INPUT_SOURCE.
# '''
# You provided the output:
# [[{'INPUT_KEY': 'input', 'INPUT_SOURCE': 'output'}, {'INPUT_KEY': 'attributes->operand_key', 'INPUT_SOURCE': 'output'}, {'INPUT_KEY': 'attributes->operand', 'INPUT_SOURCE': '#max#'}]]
# Include in each source the step number it comes from, using the format $STEPi$->output, where i is the step number. If the source is a hardcoded string, use the format #value#.
# Give an answer in the same format.
# """, [ "INPUT_KEY","INPUT_SOURCE"]))


# # You provided the output:
# # [[{'INPUT_KEY': 'input', 'INPUT_SOURCE': 'output'}, {'INPUT_KEY': 'attributes->operand_key', 'INPUT_SOURCE': '$STEP7$->output'}, {'INPUT_KEY': 'attributes->operand', 'INPUT_SOURCE': '#max#'}]]
# # This proposal is incorrect. Refine the INPUT_SOURCE and INPUT_KEY to ensure that the connection between NL2LLM and SELECT is correctly established, including all necessary attributes and the input data.
# # Especially, focus on the connection of inputs and outputs : are there coming from the correct step?
# # Give an answer in the same format.
# # """, [ "INPUT_KEY","INPUT_SOURCE"]))

# sys.exit("4")



#This looks like it is working, but I had to skip the nested operator of ITERATE that make it more complex.
#The output is :
#[[{'INPUT_SOURCE': 'output', 'INPUT_KEY': 'input'}, {'INPUT_SOURCE': '#This job title requires proficiency in any programming languages? True or False?#', 'INPUT_KEY': 'attributes->query'}]]
operators_description_linear = {
'SMARTNL2SQL':["""SMARTNL2SQL
Convert a natural language question into a SQL query to run on a database, or on the inputs.
input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
output:  [ [ {<key>:<value>, .. }, {...} ] ]
If you want to run on a database, provide empty inputs [[]] - still providing the attributes - the database will be queried.
Returns the rows from the table matching the query, including its columns (fields) names. 
attributes:
- question: the natural language query to convert.
  - runOn : must be either 'database' or 'input'
- context: optional context.
⚠️ IMPORTANT:
  - When inputs are provided, the table name MUST be 'tb'. Do NOT use any other table name like 'jobs', 'users', etc.
  - When inputs are empty ([[]]), the database will be queried instead.
  - The input list can only contain one item.
  ""","Returns the rows from the table matching the query, including its columns (fields) names."],

'JOIN_2': ["""JOIN_2
Given an input data and another one provided as attributes->new_element return n-way join 

input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
output:  [ [ {<key>:<value>, .. }, {...} ] ]
attributes:
-join_on_table1: the column name to join on in the first table.
-join_on_table2: the column name to join on in the second table. **Should be from a field compatible with join_on_table1.** 
-join_type: type str, type of join in 'inner', 'left', 'right', 'outer'
-new_element: the input to join to
-join_suffix: type list of str, each element (str) refers to the additional suffix to add to a data source/group field names to avoid conflicts. The default suffix is ‘_ds{i}’ for data source/group i in the input data.
-keep_keys: type str, one value in ‘left’, ‘both’. 
--‘left’: keep only the join index of the first data group in the input data
--‘both’: keep all join indices with suffixes.""","Returns the joined data based on the specified join conditions and attributes."],
'NL2SQL': ["""NL2SQL 
Convert a natural language question into a SQL query, and return the results.
input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
output:  [ [ {<key>:<value>, .. }, {...} ] ]
attributes:
-question: what we want to obtain from the query
-protocol: one of ["postgres", "mysql"]
-source: one of ["postgres_example"]
-database: choose between postgres or any other
-collection: choose between public or any other collection
-context : can be empty, used to provide additionnal details""","Returns the rows from the table matching the query, including its columns (fields) names."],
###Modified rowwose nl2llm input from input: [ [ {<key>:<value>, .. }, {...} ],  [...] ] to input: [ [ ] ]. It worked before also, but i want it to be able to infer on context for instance for find in this josn the key corresponding to blabla
'ROWWISE_NL2LLM':["""ROWWISE_NL2LLM 
Given a natural language query, fetch open-domain knowledge from LLM.

input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
output:  [ [ {<key>:<value>, .. }, {...} ] ]
attributes:
-query:what we want to obtain from the query
-context: can be empty, used to provide additionnal details 
-attr_names: a list of string, each string being keys you want in the output""","Returns the input table augmented with new columns attr_names containing LLM result of question attribute on each row."],
'COUNT':["""COUNT 
Count the number of elements in the first element of input, input being a list of list

input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
output:  [ [ {<key>:<value>, .. }, {...} ] ]
attributes:
Always provide \{\} as attributes, the empty dictonary""",
"Returns [[{'count':NB_ELEMENTS}]]"],
'NL2LLM':["""NL2LLM 
Given a natural language query, fetch open-domain knowledge from LLM using the input as context.

input: [ [ {<key>:<value>, .. }, {...} ] ] 
output:  [ [ {<key>:<value>, .. }, {...} ] ]
attributes:
-query:what we want to obtain from the query
-attr_names: a list of string, each string being keys you want in the output (must not be empty)""","Returns in output the answer to the question's attribute, placed inside attr_names field(s), relying on common knowledge and on input if provided."],
'MULTIPLY': ["""MULTIPLY
Multiply each input number by a given factor.
input: [ [ {<key>:<value>, .. }, {...} ],  [...]
output:  [ [ {<key>:<value>, .. }, {...} ] ]
attributes:
-factor: the factor to multiply by""","Returns the input multiplied by the factor."],
'START': ["""START
The first task, we don't have any output yet. Use hardcoded strings to connect to the next operator.""","No output, starting point of the plan."],
'JOIN': ["""JOIN 
Given an input data,  comprising two or more lists of data, return n-way join 

input: [ [ {<key>:<value>, .. }, {...} ],  [...] ]
output:  [ [ {<key>:<value>, .. }, {...} ] ]
attributes:
-join_on: **must be list of list of string**, the main list contain a list for each table to join, with inside the column name where to perform the join. If the join is performed on columns with the same name, the column name should be repeated in each list of list. For instance, '[[colA],[colb]]'.
-join_type: type str, type of join in 'inner', 'left', 'right', 'outer'
-join_suffix: type list of str, each element (str) refers to the additional suffix to add to a data source/group field names to avoid conflicts. The default suffix is ‘_ds{i}’ for data source/group i in the input data.
-keep_keys: type str, one value in ‘left’, ‘both’. 
--‘left’: keep only the join index of the first data group in the input data
--‘both’: keep all join indices with suffixes.""","Returns the joined data based on the specified join conditions and attributes."],

##i modified the opeand and operand_val of select to include max min in not in
'SELECT': ["""SELECT 
Given an input data, consisting of a list of data elements,  filter data elements based on a specified condition (record-wise).

input: [ [ {<key>:<value>, .. }, {...} ] ]
output:  [ [ {<key>:<value>, .. }, {...} ]  ]
attributes:
operand_key: type str, the key to filter the data records. Should be the exact key
operand: type str, comparison operator: =, !=, >, >=, <, <=, max, min, in, not in, like, not like
operand_val: type Any, value to compare with (not needed for max, min)
approximate_match: type bool, whether to use epsilon tolerance for numeric comparison.
eps: type float, epsilon tolerance for numeric comparison""","Returns the input data with only the records that match the specified condition."],
'APPEND': [ """APPEND
Given an input data, consisting of a list of data elements, append a new data element to the input data.
input: [ [ {<key>:<value>, .. }, {...} ] ]
output:  [ [ {<key>:<value>, .. }, {...} ]  ]
attributes:
new_element: the element to append to the input data, in the same format as the input data elements.""","Returns the input data with the new element appended."],
# 'FILTER': """FILTER
# Given an input data, consisting of a list of data elements, filter data elements based on a specified condition (record-wise).
# input: [ [ {<key>:<value>, .. }, {...} ] ]
# output:  [ [ {<key>:<value>, .. }, {...} ]  ]
# attributes:
# operand_key: type str, the key to filter the data records
# operand: type str, possible values are 'max', 'min'
}
def auto_prompt(plan:list, step:int, task:str, previous_answers:List,error_mitigation=''):
    """
    This function takes a plan and a step number, and returns a prompt to connect the operators in the plan.
    The plan is a list of steps, where each step is a dictionary with keys 'name' and 'description'.
    The step number is the index of the step to connect to the next one.
    """
    #Inconsistent with the rest, here current step is the previous step and the one that needs to be linked is +1
    if step >= len(plan) - 1:
        logging.critical("Linker: Should not happen: No more steps to connect.")
    
    current_step = plan[step]
    next_step = plan[step + 1]
    
    # logging.critical(step)
    logging.critical(plan)
    plan_to_txt= "\n".join([f"{i}. {stepplan['name']}(\"{stepplan['description']}\"): Output: "+operators_description_linear[stepplan['name']][1]+("\n Your answer for connexion at this stage:"+previous_answers[i]+"\n" if len(previous_answers)>i and len(previous_answers[i])>0 else '') +'\n' for i, stepplan in enumerate(plan)])
    if not error_mitigation=='':
        error_mitigation=f"""
        **CAUTION: this is not the first time you're building this linking. Last time it produced the following error :
        {error_mitigation}
        Be sure to produce a linking that avoid this mistake this time."""
#     prompt = f"""A planner gave the following plan to solve the following taks:
# {task}
# The tasks are as follows:
# {plan_to_txt}

# Your task is to provide the logic to connect the different steps of the plan.
# We are currently doing step {str(step)}->{str(step+1)}, connecting {current_step['name']} with {next_step['name']}.
# Here are the definitions of the operators, and the expected input, attributes and output formats:
# {operators_description_linear[current_step['name']]}

# {operators_description_linear[next_step['name']]}


# If the tasks is not doable because of a missing detail in the planner plan, start your answer with ABORT with a description of the issue.
# Give the connection from {current_step['name']} to {next_step['name']} : we want to connect each element from the input of {current_step['name']} to an element from the output of {next_step['name']} or a hardcoded string.
# Give in every dict for key 'INPUT_SOURCE' the hardcoded string or the output of the first module, and for key 'INPUT_KEY'  the input of the second module.
# For dict and nested properties, link them with '->' (e.g. 'attributes->query').
# The INPUT_SOURCE be replaced by a hardcoded string if it is the right thing to do - distinguish them with # surrounding them.

# Here is a toy example of the expected output format on a different task:
# In this task, the plan is the following:
# 1. MULTIPLY("Multiply input numbers by 2.")
# 2. MULTIPLY("Multiply input numbers by 3.")
# The description of MULTIPLY is the following:
# {operators_description_linear['MULTIPLY']}
# The connection between the two modules is the following:
# [
#     {{
#         "INPUT_SOURCE": "output",
#         "INPUT_KEY": "input"
#     }},
#     {{
#         "INPUT_SOURCE": "#3#",
#         "INPUT_KEY": "factor"
#     }}
# ]
# Each INPUT_KEY should only appear once in your whole answer.
# """
    prompt = f"""A planner has generated the following plan to solve the task:
{task}

The steps of the plan are as follows:
{plan_to_txt}

Your job is to define the logic that connects the different steps in the plan.

We are currently transitioning from step {str(step)} to step {str(step+1)}, specifically connecting:
- Step {str(step)}: {current_step['name']}
- Step {str(step+1)}: {next_step['name']}

Below are the descriptions of the relevant operators, including their expected input, attributes, and output formats:
{operators_description_linear[current_step['name']][0]}

{operators_description_linear[next_step['name']][0]}

Instructions:
- If the task cannot be completed due to missing or incomplete information in the plan, begin your response with **ABORT** followed by a clear explanation of the issue.
- Otherwise, provide the connection logic from **{current_step['name']}** to **{next_step['name']}**.
- Give for:
  - `"INPUT_JUSTIFICATION"`: a brief explanation/justification of why you use such source for such key.
  - `"LINKING_RELEVANCE"`: a brief explanation of why this connection is relevant to the task. **Does the selected source contains everything needed?**
  - `"INPUT_SOURCE"`: data from one of the previous step starting by $STEPi$-> with i the step number (e.g., $STEP1$->output, you can get specific items from it such as $STEP1$->output[0]['id'] - if you can source from multiple steps, use the earliest one) or a hardcoded string (surrounded with `#`, e.g. `"#value#"`).
  - `"INPUT_KEY"`: the input field of the next step.
- If the input/output field is nested (e.g., within a dictionary), represent it using `->` notation (e.g., `"attributes->query"`).
- Each `INPUT_KEY` should appear only once in your response.

Example:
For a simple task with the following plan:
'''
A planner has generated the following plan to solve the task:
Multiply input numbers by 2 and then by 3.

The steps of the plan are as follows:
0. START(""): Output: No output, starting point of the plan.
1. MULTIPLY ("Multiply input numbers by 2."): Output: Returns the input multiplied by the factor.
2. MULTIPLY ("Multiply the result by 3."): Output: Returns the input multiplied by the factor.

Below are the descriptions of the relevant operators, including their expected input, attributes, and output formats:
{operators_description_linear['MULTIPLY'][0]}
'''

The connection would look like:
[
    {{
        "INPUT_JUSTIFICATION": "Output from 0 cannot be used as it lacks the multiplication by 2. The output from step 1 has to be multiplied by the factor.",
        "LINKING_RELEVANCE": "It is relevant to take the output from step 1 that contains the result of the first multiplication to use it as input for the next step, so it can be multiplied again. We took the result already multiplied by 2 so we didn't take a too early step.",
        "INPUT_SOURCE": "$STEP1$->output",
        "INPUT_KEY": "input"
    }},
    {{
        "INPUT_JUSTIFICATION": "The task requires to multiply by a factor of 3.",
        "LINKING_RELEVANCE": "It is relevant to take the hardcoded 3 from the task to put it inside the factor attribute for next step. ",
        "INPUT_SOURCE": "#3#",
        "INPUT_KEY": "attributes->factor"
    }}
]

Now, provide the connection between **{current_step['name']}** and **{next_step['name']}**. Keep in mind that the current task is : {next_step['name']}("{next_step['description']}").
Each `INPUT_KEY` should appear only once in your entire response.
You MUST provide an INPUT_SOURCE for every attributes in **{next_step['name']}**, as well as for the input. The input can be empty and in that case should rely on step 0 output (INPUT_SOURCE:'$STEP0$->output').
**DO NOT** invent variable names or infer a probable column name from database for INPUT_SOURCE. Gather the names by refering to outputs of previous steps. 
When refering to an earlier step, use the earliest one but be very careful to make sure it contains all the necessary data! You should ask yourself in the justification: in the step I selected, are there already all the infos needed to answer the question or should I take a more recent step?
**DO NOT** suppose that database include information on a column if that information was also gathered by one later step. Always gather the information from that step in that situation.{error_mitigation}"""
#**Remember to use the earliest step number possible for the INPUT_SOURCE. Before relying on a recent step number, check if one of the first steps contain the necessary data.**"""
#Add '$STEPi$->' with i the index of the step you want to refer to, at the beginning of INPUT_SOURCE."""
    return prompt

def get_operator_linking(prompt):
    return standard_NL2LLM_agent(prompt, [ "INPUT_KEY","LINKING_RELEVANCE","INPUT_SOURCE"])


#TODO: IMPLEMENT from_step_X_with_refinement=-1 to rebuild the linking from that step and rerun also from that step 
#Called on error either by in run or by itself when sees a problem on a step
#We should also see if an output is available and also < to this variable, otherwise we launch the run
def execute_linking(plan,global_task,abort_trigger,ancestor_dico,steps_linking, threads,next_direction_set,dico_error_detection,lock,from_step_X_with_refinement=-1,issue_expl=None):
    prev_answers=['']

    for ielt,elt in enumerate(plan):
        if ielt<from_step_X_with_refinement:
            continue
        if ielt==0:
            #We don't link the START step
            continue
        # logging.critical('@@@DEBUG: ielt'+str(ielt)+', plan:'+str(plan))
        # if ielt==1:
            ##We want to measure the time for first linking step as it the latency when combining with planning time
        start_time = time.perf_counter()
        
        if abort_trigger.is_set():
            logging.critical('Abort signal received, not proceeding')
            return
        logging.critical('Linking: Performing linking with LLM for step '+str(ielt) +', from '+plan[ielt-1]['name'] + ' to '+ plan[ielt]['name'])
        # print('@@@@@@@@@@@@@@@@linking step ',ielt, 'len plan is', len(plan))
        # print(f"=== STEP {ielt} ===")
        # print(elt)
        if True:#ielt < len(plan) - 1:
            try:
                if from_step_X_with_refinement==ielt:
                    logging.critical('Linking: Including issue mitigation for the current step.')
                    tmp=auto_prompt(plan, ielt-1, global_task, prev_answers,issue_expl)
                else:
                    tmp=auto_prompt(plan, ielt-1, global_task, prev_answers)
            except Exception as e:
                logging.critical('Linking: Problem with auto_prompt for step '+str(ielt)+'.')
                #get stack trace
                logging.critical(''.join(traceback.format_exception(None, e, e.__traceback__)))
                logging.critical(str(e))
                with lock:
                    dico_error_detection[IssueLevel.plan_level]['autoprompt_creation'] = {}
                    dico_error_detection[IssueLevel.plan_level]['autoprompt_creation']['-1']={'summary_issue':str(e),'full_output':str(e)}
                next_direction_set.add(IssueLevel.plan_level)
                dico_error_detection[IssueLevel.plan_level]['issue_path']=','.join(['autoprompt_creation','-1'])
                abort_trigger.set()
                return
            # print(tmp)
            # print("==="*10)
            logging.critical('No error this time')
            res=get_operator_linking(tmp)
            if abort_trigger.is_set():
                logging.critical('Abort signal received, not proceeding')
                return
            logging.critical('Linking: Starting error detection at step '+str(ielt)+'.')
            for name_mitigator,mitigator in mitigators_linking:
                    # dico_error_detection[name_mitigator]=dict()
                    t = threading.Thread(target=run_error_detection, args=(mitigator, {'task':global_task,'plan':plan,'step_linking':res,'step_number':ielt}, next_direction_set,dico_error_detection,IssueLevel.linking_level,name_mitigator, abort_trigger,lock))
                    t.start()
                    threads.append(t)
            #TODO: the error detection - in different thread - for linking
            if abort_trigger.is_set():
                logging.critical('Abort signal received, not proceeding')
                return
            prev_answers.append(str(res)) #would be smart to not have two lists for the same thing
            ancestor_dico[ielt]=[]
            # print(res)
            # print(type(res[0][0]))
            for this_reso in res[0]:
                # print(this_reso)
                # print(type(this_reso))
                this_res=this_reso['INPUT_SOURCE']
                if '$STEP' in this_res:
                    this_res= this_res.split('$STEP')
                    # print(this_res)
                    this_res=[int(elt.split('$')[0]) for elt in this_res if len(elt)>0 and '$->output' in elt]
                    if ielt in this_res:
                        logging.critical('Linking : weird results as current step needed for current step linking : '+str(res))
                    ancestor_dico[ielt]+=this_res
            res[0][-1]['TIME_LINKING']= time.perf_counter() - start_time
            steps_linking[ielt]=res[0]
            # print(res)
        # else:
        #     print("This is the last step, no more connection to do.")

    # return steps_linking, ancestor_dico


    # ancestor_dico={0:[], 1: [0], 2: [1], 3: [0], 4: [1, 3], 5: [4], 6: [4, 5], 7: [6, 2], 8: [7, 5]}
    # print("ancestor_dico")
    # print(ancestor_dico)
def detect_orphans(ancestor_dico,orphans):
    # orphans=[]
    ancestor_dico_reversed={}
    for key,val in ancestor_dico.items():
        if key not in ancestor_dico_reversed:
            ancestor_dico_reversed[key]=[]
        for elt2 in val:
            if elt2 not in ancestor_dico_reversed:
                ancestor_dico_reversed[elt2]=[]
            if key not in ancestor_dico_reversed[elt2]:
                ancestor_dico_reversed[elt2].append(key)
    # print('Detection of orphan steps:')
    # print(ancestor_dico_reversed)
    # for ielt,elt in enumerate(ancestor_dico_reversed):
    #     if len(ancestor_dico_reversed[ielt])==0:
    #         # print(f"Step {ielt} is an orphan step, it is not used by any other step.")
    #         orphans+=[ielt]
    #         # sys.exit("Exiting script now")
    #     # else:
    #     #     print(f"Step {ielt} is used by steps {ancestor_dico_reversed[ielt]}")
    # # return orphans
    for ielt in range(1, max(list(ancestor_dico.keys()))):
        if not ielt in ancestor_dico_reversed.keys():
            orphans+=[ielt]


def post_linking(steps_linking):
    #Or maybe it should be a class then, rather than calling it back with the parameters
    #Would make sense also with regards to the autocorrect fonctions
    """During the execution, takes what was produced durng the operator linking step to link things between them."""
    return

if __name__ == "__main__":
    plan=[{'name':"START", 'description':""},
    {'name':"NL2SQL", 'description':"Find all unique job title in postgres database and public collection."},
    {'name':"ROWWISE_NL2LLM", 'description':"Add to this table the information for each job title if they require proficiency in programming languages."},
    {'name':"NL2SQL", 'description':"Find all job postings."},
    {'name':"APPEND", 'description':"Append the job postings to the table containing job titles and their proficiency in programming languages."},
    {'name':"NL2LLM", 'description':"Find the job postings columns database that correspond to the job title, and to maximum salary."},
    {'name':"JOIN", 'description':"Join the job postings with the job titles that require proficiency in programming languages."},
    {'name':"SELECT", 'description':"Keep only the job postings with proficiency in programming languages."},
    # {'name':"NL2LLM", 'description':"Which column in database corresponds to the maximum salary?"},
    {'name':"SELECT", 'description':"Find the maximum salary among these job postings."}]
    global_task="Find the maximum salary among people who should have proficiency in at least one programming language."

    execute_linking(plan,global_task)




# sys.exit("Exiting script now")

# input_data = [[]]
# attributes = {

#     "question": "Find all unique job title, limit to 20 total job title.",  #ADD THE CONSTRAINT OF 20 FOR TESTING
#     "protocol": "postgres",
#     "database": "postgres",
#     "collection": "public",
#     "case_insensitive": True,
#     "additional_requirements": "",
#     "context": "",#This is a job database with information about job postings, skills, companies, and salaries",
#     # schema will be fetched automatically if not provided
# }
# # nl2llm_operator = NL2LLMOperator()
# # properties_NL2LLM = nl2llm_operator.properties

# # properties_NL2LLM['service_url'] = 'ws://localhost:8001'  # update this to your service url
# # attributes_NL2LLM = {
# #     "query": "This job title requires proficiency in any programming languages? True or False?",
# #     "context": "",
# #     # "attr_names": ["language", "popularity_rank", "description"],
# #     "attr_names": ["proficiency"]
# # }

# # def iterate(input_to_iter, function_to_use, input_data, attributes, properties, lambda_where_to_apply_input):
# #     # attr=copy.deepcopy(attributes)
# #     res=[]
# #     for inp in input_to_iter:
# #         inp,attr,prop = lambda_where_to_apply_input(inp, attributes, properties)
# #         result = function_to_use([inp], attr, prop)
# #         # print(f"=== ITERATE TMP RESULT ===")
# #         # print(result)
# #         res+= [result[0][0]|inp]
# #     return res

# # # just used to get the default properties
# # nl2sql_operator = NL2SQLOperator()
# # properties = nl2sql_operator.properties
# # properties['service_url'] = 'ws://localhost:8001'  # update this to your service url

# # result = nl2sql_operator_function(input_data, attributes, properties)
# # print(result)
# # #Semantic_Query("SELECT job_title WHERE 'expected_to_be_proficient_in_programming_language'")-->tb2
# # #OR ITERATE(tb2, NL2LLM('This job title requires proficiency in which programming languages?'))

# # def set_attr(input_data, attributes, properties):
# #     attributes['context'] = input_data
# #     return input_data, attributes, properties
# # iterate_result = iterate(result, nl2llm_operator_function, input_data, attributes_NL2LLM, properties_NL2LLM, set_attr)
# # print("=== ITERATE RESULT ===")
# # print(iterate_result)
# attributes = {
#     "question": "Get the first 100 job postings",  #ADD THE CONSTRAINT OF 20 FOR TESTING
#     "protocol": "postgres",
#     "database": "postgres",
#     "collection": "public",
#     "case_insensitive": True,
#     "additional_requirements": "",
#     "context": "",#This is a job database with information about job postings, skills, companies, and salaries",
#     # schema will be fetched automatically if not provided
# }
# result = nl2sql_operator_function(input_data, attributes, properties)
# attributes = {"join_on": [["job_title"],["job_title"]], "join_type": "inner", "keep_keys": "left"}
# print("=== INP DATA ===")
# input_data=[result, iterate_result]
# print(input_data)

# result = join_operator_function(input_data, attributes)
# print("=== JOIN RESULT ===")
# print(result)
# #Join(innerJoin, ORIGINAL_TABLE, tb2)-->tb3
# #tb3-->NL2SQL("Find the maximum salary among these job postings.")-->tb4

# #Another plan less efficient could be to do it row by row, with an ITERATE operator (this operator make more sense on other tasks to be determined - if we have semantic query I am not so sure)
