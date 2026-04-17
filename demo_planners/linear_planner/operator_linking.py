"""Operator-linking prompt builders and execution helpers for the linear planner."""
import copy
import json
import sys
from typing import List, Dict, Any, Callable, Optional
import logging
from blue.operators.nl2llm_operator import *
from blue.operators.nl2sql_operator import *
from blue.operators.join_operator import *
from demo_planners.utils import *
from demo_planners.linear_planner.error_tackling import *
import threading



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
-source: one of ["default"]
-database: choose between postgres or any other
-collection: choose between public or any other collection
-context : can be empty, used to provide additionnal details""","Returns the rows from the table matching the query, including its columns (fields) names."],
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
    
    logging.critical(plan)
    plan_to_txt= "\n".join([f"{i}. {stepplan['name']}(\"{stepplan['description']}\"): Output: "+operators_description_linear[stepplan['name']][1]+("\n Your answer for connexion at this stage:"+previous_answers[i]+"\n" if len(previous_answers)>i and len(previous_answers[i])>0 else '') +'\n' for i, stepplan in enumerate(plan)])
    if not error_mitigation=='':
        error_mitigation=f"""
        **CAUTION: this is not the first time you're building this linking. Last time it produced the following error :
        {error_mitigation}
        Be sure to produce a linking that avoid this mistake this time."""
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
    return prompt

def get_operator_linking(prompt):
    return standard_NL2LLM_agent(prompt, [ "INPUT_KEY","LINKING_RELEVANCE","INPUT_SOURCE"])


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
        start_time = time.perf_counter()
        
        if abort_trigger.is_set():
            logging.critical('Abort signal received, not proceeding')
            return
        logging.critical('Linking: Performing linking with LLM for step '+str(ielt) +', from '+plan[ielt-1]['name'] + ' to '+ plan[ielt]['name'])
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
            if abort_trigger.is_set():
                logging.critical('Abort signal received, not proceeding')
                return
            prev_answers.append(str(res)) #would be smart to not have two lists for the same thing
            ancestor_dico[ielt]=[]
            for this_reso in res[0]:
                this_res=this_reso['INPUT_SOURCE']
                if '$STEP' in this_res:
                    this_res= this_res.split('$STEP')
                    this_res=[int(elt.split('$')[0]) for elt in this_res if len(elt)>0 and '$->output' in elt]
                    if ielt in this_res:
                        logging.critical('Linking : weird results as current step needed for current step linking : '+str(res))
                    ancestor_dico[ielt]+=this_res
            res[0][-1]['TIME_LINKING']= time.perf_counter() - start_time
            steps_linking[ielt]=res[0]



def detect_orphans(ancestor_dico,orphans):
    ancestor_dico_reversed={}
    for key,val in ancestor_dico.items():
        if key not in ancestor_dico_reversed:
            ancestor_dico_reversed[key]=[]
        for elt2 in val:
            if elt2 not in ancestor_dico_reversed:
                ancestor_dico_reversed[elt2]=[]
            if key not in ancestor_dico_reversed[elt2]:
                ancestor_dico_reversed[elt2].append(key)
    #         orphans+=[ielt]
    for ielt in range(1, max(list(ancestor_dico.keys()))):
        if not ielt in ancestor_dico_reversed.keys():
            orphans+=[ielt]


def post_linking(steps_linking):
    """During the execution, takes what was produced durng the operator linking step to link things between them."""
    return

