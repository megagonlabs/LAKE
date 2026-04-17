
import json
from demo_planners.utils import *
#And because it can be too lengthy, have one small part for the initial prompt
#And one longer for the refinement prompt to correct errors, where we can put only the relevant operators




class IssueLevel:
    none='termination'
    plan_level = "plan_level (related to the plan logic and executability)"
    linking_level = "linking_level (related to the linking between the tools)"
    execution_level = "execution_level (related to the execution behavior of tools)"
    post_verification= "post_verification (related to analysis of the end result)"
    in_run_verification= "in_run_verification (related to analysis of the results of an intermediate state)"

def NL2SQLInstantCheck(predecessors,successors):
    if len(successors)==0 and len(predecessors)>1:
        return "NL2SQL is used at the end of pipeline consisting of multiple steps. Since NL2SQL cannot operate on previous results, this plan cannot be correct. Be sure that the last step is not NL2SQL if there are multiple steps."

def SelectInstantCheck(predecessors,successors):
    if predecessors[-1]=='START':
        return "You cannot perform SELECT without having retrieved data first. Be sure to add a prior step that will retrieve data."
    # if predecessors[-1]=='NL2SQL':
    #     return "It seems you want to use SELECT on the result of a NL2SQL. This does not make sense,"
def JoinInstantCheck(predecessors,successors):
    minLenOutput=0
    for predecessor in predecessors:
        if predecessor=='APPEND':
            minLenOutput=max(minLenOutput,2)
            return "" #why continuing
        minLenOutput=max(minLenOutput,1)
    if minLenOutput<2:
        return "You cannot perform JOIN without having retrieved and merged the data in a single output first."
def AppendInstantCheck(predecessors,successors):
    if len(predecessors[-1])<3:
        return "You cannot perform APPEND without having retrieved at least retrieved two sources of data to merge."
    


    
instant_check_fcts={'NL2SQL':NL2SQLInstantCheck, 'SELECT': SelectInstantCheck, 'JOIN':JoinInstantCheck, 'APPEND':AppendInstantCheck}


def run_error_detection(mitigator, current_log, next_direction_set, resdico:defaultdict,level,name_mitigator, stop_event,lock):
    if stop_event.is_set():
        logging.critical('Abort signal received, not proceeding')
        return
    logging.critical('Error detection : '+name_mitigator + ' at level '+level+' started.')
    if 'step_number' in current_log.keys():
        current_nb=str(current_log['step_number'])
    else:
        current_nb=str(-1)
    mitigator_result=mitigator(current_log)
    presence_issue,summary_issue=detect_issue(mitigator_result)
    # current_nb=len(resdico[level])
    # logging.critical('len(resdico[level])'+str(len(resdico[level]))+', type(resdico[level])'+str(type(resdico[level])))
    # logging.critical('len(resdico[level][name_mitigator])'+str(len(resdico[level][name_mitigator]))+', type(resdico[level][name_mitigator])'+str(type(resdico[level][name_mitigator])))
    with lock:
        if name_mitigator not in resdico[level]:
            resdico[level][name_mitigator] = {}
        resdico[level][name_mitigator][current_nb]={'summary_issue':summary_issue,'full_output':mitigator_result}
    if presence_issue:
        # logging.critical('Error detection: Stop event occurred, disabled for debug, put back!!!!')
        stop_event.set()
        logging.critical('! Error detection : '+name_mitigator + ' at level '+level+' ended with issue.')
        # remove the # !
        with lock:
            if not 'ISSUE_LEVEL' in mitigator_result[0][0].keys() :
                next_direction_set.add(level)
            else:

                new_level={'plan':IssueLevel.plan_level, 'linking':IssueLevel.linking_level}[mitigator_result[0][0]['ISSUE_LEVEL']]
                if not new_level==level:
                    logging.critical('! Error detection: mitigator changed error level from '+level+' to '+new_level+'.')
                next_direction_set.add(new_level)
                if name_mitigator not in resdico[new_level]:
                    resdico[new_level][name_mitigator] = {}
                resdico[new_level][name_mitigator][current_nb]={'summary_issue':summary_issue,'full_output':mitigator_result}
                resdico[new_level]['issue_path']=','.join([name_mitigator,current_nb])
            resdico[level]['issue_path']=','.join([name_mitigator,current_nb])
            
    else:
        logging.critical('Error detection : '+name_mitigator + ' at level '+level+' ended with no issue.')










#In fact I guess the 3 handlers, idea level, tool common issue, this type of query frequent issue and solution, might be run parallely
# Could be a system named like multi catcher

def universal_correction(output, refinement_prompt, input_filter,output_matcher):
    """Abstraction for a specific type of refinement, for a prompt localized on a specific task, and reformat the output to be universal
    Might be useful, but I think it is too complicate for nothing"""
    return

def summarize_issue_and_tackling():
    """state what was wrong before, say the path followed to improve the situation, provide assessment of the new situation
    Maybe can summarize everything from the beginning? So we only provide the essence to the new prompt, and not all the stuffs that he will have to digest"""
    return



def detect_issue(mitigator_result):
    summary_issue=[]
    presence_issue=False
    for elt in mitigator_result[0]:
        for key in [x for x in list(elt.keys()) if '_TRUEORFALSE' in x]:
            if (type(elt[key])==str and elt[key].lower()=='true') or elt[key]==True:
                summary_issue+=[elt["ISSUE_SUMMARY"]]
                presence_issue=True
                break
    return presence_issue,json.dumps(summary_issue,indent=2)




def correct_plan_idea(current_output):
    prompt_sense_check="""
    You are a planner validator. Your task is the following.
    Given this specific task: {overall_task}
    A planner colleague agent produced this plan:
    {tentative_plan}

    The tools used are the following:
    {used_tools}

    For your information, these tools are also available:
    {other_available_tools}

    The available data is the following:
    {data_structure}

    Does this plan make sense?
    Given the specific characteristics of each used operator, have they been followed rigourously?
    Is any operator misplaced or requesting another step here that has not been prepared?

    You should structure your output as a JSON containing a list of dictionaries, one dictionary per step item. For each of this step item, provide the following keys:
    - ORIGINAL_STEP_NAME: repeat the name of the tool used
    - ORIGINAL_STEP_DESCRIPTION: repeat the description given to the tool at this step
    - ISSUE_TOOL_REQUIREMENT_JUSTIFICATION: think about the tool requirement. Is everything available at this stage for the tool to operate? Is everything needed for the tool included in one of the step predecessing the tool? Include in your thinking for this step the tool requirement. Is everything that you mentioned in your reasoning was indeed a task performed by an earlier step? If you detect an issue explain it and explain how it can be solved
    - ISSUE_TOOL_REQUIREMENT_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_PLAN_LOGIC_JUSTIFICATION: Are there any step that direct the answer in a different direction than what the user is expecting? Is the sequence of operator logical? If you detect an issue explain it and explain how it can be solved
    - ISSUE_PLAN_LOGIC_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_MISSING_PIECES_JUSTIFICATION: include in your thinking for this step the tool requirement.  Also think about the data structure. Are we asking the current tool to rely on a data that is not available neither from database nor from a previous step? If we are using data from a previous step, can this tool use it as is? If you detect an issue explain it and explain how it can be solved
    - ISSUE_MISSING_PIECES_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_FINAL_GOAL_JUSTIFICATION: Is the final step aligned with what the user asked for?  If you detect an issue explain it and explain how it can be solved
    - ISSUE_FINAL_GOAL_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_SUMMARY: Provide a brief summary of the issues detected in this step, if any. If no issue was detected, write "". Give all details for the planner to be able to correct the plan.
    """
    #removed from first line: "You work at the idea level." Not sure if needed
    task=current_output['task']
    plan=current_output['plan']
    
    used_tools_name=[x['name'] for x in plan]
    available_tools_name=[x for x in get_available_tools() if not x in used_tools_name]
    used_tools=get_tool_description( used_tools_name)#, level=['basic','advanced'])
    other_available_tools=get_tool_description( available_tools_name)
    prompt=prompt_sense_check.format(overall_task=task, tentative_plan=plan,used_tools=used_tools, other_available_tools=other_available_tools, data_structure=data_infos)
    # print(prompt)
    return standard_NL2LLM_agent(prompt,["ORIGINAL_STEP_NAME","ORIGINAL_STEP_DESCRIPTION","ISSUE_TOOL_REQUIREMENT_JUSTIFICATION","ISSUE_TOOL_REQUIREMENT_TRUEORFALSE","ISSUE_PLAN_LOGIC_JUSTIFICATION","ISSUE_PLAN_LOGIC_TRUEORFALSE","ISSUE_MISSING_PIECES_JUSTIFICATION","ISSUE_MISSING_PIECES_TRUEORFALSE","ISSUE_FINAL_GOAL_JUSTIFICATION","ISSUE_FINAL_GOAL_TRUEORFALSE","ISSUE_SUMMARY"])


# Execution


def correct_execution_error(current_output):
    task=current_output['task']
    plan=current_output['plan']


    return 



# Post-run checks

def in_run_execution_check(current_output):

    prompt_inrun_check="""
    You are a plan execution validator. A planner agent together with a linker agent built a plan to solve the given task: {overall_task}
    The plan is:
    {tentative_plan}

    The tools used in this plan are the following:
    {used_tools}

    For your information, these tools are also available:
    {other_available_tools}

    To execute the current tool, {current_step_name_and_description}, the linker used the following inputs:
    {tool_inputs_and_attributes}
    The execution resulted in the following output:
    {result_output}

    Is this output usable in the current format?
    Does this output go in the right direction to reach the final goal?
    Is the execution revealing any omittment from the planner or the linker?

    You should structure your output as a JSON containing a single dictionary answering the following points. Provide the following keys:
    - ORIGINAL_STEP_NAME: repeat the name of the tool used
    - ORIGINAL_STEP_DESCRIPTION: repeat the description given to the tool at this step
    - ISSUE_NEXT_STEPS_USABILITY_JUSTIFICATION: think about the next steps. If they need data from this step, will the output be digestable by them? Or is there an issue? If you detect an issue explain it and explain how it can be solved
    - ISSUE_NEXT_STEPS_USABILITY_TRUEORFALSE: Answer True if there is an issue, False otherwise
    - ISSUE_EXPECTED_OUTPUT_JUSTIFICATION: think about the output. Is it going in the right direction? Or is there an issue? If you detect an issue explain it and explain how it can be solved
    - ISSUE_EXPECTED_OUTPUT_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_OTHER_LEVEL_JUSTIFICATION: Is the execution revealing any omittment from the planner or the linker? If you detect an issue explain it and explain how it can be solved
    - ISSUE_OTHER_LEVEL_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_SUMMARY: Provide a brief summary of the issues detected in this step, if any. If no issue was detected, write "". Give all details to be able to correct the issue at next iteration.
    """
    #removed [if there is an issue] that would request interrupting the plan




    prompt_inrun_empty_check="""
    You are a plan execution validator. A planner agent together with a linker agent built a plan to solve the given task: {overall_task}
    The plan is:
    {tentative_plan}

    The tools used in this plan are the following:
    {used_tools}

    For your information, these tools are also available:
    {other_available_tools}

    To execute the current tool, {current_step_name_and_description}, the linker used the following inputs:
    {tool_inputs_and_attributes}
    The execution resulted in an empty output {result_output}

    Is it an actual result or is it due to a misbehavior in the plan? 


    **Use the rules provided with the tools, don't raise an issue out of the blue**

    You should structure your output as a JSON containing a single dictionary answering the following points. Provide the following keys:
    - ORIGINAL_STEP_NAME: repeat the name of the tool used
    - ORIGINAL_STEP_DESCRIPTION: repeat the description given to the tool at this step
    - ISSUE_EMPTY_RESULT_JUSTIFICATION: Is the empty result normal or is it due to a misbehavior in the plan? If you detect an issue explain it and explain how it can be solved
    - ISSUE_EMPTY_RESULT_TRUEORFALSE: Answer True if there is an issue, False otherwise
    - ISSUE_SUMMARY: Provide a brief summary of the issues detected in this step, if any. If no issue was detected, write "". Give all details for the planner to be able to correct the plan."""
    #removed Was some assumption about the data incorrect?
    #removed Is it worth it continuing the plan's execution with the empty results? Or will all following results be empty too?
    #because I fear that then it signals an issue when it is simply an empty results
    #I think it is a waste to continue the plan if everything will be empty, but it implies a different handling if we want to stop it here because the signal should be termination and not an error


    step_nb=current_output['step_number']
    overall_results=current_output['overall_results']
    steps_linking=current_output['steps_linking']
    """check correctness after each step, useful to stop as early as necessary"""
    step_output=recursive_limit_for_dico(overall_results[step_nb]['output'], number_to_display=10)
    step_linking=str(steps_linking[step_nb])
    if step_output==[] or step_output==[[]]:#len(step_output)==0 or len(step_output[0])==0:
        prompt=prompt_inrun_empty_check
        list_ret=["ORIGINAL_STEP_NAME","ORIGINAL_STEP_DESCRIPTION","EMPTY_RESULT_JUSTIFICATION","EMPTY_RESULT_TRUEORFALSE","ISSUE_SUMMARY"]
    else:
        prompt=prompt_inrun_check
        list_ret=["ORIGINAL_STEP_NAME","ORIGINAL_STEP_DESCRIPTION","ISSUE_NEXT_STEPS_USABILITY_JUSTIFICATION","ISSUE_NEXT_STEPS_USABILITY_TRUEORFALSE","ISSUE_EXPECTED_OUTPUT_JUSTIFICATION","ISSUE_EXPECTED_OUTPUT_TRUEORFALSE","ISSUE_OTHER_LEVEL_JUSTIFICATION","ISSUE_OTHER_LEVEL_TRUEORFALSE","ISSUE_SUMMARY"]
    task=current_output['task']
    plan=current_output['plan']
    txt_plan='\n'.join([str(x[0])+'.'+x[1]['name']+'('+x[1]['description']+')' for x in enumerate(current_output['plan'])])
    used_tools_name=[x['name'] for x in plan]
    available_tools_name=[x for x in get_available_tools() if not x in used_tools_name]
    used_tools=get_tool_description( used_tools_name)#, level=['basic','advanced'])
    other_available_tools=get_tool_description( available_tools_name)

    prompt=prompt.format(overall_task=task,tentative_plan=txt_plan,used_tools=used_tools,other_available_tools=other_available_tools,current_step_name_and_description=f"{plan[step_nb]['name']}:{plan[step_nb]['description']}",tool_inputs_and_attributes=step_linking,result_output=step_output)
    # logging.critical("DEBUG PROMPT INRUNCHECK : "+prompt)
    ret_dico=standard_NL2LLM_agent(prompt,list_ret)
    ret_dico[0][0]['original_stepNB']=step_nb
    return ret_dico

def correct_linking_logic(current_output):

    init_prompt="""You are a linking validator. Your task is the following.
    From a task, a planner agent built a plan, and a linker agent links the steps between them.
    Your task is to check if the linking built by the linker makes sense and follows rules and guidelines.

    Given this specific task: {overall_task}
    A planner colleague agent produced this plan for step {step_nb} ({tool_name_and_desc}):
    {tentative_plan}
    Then the linker produced the following linking:
    {current_linking}

    The tools used are the following:
    {used_tools}

    The rules for linking are the following:
    Each element of the list is a dictionary that provides the connection logic from the current step to the next one.
    It contains:
    - `"INPUT_JUSTIFICATION"`: a brief explanation/justification of why you use such source for such key.
    - `"LINKING_RELEVANCE"`: a brief explanation of why this connection is relevant to the task. **Does the selected source contains everything needed?**
    - `"INPUT_SOURCE"`: data from one of the previous step starting by $STEPi$-> with i the step number (e.g., $STEP1$->output, you can get specific items from it such as $STEP1$->output[0]['id'] - if you can source from multiple steps, use the earliest one) or a hardcoded string (surrounded with `#`, e.g. `"#value#"`).
    - `"INPUT_KEY"`: the input field of the next step.
    - If the input/output field is nested (e.g., within a dictionary), represent it using `->` notation (e.g., `"attributes->query"`).
    - Each `INPUT_KEY` should appear only once in your response.

    Is this linking making sense?
    It is assuming the availability of data from a step that does not exist?
    Given the specific characteristics of each used operator, have they been followed rigourously?
    Is the linking done on a step that is not usable in the current state? (For instance, if it needs merging or joining but has not been completed yet)
    Finally, some problems might require to redo a new plan to tackle them, for instance to include a new step. Is the issue requesting a new plan?


    You should structure your output as a JSON containing a single dictionary answering the following points. Provide the following keys:
    - ORIGINAL_STEP_NAME: repeat the name of the tool used
    - ORIGINAL_STEP_DESCRIPTION: repeat the description given to the tool at this step
    - ISSUE_LINKING_LOGIC_JUSTIFICATION: Is the linking lacking logic? Explain here
    - ISSUE_LINKING_LOGIC_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_MISSING_PIECE_JUSTIFICATION: Is the linking basing on a missing piece? Explain here
    - ISSUE_MISSING_PIECE_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_TOOLS_RULES_JUSTIFICATION: Are the rules and requirements of tools followed? Explain here
    - ISSUE_TOOLS_RULES_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_UNUSABLE_INPUT_JUSTIFICATION: Is any input or attributes set in an unusable format? Explain here
    - ISSUE_UNUSABLE_INPUT_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_LEVEL_EXPLANATION: If the issue requires redoing the plan, explain why in this field
    - ISSUE_LEVEL: Answer with single word 'plan' if you think tackling the issue requires a new plan. Otherwise, answer 'linking' to simply redo the linking (prefered).
    - ISSUE_SUMMARY: Provide a brief summary of the issues detected in this step, if any. If no issue was detected, write "". Give all details to be able to correct the issue at next iteration."""
    step_linking=current_output['step_linking']
    task=current_output['task']
    plan=current_output['plan']
    step_nb=current_output['step_number']
    ###DONT GIVE ALL TOOLS JUST CURRENT ONES
    # THEN WE CAN ALSO INTEGRATE COMMON ERROR OR OUTSIDE THIS FCT
    txt_plan='\n'.join([str(x[0])+'.'+x[1]['name']+'('+x[1]['description']+')' for x in enumerate(current_output['plan'])])
    relevant_steps=[]
    for elt in step_linking[0]:
        if "$STEP" in elt["INPUT_SOURCE"]:
            relevant_steps+=[str(elt["INPUT_SOURCE"].split('$STEP')[0].split('$')[0])]

    used_tools_name=[current_output['plan'][step_nb]]+[x['name'] for ix,x in enumerate(plan) if ix in relevant_steps]
    used_tools=get_tool_description( used_tools_name)#, level=['basic','advanced'])
    list_ret=['ORIGINAL_STEP_NAME','ORIGINAL_STEP_DESCRIPTION','ISSUE_LINKING_LOGIC_JUSTIFICATION','ISSUE_LINKING_LOGIC_TRUEORFALSE','ISSUE_MISSING_PIECE_JUSTIFICATION','ISSUE_MISSING_PIECE_TRUEORFALSE','ISSUE_TOOLS_RULES_JUSTIFICATION','ISSUE_TOOLS_RULES_TRUEORFALSE','ISSUE_UNUSABLE_INPUT_JUSTIFICATION','ISSUE_UNUSABLE_INPUT_TRUEORFALSE','ISSUE_LEVEL_EXPLANATION','ISSUE_LEVEL']
    prompt=init_prompt.format(overall_task=task,tentative_plan=txt_plan,current_linking=str(step_linking),used_tools=used_tools,tool_name_and_desc=f"{plan[step_nb]['name']}:{plan[step_nb]['description']}",step_nb=str(step_nb))
    # logging.critical("DEBUG PROMPT INRUNCHECK : "+prompt)
    ret_dico=standard_NL2LLM_agent(prompt,list_ret)
    # logging.critical('DEBUG: DICO RET BEFORE APPEND: '+str(ret_dico))
    ret_dico[0][0]['original_stepNB']=step_nb
    # logging.critical('DEBUG: DICO RET AFTER APPEND: '+str(ret_dico))
    return ret_dico
def post_in_run_check(current_output):
    init_prompt="""You are an issue solver for a planner. A planner agent created a plan to solve a task, a linker connected these steps, and execution occurred.
    Unfortunately, the execution ran into an issue. 
    The following issue has been identified on the result of step {step_number_issue} :
    {issue_found}
    
    Given:
    - The task at hand is :
    {task}

    - The current plan is :
    {current_plan}    

    -The linking made for the tool at that step is (each INPUT_SOURCE is the data to put inside the current tool for the input or attribute INPUT_KEY):
    {linking_step}

    -Tools description are:
    {tools_description}

    Find out if the issue should be managed by a refinement of the plan or by refining the linking.
    Give your output as

    
    
    You should structure your output as a JSON containing a list of dictionaries, one dictionary per step item. For each of this step item, provide the following keys:
    - ISSUE_EXPLANATION: Give reasoning and explanation to justify your choices and help the mitigator that needs to solve the issue. Keep in mind that the planner or linker will use your answer to tackle the issue. **Consider also if the error can be due to a plan logic considering the tools rules**
    - ISSUE_LEVEL: Answer with single word 'plan' or 'linking' depending on the task that needs to be redone
    - RESTART_AT_STEP_NB: if issue comes from linking, specify from which step the linking should be restarted. For instance, if the problem comes from a specific input from an earlier step, and that it is not plan related, it might be that the linking needs to be done again at that step. 
    - ISSUE_SUMMARY: Provide a brief summary of the issues detected in this step, if any. If no issue was detected, write "". Give all details to be able to correct the issue at next iteration."""
    step_number_issue=current_output['step_number_issue']
    issue_found=current_output['issue_found']
    task=current_output['task']
    current_plan=current_output['current_plan'][0]
    linking_step=current_output['linking_step']
    logging.critical('CURRENT PLAN POST IN RUN CHECK:'+str(current_plan))
    used_tools_name=[x['tool'] for x in current_plan]
    tools_description=get_tool_description( used_tools_name)
    prompt=init_prompt.format(step_number_issue=step_number_issue,issue_found=issue_found,task=task,current_plan=current_plan,linking_step=linking_step,tools_description=tools_description)  
    return standard_NL2LLM_agent(prompt,['ISSUE_EXPLANATION','ISSUE_LEVEL','RESTART_AT_STEP_NB'])







def correct_tools_common_issue(current_output):
    prompt_common_operator_errors="""You are a planner validator. Your task is the following.
    Given this specific task: {overall_task}
    A planner colleague agent produced this plan:

    {tentative_plan}


    When using these tools, planner tend to make the following errors:
    {tool_frequent_error}

    Are any of these frequent errors present in this plan?

    You should structure your output as a JSON containing a list of dictionaries, one dictionary per step item. For each of this step item, provide the following keys:
    - ORIGINAL_STEP_NAME: repeat the name of the tool used
    - ORIGINAL_STEP_DESCRIPTION: repeat the description given to the tool at this step
    - ERROR_REASONING: Go over every mentioned error, identify if it applies here
    - ERROR_PRESENCE_TRUEORFALSE: If you found at least one error during the reasoning step, answer True. Else, answer False
    - ERROR_MITIGATION: Summarize the needed action to mitigate the error, otherwise leave empty
    - ISSUE_SUMMARY: Provide a brief summary of the issues detected in this step, if any. If no issue was detected, write "". Give all details to be able to correct the issue at next iteration."""

    return None



def post_execution_check(current_output):
    """check correctness at the end"""
    init_prompt="""You are a global assesser of the plan execution. A planner agent created a plan to solve a task, a linker connected these steps, and execution occurred.
    Your task is to assess the overall quality of the execution and the result.
    The task at hand is :
    {task}
    The plan is :
    {current_plan}
    The linking made for each tool is (each INPUT_SOURCE is the data to put inside the current tool for the input or attribute INPUT_KEY):
    {linking_steps}
    The overall results are:
    {overall_results}  
    The tools used are:
    {tools_description}
    The data available, permitting to determine the tools to use is:
    {data_structure}

    End result:
    {final_result}
    
    
    Now analyze the end result : is there something wrong with it? Maybe one of the tool didn't consider the data structure : for instance, trying to find a value in a column directly not considering the data structure.

    You should structure your output as a JSON containing a single dictionary answering the following points. Provide the following keys:
    - ISSUE_INCORRECT_DATA_CONSIDERATION_JUSTIFICATION: Is the data considered in a format that is not the actual format of the data? Is the description given to the tool too restrictive and consequently removing good elements? Check for each step. If you detect an issue explain it and explain how it can be solved. Look at the output of items, and check if the data is compatible with the usage made of it. If the data is not adapted for the task, maybe the plan should be redone to transform it, if possible? **You should not give your opinion on data but instead look at how the data was used compared to what is in the data structure**
    - ISSUE_INCORRECT_DATA_CONSIDERATION_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_INCORRECT_TOOL_CONSIDERATION_JUSTIFICATION: Is the tools selection actually making sense? Check for each step. If you detect an issue explain it and explain how it can be solved
    - ISSUE_INCORRECT_TOOL_CONSIDERATION_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_MISMATCH_JUSTIFICATION: Is the final result actually answering the task? If you detect an issue explain it and explain how it can be solved
    - ISSUE_MISMATCH_TRUEORFALSE: Answer True if this problem is present, False otherwise
    - ISSUE_LEVEL_EXPLANATION: If the issue requires redoing the plan, explain why in this field. If the linking is enough, explain why. If no issue was detected, write "".
    - ISSUE_LEVEL: Answer with single word 'plan' if you think tackling the issue requires a new plan, 'linking' if redoing the linking between steps enough, and 'none' if no issue was detected.
    - RESTART_AT_STEP_NB: if issue comes from linking, specify from which step the linking should be restarted. Else, put -1
    - ISSUE_SUMMARY: Provide a brief summary of the issues detected in this step, if any. If no issue was detected, write "". Give all details to be able to correct the issue at next iteration. If the issue comes from a specific step, mention it."""

    task = current_output['task']
    plan = current_output['plan']
    linking_steps = current_output['steps_linking']
    overall_results = recursive_limit_for_dico(current_output['overall_results'], number_to_display=20)
    final_result = recursive_limit_for_dico(current_output['final_result'], number_to_display=20)
    if final_result==[] or final_result==[[]]:
        final_result="""[]
        **Since the final result is empty, it might be a normal output OR there was a misuse in one of the tool/data**"""
    else:
        final_result=json.dumps(final_result, indent=2)


    # Format plan for readability
    txt_plan = '\n'.join([f"{i}.{step['name']}({step['description']})" for i, step in enumerate(plan)])

    # Get tool descriptions
    used_tools_name = [step['name'] for step in plan]
    tools_description = get_tool_description(used_tools_name)

    prompt = init_prompt.format(
        task=task,
        current_plan=txt_plan,
        linking_steps=json.dumps(linking_steps, indent=2),
        overall_results=json.dumps(overall_results, indent=2),
        tools_description=tools_description,
        data_structure=data_infos,
        final_result=final_result,
    )

    ret_dico = standard_NL2LLM_agent(
        prompt,
        [
            "ISSUE_INCORRECT_DATA_CONSIDERATION_JUSTIFICATION",
            "ISSUE_INCORRECT_DATA_CONSIDERATION_TRUEORFALSE",
            "ISSUE_INCORRECT_TOOL_CONSIDERATION_JUSTIFICATION",
            "ISSUE_INCORRECT_TOOL_CONSIDERATION_TRUEORFALSE",
            "ISSUE_MISMATCH_JUSTIFICATION",
            "ISSUE_MISMATCH_TRUEORFALSE",
            "ISSUE_LEVEL_EXPLANATION",
            "ISSUE_LEVEL",
            "RESTART_AT_STEP_NB",
            "ISSUE_SUMMARY",
        ],
    )
    return ret_dico


mitigators_execution=[['execution_error',correct_execution_error]]
mitigators_plan=[['general_plan_logic',correct_plan_idea]]
mitigators_linking=[]
mitigators_results_inrun=[]
mitigators_results_post=[['post_execution_check',post_execution_check]]














