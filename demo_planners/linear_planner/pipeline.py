import traceback
from demo_planners.linear_planner.operator_linking import *
from demo_planners.linear_planner.planner import *
# import demo_planners.garbage.linear_planner_multichoices as lpm
from demo_planners.simple_agents_runnable import *
from demo_planners.linear_planner.error_tackling import *
from collections import defaultdict
from time import *
#The plan for the plan is 
# First, we get the initial answer
# So we need a function that takes as input the task, the resilience level (?? I guess I meant iteration number - but this is useless if we have the previous logs), maybe also the difficulty level - if the two are independant and this can be infered by another thing, the abstraction level - can be useful for when we are stuck - in fact maybe these different parameters can be selected dynamically, and it could be a param dico?, 
# I guess it should also take an existing plan in the case of refinement
from datetime import datetime


#We have so much information in data, I guess upon failure we can use them more and more

###OP LINKING OPE
# def convert_plan_from_text_to_opelinking_dict(input_plan):
#     output_plan=input_plan.split('\n')

#     for elt in input_plan:
#         output_plan+=[{'name':, 'description':}]
#     return output_plan 


#Other issue : the linking might be done on useless data. For instance, if we ask to list candidates skills, and then ask a nl2llm for each of them to answer true or false if they are proficient in a programing language, it might be that the first module gives unrelated data such as duration of job posting, but still the next one will state true or false




# A PRE STEP could be to think about a reasoning plan before actually building it. Like the output would be a natural language program
#The second key of json could be a thinking about this program : does it make sense does it forget anything does it consider the structure of the data ...
#Then the third key could be a refinement of the plan using the second key.
#The value for third key could be an additionnal input for the planner

###THERE SHOULD also be probably in the end a step : is te output coherent with what the user asked for. If not, what is the problem. 

import threading
import time
import re

def parse_plan_txt2dict(plan_lines):
    """
    Converts lines like:
    1.NL2SQL("param")
    2.NL2SQL("param2")
    Into a list of dicts with 'index', 'function', and 'parameter'
    """
    result = []
    for line in plan_lines:
        match = re.match(r'(\d+)\.(\w+)\((.*)\)', line.strip())
        if match:
            idx, func, param = match.groups()
            # Remove surrounding quotes from param if present
            param = param.strip()
            if param.startswith('"') and param.endswith('"'):
                param = param[1:-1]
            # result.append({
            #     'index': int(idx),
            #     'function': func,
            #     'parameter': param
            # })
            result.append({
                'name': func,
                'description': param
            })
    return result


def parse_plan_dict2dict(plan_dict):
    res=[{'name':elt['tool'],'description':elt['tool_task']} for elt in plan_dict[0]]
    return res


# def get_overall_linking(plan_steps,abort_trigger):
    
#     steps_linking, ancestor_dico= execute_linking(plan_steps, global_task)
#     return steps_linking, ancestor_dico




def operator_linking_refiner():
    
    return

def execute_linking_wrapper(plan_steps, task, abort_trigger,ancestor_dico,steps_linking,orphans, threads, next_direction_set,dico_error_detection,lock,from_step_X_with_refinement,issue_expl):
    execute_linking(plan_steps, task, abort_trigger,ancestor_dico,steps_linking,threads,next_direction_set,dico_error_detection,lock, from_step_X_with_refinement,issue_expl)
    logging.critical("Linking: Linking over, executing orphans detection")

    if abort_trigger.is_set():
        return
    try:
        detect_orphans(ancestor_dico, orphans)
        if len(orphans)>1:
            logging.critical('! Linking: Orphans found:'+str(orphans)+' (includes last node that is not an orphan)')
            #TODO: refinement steps
        logging.critical('Linking: Orphans detection finished')
    except Exception as e:
        logging.critical('Linking: Orphan detection failed, for some reason. Some reason:' + str(e)+ (traceback.format_exc() if 'traceback' in globals() else None) +', ancestor dico : '+ str(ancestor_dico))
def operator_linking_construction_and_task_run(plan_steps,task,abort_trigger,steps_linking, orphans, output,ancestor_dico,lock,threads,next_direction_set,dico_error_detection, from_step_X_with_refinement,issue_expl):
    # logging.critical("Interpreting linking results")
    t0 = threading.Thread(
        target=execute_linking_wrapper,
        args=(plan_steps, task, abort_trigger,ancestor_dico,steps_linking,orphans,threads, next_direction_set,dico_error_detection,lock, from_step_X_with_refinement,issue_expl)
    )
    t0.start()
    threads.append(t0)
    # steps_linking=steps_linking[0]
    # logging.critical('=======get_overall_linking==========')
    # logging.critical(steps_linking, ancestor_dico)
    # logging.critical(type(steps_linking))
    #As this stage we could also run the basics verification such as orphan
    #This and other verification could be launched simultaneously to the 



    #####0826: TO ADD : ONCE LINKING FINISH LAUNCH THIS: SHOULD BE A THREAD
    
    # threads.append(t) # I don't think we need it
    #Execution
    logging.critical("Executing plan from linking")
    execute_plan_from_linking(plan_steps,steps_linking,ancestor_dico,output,lock,threads,next_direction_set,dico_error_detection,abort_trigger,task,from_step_X_with_refinement)
    # return {'steps_linking':steps_linking, 'orphans':orphans},output
    #Upon failure now that we have the real results maybe we could run another type of operator linking, first one would be plan-based and this one would be results-based


def tool_linking_plan_to_values(ope_linking_result,results_so_far,step_at_hand,abort_trigger,dico_error_detection,next_direction_set, lock , plan):
    # logging.critical('='*10,'tool_linking_plan_to_values','='*10,'\n',recursive_limit_for_dico(results_so_far,3))
    input_tmp=[[]]
    attributes_tmp={}
    properties_tmp={}
    logging.critical("Linking: Linking from LLM result number "+str(step_at_hand)+", len linking is "+str(len(list(ope_linking_result.keys()))))
    # logging.critical('!!!!!!!ope_linking_result!!!!', ope_linking_result)
    # logging.critical("=======+DEBUG OPE LINKING RES:"+str(ope_linking_result))
    all_keys=[x['INPUT_KEY'] for x in ope_linking_result[step_at_hand]]
    if not len(all_keys)==len(set(all_keys)):
        # raise Exception("You cannot have multiple identical INPUT_KEY")
        level = IssueLevel.execution_level
        name_mitigator = 'LinkingExecutionError'
        tool_name = plan[step_at_hand]['name'] if plan and step_at_hand < len(plan) else None
        tool_desc = plan[step_at_hand]['description'] if plan and step_at_hand < len(plan) else None
        error_info = [{
            "original_stepNB": str(step_at_hand),
            "tool_name": tool_name,
            "tool_description": tool_desc,
            "exception_message": "You cannot have multiple identical INPUT_KEY. Maybe an intermediate step is missing? Linking gave the following result for this step : "+json.dumps(ope_linking_result),
        }]
        logging.critical('Linking: ! Error as multiple implementation of the same key')
        summary_issue = json.dumps(error_info, indent=2)
        mitigator_result = summary_issue  # or add more details if needed
        with lock:
            if name_mitigator not in dico_error_detection[level]:
                dico_error_detection[level][name_mitigator] = {}
            dico_error_detection[level][name_mitigator][str(step_at_hand)] = {
                'summary_issue': summary_issue,
                'full_output': mitigator_result
            }
            next_direction_set.add(level)
            dico_error_detection[level]['issue_path']=','.join([name_mitigator,str(step_at_hand)])

        abort_trigger.set()
        return
    for elt in ope_linking_result[step_at_hand]:
        content=elt['INPUT_SOURCE']
        
        if '#' in content:
            content=content.split('#')
            new_content=[]
            #To avoid bad splitting, could be smarter to find another symbol than #. If it is in the middle of a text, we consider the # to not count
            # logging.critical('content',content)
            for ielttmp,elttmp in enumerate(content):
                if ielttmp==0:
                    new_content+=[elttmp]
                elif len(content[ielttmp-1])>0 and len(elttmp)>0 and(elttmp[0].isalnum() or  content[ielttmp-1][-1].isalnum()):
                    new_content[-1]+='#'+elttmp
                else:
                    new_content+=[elttmp]
            content=new_content
            # logging.critical('new_content',new_content)
            assert len(content)==3
            assert content[0]==content[2]==''
            ###ERROR SHOULD PROBABLY BE USED TO SAY WHAT WENT WRONG #TODO
            content=content[1]
            if content[0]==content[-1]=="'":
                content=content[1:-1]
        elif '$STEP' in content:
            content=content.split('$STEP')
            assert len(content)==2
            assert content[0]==''
            content=content[1].split('$->')
            content[0]=int(content[0])
            if content[1]=='output':
                # if content[0]==0:
                #     content=[[]]
                # else:
                content=results_so_far[content[0]]['output']
            elif content[1].startswith('output'):
                content=get_nested_value(results_so_far[content[0]]['output'], content[1][6:])
                logging.critical("Linker: LLM used nested list to get INPUT_SOURCE - getting a sub list of a previous output")
            else:
                raise Exception('What is that?')
        ##NOW WE HAVE the value, let's put it in the right spot

        destination=elt['INPUT_KEY']
        if destination=='input':
            input_tmp=content
        elif destination.startswith('input'):
            
            input_tmp=set_nested_value(input_tmp,content, destination[5:])
            logging.critical("Linker: LLM used nested list to define INPUT_KEY - probably avoiding append")
        elif destination.startswith('attributes->'):
            attributes_tmp[destination[12:]]=content
        else:
            raise Exception("The linking mentions a destination that neither is an input neither starts by attributes->. Refine this:"+destination)
        
    # logging.critical('debug:',json.dumps({'input':input_tmp,'attributes':attributes_tmp},indent=4))
    return input_tmp, attributes_tmp, properties_tmp



####PLAN OPE

def get_plan_aio(task:str,previous_logs=[], special_task={'addition':1},tools_list=['JOIN_2','SELECT','NL2LLM','ROWWISE_NL2LLM','NL2SQL']):
    """special task contains additionnal information needed, for instance upon iteration if there was a failure
    CoTF can be about both meta operator and global one"""
    #0828 : added, might need to be removed: To solve the issue, modifying a specific step might not be enough, and there are cases where you need to reorder or completely redo the plan. 
    #added number 2 : But it is not always the case! Consider the history of your mistake to decide
    error_mitigation="""
    
    **IMPORTANTLY, you were already executed on this task, and produced the following plans:
    {previous_iterations}
    Please build a new plan considering your previous mistakes, and minding to not produce a similar one. To solve the issue, modifying a specific step might not be enough, and there are cases where you need to reorder or completely redo the plan. But it is not always the case! Consider the history of your mistake to decide
    **For each step, in the reason field justifying the relevance of a step, EXPLAIN why the step you propose is adequate considering the previously observed errors**"""

    iteration="""####ITERATION {iteration_nb}
    
    {previous_plan} 
    It was detected that this plan has the following issue : 
    {issue_detected}
    
    """

    
    if len(previous_logs)>0 and previous_logs[-1]['next_direction']==IssueLevel.plan_level:
        # logging.critical('DEBUG: PLAN WITH REFINEMENT')
        previous_iterations=""
        iteration_nb=0
        for elt in previous_logs:
            if not elt['next_direction']==IssueLevel.plan_level:
                #If the issue was not the plan should we still display it here?
                continue
            previous_iterations+=iteration.format(iteration_nb=str(iteration_nb),previous_plan=get_plan_text(elt['raw_plan']),issue_detected=elt['issue_summary_next_step'])
            iteration_nb+=1
        error_mitigation=error_mitigation.format(previous_iterations=previous_iterations)
        # logging.critical('DEBUG: error_mitigation for plan'+str(error_mitigation))
        return get_plan(task,error_mitigation, special_task,tools_list=tools_list)
    else:
        return get_plan(task,special_task,tools_list=tools_list)


####EXECUTION STUFFS
def execute_plan_from_linking(plan,steps_linking,ancestor_dico,overall_results,lock,threads,next_direction_set,dico_error_detection,abort_trigger,task,from_step_X_with_refinement):
    # overall_results=defaultdict(dict)
    # for step, linking in zip(parse_plan_dict2dict(plan_steps),steps_linking):
    # lock = threading.Lock()
    # threads = []
    for step_number in range(max(1,from_step_X_with_refinement),len(plan)):
        logging.critical('Execution: preparing to start step '+str(step_number))
        # needed_steps_results=ancestor_dico[step_number]
        NLTool=plan[step_number]['name']
        t = threading.Thread(
            target=execute_step,
            args=(NLTool, steps_linking, overall_results, ancestor_dico, step_number, lock,next_direction_set,dico_error_detection,abort_trigger,threads,task,plan)
            #TODO: isn't it a bit dangerous to add threads to parameter of a thread?
        )
        t.start()
        threads.append(t)



    #PB: at the beginning the steps linking is not ready so...
    # for step_number, (planstep,linking) in enumerate(zip(plan[1:],steps_linking),start=1):
    #     logging.critical('Execution: preparing to start step '+str(planstep))
    #     needed_steps_results=ancestor_dico[step_number]
    #     NLTool=planstep['name']
    #     t = threading.Thread(
    #         target=execute_step,
    #         args=(NLTool, linking, overall_results, needed_steps_results, step_number, lock)
    #     )
    #     t.start()
    #     threads.append(t)

    #not needed
    # for t in threads:
    #     t.join()
    # return overall_results


def isReadyToRun(overall_results, step_number, ancestor_dico,linking):
    """check if the needed results from other steps are available and we have the linking done for our step number"""
    if not step_number in ancestor_dico.keys():
        logging.critical('Execution: Step '+str(step_number)+' is waiting for ancestor list for this step. Step with list of ancestors available:'+str(list(ancestor_dico.keys())))#str(",".join(list(ancestor_dico.keys()))))
        return False
    # logging.critical('Execution: Step '+str(step_number)+' has its ancestor list, proceeding.')
    needed_steps_results=ancestor_dico[step_number]
    for elt in needed_steps_results:
        if elt not in overall_results.keys():
            # logging.critical("CHECKING "+str(type(overall_results)))
            # logging.critical("CHECKING 2 "+str(",".join(list(overall_results.keys()))))
            logging.critical('Execution: Step '+str(step_number)+' is waiting for results from steps '+str(needed_steps_results)+' to start. Steps results available:'+str(list(overall_results.keys())))#str(",".join(list(overall_results.keys()))))
            return False
    if step_number not in linking.keys():
        logging.critical('Execution: Step '+str(step_number)+' is waiting for linking for this step. Steps linking available:'+str(list(linking.keys())))
        #Should be equivalent to first if
        return False
    return True
def execute_step(NLTool,linking, overall_results, ancestor_dico, step_number,lock,next_direction_set,dico_error_detection,abort_trigger,threads,task,plan):
    logging.critical('Execution: Step '+str(step_number)+' called.')
    while not isReadyToRun(overall_results, step_number, ancestor_dico, linking):
        if abort_trigger.is_set():
            logging.critical('Execution: Step '+str(step_number)+' aborting as abort trigger is set.')
            return
        sleep(2)
    needed_steps_results=ancestor_dico[step_number]
    if max(needed_steps_results+[0])+1<step_number:
        logging.critical('Execution: Optimization : Step '+str(step_number)+' started running at position '+str(max(needed_steps_results+[0])+1)+'.')
    logging.critical('Execution: Step '+str(step_number)+' running.')
    try:
        out,inp, attributes,properties=execute_step_sub(NLTool,linking, overall_results,step_number,abort_trigger,dico_error_detection,next_direction_set, lock,plan)
    # ...existing code...
    except Exception as e:
        logging.critical('Execution: ! Error executing : going to next .refine() for correction')
        logging.critical('Execution: ! Error  :'+ traceback.format_exc() if 'traceback' in globals() else None)
        level = IssueLevel.execution_level
        name_mitigator = 'ErrorTraceback'
        tool_name = plan[step_number]['name'] if plan and step_number < len(plan) else None
        tool_desc = plan[step_number]['description'] if plan and step_number < len(plan) else None
        error_info = {
            "original_stepNB": step_number,
            "tool_name": tool_name,
            "tool_description": tool_desc,
            "exception_type": type(e).__name__,
            "exception_message": str(e),
            "traceback": traceback.format_exc() if 'traceback' in globals() else None
        }
        summary_issue = json.dumps(error_info, indent=2)
        mitigator_result = summary_issue  # or add more details if needed
        with lock:
            if name_mitigator not in dico_error_detection[level]:
                dico_error_detection[level][name_mitigator] = {}
            dico_error_detection[level][name_mitigator][str(step_number)] = {
                'summary_issue': summary_issue,
                'full_output': mitigator_result
            }
        next_direction_set.add(level)
        dico_error_detection[level]['issue_path']=','.join([name_mitigator,str(step_number)])

        abort_trigger.set()
        return

        # with lock:
        #     if name_mitigator not in resdico[level]:
        #         resdico[level][name_mitigator] = {}
        #     resdico[level][name_mitigator][current_nb]={'summary_issue':summary_issue,'full_output':mitigator_result}
        # if presence_issue:
        #     # logging.critical('Error detection: Stop event occurred, disabled for debug, put back!!!!')
        #     stop_event.set()
        #     logging.critical('! Error detection : '+name_mitigator + ' at level '+level+' ended with issue.')
        #     # remove the # !
        #     with lock:
        #         if not 'ISSUE_LEVEL' in mitigator_result[0][0].keys() :
        #             next_direction_set.add(level)
        #         else:

        #             new_level={'plan':IssueLevel.plan_level, 'linking':IssueLevel.linking_level}[mitigator_result[0][0]['ISSUE_LEVEL']]
        #             if not new_level==level:
        #                 logging.critical('! Error detection: mitigator changed error level from '+level+' to '+new_level+'.')
        #             next_direction_set.add(new_level)
        #             if name_mitigator not in resdico[new_level]:
        #                 resdico[new_level][name_mitigator] = {}
        #             resdico[new_level][name_mitigator][current_nb]={'summary_issue':summary_issue,'full_output':mitigator_result}
        #             resdico[new_level]['issue_path']=','.join([name_mitigator,current_nb])
        #         resdico[level]['issue_path']=','.join([name_mitigator,current_nb])



    if out==[]:
        out=[[]]
    if not type(out[0])==list:
        out=[out]
    with lock:
        overall_results[step_number]= {'output':out, 'tool_input_and_attributes':'input:\n'+str(inp)+'\nattributes:\n'+str(attributes)+'\nproperties:\n'+str(properties)}
    logging.critical('Execution: Step '+str(step_number)+' finished running.')
    for name_mitigator,mitigator in mitigators_results_inrun:
            # dico_error_detection[name_mitigator]=dict()
            t = threading.Thread(target=run_error_detection, args=(mitigator, {'task':task,'plan':plan,'overall_results':overall_results,'steps_linking':linking,'step_number':step_number}, next_direction_set,dico_error_detection,IssueLevel.in_run_verification,name_mitigator, abort_trigger,lock))
            t.start()
            threads.append(t)

def execute_step_sub(NLTool,ope_linking_result, results_so_far,step_number,abort_trigger,dico_error_detection,next_direction_set, lock,plan):
    """results so far be the outputs of other steps. ope linking result contains everything else"""
    inp, attributes, properties= tool_linking_plan_to_values(ope_linking_result,results_so_far,step_number,abort_trigger,dico_error_detection,next_direction_set, lock,plan)
    result=NL_to_RUN(NLTool, inp, attributes, properties)
    return result, inp, attributes,properties





###GENERAL

# def issue_detection():
#     """3 levels. I dont know if the first one should be handled there. the first one should be auto detection such as orphans. i dont think it make sense to wait the issue detection to state this
#     I think a lot of stuffs for this step can be rule based
#     The second level is execution problem - including detecting if empty results is a problem or not
#     The other level is detection by LLM I think - but that is maybe more a consistency check. And it should maybe both come before and after the plan execution"""
#     return



def get_next_direction(next_direction_set):
    if IssueLevel.execution_level in next_direction_set:
        return IssueLevel.execution_level
    #useless to try to improve connection if the plan is problematic
    elif IssueLevel.plan_level in next_direction_set:
        return IssueLevel.plan_level
    #an execution issue can happen before the linking could be verified for this step. So we prefer to deal of the linking first
    #it doesn't take into consideration the fact that it could be from different steps, but for now we prefer this solution
    #plus even if it comes from two different places, asking for two corrections in a single step might be too much complication
    elif IssueLevel.linking_level in next_direction_set:
        return IssueLevel.linking_level
    
    elif IssueLevel.in_run_verification in next_direction_set:
        return IssueLevel.in_run_verification
    #Anyway there is little chance we reached post verification if there was another problem
    #Except for problem of logic from plan, but they can be checked from the second LLM execution wave, way before the end verification
     
    elif IssueLevel.post_verification in next_direction_set:
        return IssueLevel.post_verification
    else:
        return IssueLevel.post_verification
    

def build_summary_from_errors(dico_error_detection, next_direction):
    summary='Module {name_module} found the following problem in the specified step(s): \n{problem}'
    if next_direction in [IssueLevel.none,IssueLevel.post_verification]:
        return ['',-1]
    ###We correct the first found error in the category
    # logging.critical("!!!!DEBUG DICO ERROR DETECTION!!!!"+str(dico_error_detection))
    
    # module_name=list(dico_error_detection[next_direction].keys())[0]
    [name_mitigator,current_nb]=dico_error_detection[next_direction]['issue_path'].split(',')
    # if not current_nb in dico_error_detection[next_direction][name_mitigator].keys():
    #     logging.critical('DEBUG : dico_error_detection[next_direction][name_mitigator]'+str(dico_error_detection[next_direction][name_mitigator]))
    summary=summary.format(name_module=name_mitigator,problem=dico_error_detection[next_direction][name_mitigator][current_nb]['summary_issue'])
    possible_keys=[x for x in ['RESTART_AT_STEP_NB','original_stepNB'] if x in dico_error_detection[next_direction][name_mitigator][current_nb].keys()]
    step=dico_error_detection[next_direction][name_mitigator][current_nb][possible_keys[0]] if len(possible_keys)>0 else -1
    return summary, step


def from_task_to_result(task:str, previous_logs=[], tools_list=['JOIN_2','SELECT','NL2LLM','ROWWISE_NL2LLM','NL2SQL'])->list[dict]:
    next_direction_set=set()
    #logs should probably contain : trace of results at different iteration (And the precise modification or prompt that lead there), and execution trace
    current_log={'task':task, 'next_direction':'termination'}

    #{'result':'', 'path_followed':'', 'issue':'', , 'CoTF':[] , 'summary_op':'',}
    #V0: for now lets not do any correction
    #three steps :
    #get plan :
    abort_trigger = threading.Event()
    lock=threading.Lock()
    #issue_summary_next_step=''
    #No need for threading at this stage: nothing can be done without plan

    #If 5 consecutive errors of plan, we restart from scratch


    if len(previous_logs)>0 and previous_logs[-1]['next_direction']==IssueLevel.post_verification:
        logging.critical('Post verification: Post verification started.')
        task = previous_logs[-1]['task']
        plan = previous_logs[-1]['plan']
        linking_steps = previous_logs[-1]['operators_linking']
        overall_results = recursive_limit_for_dico({x:previous_logs[-1]['steps_results'][x]['output'] for x in previous_logs[-1]['steps_results'].keys()}, number_to_display=20)
        last_idx=[x for x in previous_logs[-1]['steps_results'].keys()][-1]
        final_result = recursive_limit_for_dico(previous_logs[-1]['steps_results'][last_idx]['output'], number_to_display=20)
        new_issue_desc=post_execution_check({'task':task,'plan':plan,'steps_linking':linking_steps,'overall_results':overall_results,'final_result':final_result})
        logging.critical("debug new_issue_desc:"+json.dumps(new_issue_desc,indent=2))
        new_issue_desc=new_issue_desc[0] if type(new_issue_desc)==list else new_issue_desc
        new_issue_desc=new_issue_desc[0] if type(new_issue_desc)==list else new_issue_desc

        previous_logs[-1]['next_direction']={'plan':IssueLevel.plan_level, 'linking':IssueLevel.linking_level,'none':IssueLevel.none}[new_issue_desc['ISSUE_LEVEL'].lower()]
        previous_logs[-1]['issue_summary_next_step']=('The following problem was found \n'+json.dumps(new_issue_desc['ISSUE_SUMMARY'])) if len(str(new_issue_desc['ISSUE_SUMMARY']))>5 else ''
        previous_logs[-1]['issue_step_nb']=new_issue_desc['RESTART_AT_STEP_NB']
        if previous_logs[-1]['next_direction']==IssueLevel.none:
            logging.critical('Post verification: No issue found, terminating.')
        else:
            logging.critical('Post verification: Issue found, going to '+str(previous_logs[-1]['next_direction'])+' at step '+str(previous_logs[-1]['issue_step_nb'])+'.')
        # previous_logs+=[current_log]
        return previous_logs
        

    # if len(previous_logs)>0 and [elt['next_direction']==IssueLevel.plan_level for elt in previous_logs[-10:]]==[True]*10:
    #     logging.critical('General: 10 consecutive errors from planning, restarting from scratch.')
    #     return []
    if len(previous_logs)>5 and not IssueLevel.plan_level in [elt['next_direction'] for elt in previous_logs[-5:]]:
        #TODO: MAYBE ADD A DESCRIPTION TO SAY THAT LINKING FAILED MULTIPLE TIMES SO HOW TO MODIFY THE PLAN WHY THE SPECIFIC STEP FAILS############################################################################################
        logging.critical('General: 5 consecutive steps with no plan refining. Refining plan')
        previous_logs[-1]['next_direction']=IssueLevel.plan_level
    if len(previous_logs)>0 and previous_logs[-1]['next_direction'] in[IssueLevel.in_run_verification, IssueLevel.execution_level]: 
        # logging.critical('previous_logs[-1][issue_summary_next_step]:'+str(previous_logs[-1]['issue_summary_next_step']))
        # issue_summary=previous_logs[-1]['issue_summary_next_step']
        # logging.critical('error_in_json:'+json.dumps(error_in_json,indent=2))
        step_number_issue=previous_logs[-1]['issue_step_nb']#error_in_json['original_stepNB']
        # previous_logs+=[previous_logs[-1]]
        #In fact why keeping both, it justs create a lot of noise
        #Better changing the value of last one
        new_issue_desc=post_in_run_check({'step_number_issue':step_number_issue,'issue_found':previous_logs[-1]['issue_summary_next_step'],'task':previous_logs[-1]['task'],'current_plan':previous_logs[-1]['raw_plan'],'linking_step':previous_logs[-1]['operators_linking'][step_number_issue]})[0][0]
        # ['ISSUE_EXPLANATION','ISSUE_LEVEL','RESTART_AT_STEP_NB']
        previous_logs[-1]['next_direction']={'plan':IssueLevel.plan_level, 'linking':IssueLevel.linking_level}[new_issue_desc['ISSUE_LEVEL']]
        previous_logs[-1]['issue_summary_next_step']='The following problem was found \n'+json.dumps(new_issue_desc)
        previous_logs[-1]['issue_step_nb']=new_issue_desc['RESTART_AT_STEP_NB']
        return previous_logs


    dico_error_detection = defaultdict(dict)
    threads=[]
    if len(previous_logs)==0 or previous_logs[-1]['next_direction']==IssueLevel.plan_level:
        start_time = time.perf_counter()
        logging.critical('Planning: Planning task started.')
        plan_steps=get_plan_aio(task,previous_logs,tools_list=tools_list)
        plan_time_plan= time.perf_counter() - start_time   
        # logging.critical('Planning: fake plan, remember to put back the real one')
        # plan_steps=json.loads("""[[{"step_number": 1, "reason": "To address the issue of mismatched job titles, we first need to identify job titles that require proficiency in programming languages. This step is crucial as it sets the foundation for subsequent steps by ensuring we have the correct job titles. We will use the 'frequent_skills_by_title' table to find job titles associated with programming languages.", "tool": "NL2SQL", "tool_task": "Select distinct 'short_job_title' from 'frequent_skills_by_title' where 'skill_required' includes programming languages such as 'Python', 'Java','C++', 'JavaScript', or 'SQL'."}, {"step_number": 2, "reason": "To ensure we have the correct salary information, we need to retrieve salary data for all job titles. This step is necessary to prepare for a join operation that will match job titles with their corresponding salaries.", "tool": "NL2SQL", "tool_task": "Select 'short_job_title', 'avg_min_salary' from 'avg_min_salary_by_title'."}, {"step_number": 3, "reason": "To resolve the issue of mismatched job titles and duplicates, we will perform a join operation on the distinct job titles from step 1 and the salary data from step 2. This step ensures that we only consider job titles that require programming skills and have corresponding salary data.", "tool": "JOIN_2", "tool_task": "Join the results from step 1 and step 2 on 'short_job_title'."}, {"step_number": 4, "reason": "To find the maximum salary among the job titles that require programming skills, we need to filter the joined data to identify the record with the highest salary. This step is crucial to fulfill the main task requirement.", "tool": "SELECT", "tool_task": "Select the record with the maximum 'avg_min_salary' from the joined data."}]]""")
        # logging.critical('=======RAW PLAN STEPS==========')
        # logging.critical(plan_steps)
        current_log['raw_plan']=plan_steps
        current_log['plan_time_plan']=plan_time_plan
        logging.critical('Planning: Planning task ended.')
        if not plan_steps[0][0]['tool']=='START':
            plan_steps[0]=[{'step_number':0, 'tool':'START', 'tool_task':''}]+plan_steps[0] #WE NOW INCLUDE THE START STEP THAT IS FOR REASONING #REMOVED
       
        # issue="" 
        # for i_p,plan_step in enumerate(plan_steps[0][1:],start=1):
        #     plan_step_fct=plan_step['tool']
        #     logging.critical('Instant rule-based plan check: checking '+plan_step_fct)
            
        #     if plan_step_fct in instant_check_fcts:
        #         predecessors=[x['tool'] for x in plan_steps[0][:i_p]]
        #         successors=[x['tool'] for x in plan_steps[0][i_p+1:]]
        #         tmp=instant_check_fcts[plan_step_fct](predecessors,successors)
        #         if (tmp is not None ) and len(tmp)>0:
        #             issue+=tmp+'\n'
        #             logging.critical('Instant rule-based plan check: !Issue  on '+plan_step_fct+':'+tmp)
        # if len(issue)>0:
        #     current_log['next_direction']=IssueLevel.plan_level
        #     current_log['issue_summary_next_step']='**You need to address the following issues in the plan**\n'+issue
        #     previous_logs+=[current_log]
        #     return previous_logs
        # TODO: refine instant check so that it works
            
        plan_steps=parse_plan_dict2dict(plan_steps)
        current_log['plan']=plan_steps
        logging.critical('Planning: checking the plan...')
        for name_mitigator,mitigator in mitigators_plan:
                
                # dico_error_detection[name_mitigator]=dict()
                t = threading.Thread(target=run_error_detection, args=(mitigator, current_log, next_direction_set,dico_error_detection,IssueLevel.plan_level,name_mitigator, abort_trigger,lock))
                t.start()
                threads.append(t)
    else:
        logging.critical('Planning: sticking to last round plan')
        plan_steps=previous_logs[-1]['plan']    
        current_log['raw_plan']=previous_logs[-1]['raw_plan']
        current_log['plan']=plan_steps
        
        
    
    #We loose the justificatiom right above. Could be useful for refining but for now lets keep like this. #TODO
    logging.critical('=======PLAN STEPS==========')
    logging.critical(plan_steps)
    #Now that we have the plan, we can run the operator linking
    steps_linking=defaultdict(list)
    orphans=[]
    ancestor_dico=defaultdict(dict)
    #ope_linking, ope_run_output=
    ope_run_output=defaultdict(dict)
    ope_run_output[0]['output']=[[]]
    issue_expl=None

    if len(previous_logs)>0 and previous_logs[-1]['next_direction']==IssueLevel.linking_level:
        # logging.critical('DEBUG ERROR JSON:'+previous_logs[-1]['issue_summary_next_step'])
        issue_expl=previous_logs[-1]['issue_summary_next_step']
        restart_at_step_nb=previous_logs[-1]['issue_step_nb']
        steps_linking={k:v for k,v in previous_logs[-1]['operators_linking'].items() if int(k)<int(restart_at_step_nb)}
        ancestor_dico={k:v for k,v in previous_logs[-1]['ancestor_dico'].items() if int(k)<int(restart_at_step_nb)}
        ope_run_output={k:v for k,v in previous_logs[-1]['steps_results'].items() if int(k)<int(restart_at_step_nb)}
        from_step_X_with_refinement=restart_at_step_nb
    else:
        from_step_X_with_refinement=-1
    operator_linking_construction_and_task_run(plan_steps, task, abort_trigger,steps_linking, orphans, ope_run_output,ancestor_dico,lock,threads,next_direction_set,dico_error_detection, from_step_X_with_refinement,issue_expl)
    # (plan_steps,task,abort_trigger
    
    #This doesn't look very parallel nor like the cascade I want, maybe I should do this right now
    #METAOPERATOR PROBLEM DETECTION:
    
    # for mitigators_level,next_direction_label in [[mitigators_linking,'linking_issue_mitigation'],[mitigators_plan,'plan_issue_mitigation'],[mitigators_execution,'execution_issue_mitigation']]:
    #     if not next_direction=='termination':
    #         break
    #     for mitigator in mitigators_level:
    #         mitigator_result=mitigator(previous_logs)
    #         presence_issue,summary_issue=detect_issue(mitigator_result)
    #         if presence_issue:
    #             next_direction=next_direction_label
    #             issue_summary_next_step=summary_issue
    
    

    #summary op can be kind of a summary of what was tried and what happened, so it can be used for next iteration if needed
    # for t in threads:
    #     t.join()

    while any(t.is_alive() for t in threads):
        if abort_trigger.is_set():
            # logging.critical('abort trigger')
            time.sleep(5)
            break
        time.sleep(1)  # give threads time
        # logging.critical('not abort')

    current_log['operators_linking']=steps_linking
    current_log['steps_results']=ope_run_output

    current_log['ancestor_dico']=ancestor_dico
    current_log['dico_error_detection']=dico_error_detection
    current_log['next_direction']=get_next_direction(next_direction_set)
    current_log['issue_summary_next_step'], current_log['issue_step_nb']=build_summary_from_errors(dico_error_detection, current_log['next_direction'])
    # logging.critical('DEBUG : next_direction is '+current_log['next_direction']+', list was'+str(next_direction_set))

    current_log['plan_tree'] = build_plan_tree(plan_steps, ancestor_dico)
    plan_tree_text = "\n".join(plan_tree_to_text(current_log['plan_tree']))
    current_log['plan_tree_text'] = plan_tree_text
    logging.critical('Plan tree (text):\n' + plan_tree_text)
    logging.critical('Plan tree: Plan tree for this round was'+json.dumps(current_log['plan_tree'], indent=4))
    previous_logs+=[current_log]
    #next_direction can be termination, because we can have empty result but not need to retry again
    return previous_logs

def plan_tree_to_text(tree, indent=0):
    """Recursively pretty-print the plan tree as text."""
    lines = []
    for node in tree:
        prefix = '  ' * indent
        lines.append(f"{prefix}- {node['tool']}: {node['description']}")
        if node['children']:
            lines.extend(plan_tree_to_text(node['children'], indent + 1))
    return lines
def build_plan_tree(plan, ancestor_dico):
    """
    Build a tree from a linear plan and its ancestor dictionary.
    Each node is a dict: {'step': step_idx, 'tool': ..., 'description': ..., 'children': [...]}
    """
    nodes = []
    for idx, step in enumerate(plan):
        nodes.append({
            'step': idx,
            'tool': step['name'],
            'description': step['description'],
            'children': []
        })
    # Build parent-child relationships
    for idx, ancestors in ancestor_dico.items():
        for anc in ancestors:
            nodes[anc]['children'].append(nodes[idx])
    # Find root(s): steps with no ancestors
    all_children = set(sum(ancestor_dico.values(), []))
    roots = [node for idx, node in enumerate(nodes) if idx not in all_children]
    return roots


def general_execute_task(task:str,tools_list=['JOIN_2','SELECT','NL2LLM','ROWWISE_NL2LLM','NL2SQL']):
    """simple loop handler to get a plan and execute it. Acts at the plan level to refine the plan (not linking)"""
    logs=[]
    round=0
    
    # current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    # filename = f"demo_planners/logs_planner/tmp_{current_time_str}.json"
    # logging.critical(f'General: Logs of the different rounds of .refine() will be saved in {filename}')
    while len(logs)==0 or not logs[-1]['next_direction']=='termination':
        logging.critical('Global : Round '+str(round)+' of .refine() started')
        logs=from_task_to_result(task,logs, tools_list=tools_list)
        round+=1
        # f=open(filename,'w')
        # json.dump(recursive_limit_for_dico(logs,5),f, indent=4)
        # f.close()
    
    #OLD TESTING STUFF
    # f=open('demo_planners/stuffs/jsondumplogs1.json','r')
    # logs=json.load(f)
    # f.close
    # logging.critical('====================CORRECT PLAN IDEA============================')
    # logging.critical(json.dumps(correct_plan_idea(task,logs[0]['plan']),indent=4))
    
    return logs



if __name__ == "__main__":
    # global_task="Find the maximum salary among people who should have proficiency in at least one programming language."

    global_task="What jobs are available for data scientists in the bay area?"
    output=general_execute_task(global_task, tools_list=['JOIN_2','SELECT','NL2LLM','ROWWISE_NL2LLM','NL2SQL'])
    # logging.critical(type(output))
    index_last_step=output[-1]['steps_results'].keys()
    index_last_step=[int(x) for x in index_last_step if str(x).isdigit()]
    index_last_step=max(index_last_step) if len(index_last_step)>0 else 0
    logging.critical('====================OUTPUT END============================')
    logging.critical(output[-1]['steps_results'][index_last_step]['output'] if index_last_step in output[-1]['steps_results'].keys() else 'No step executed') 

    logging.critical(output[-1]['plan_tree'][index_last_step]['plan_tree'] if index_last_step in output[-1]['plan_tree'].keys() else 'No plan to see') 