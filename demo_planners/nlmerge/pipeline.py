from demo_planners.linear_planner.pipeline import *
from demo_planners.nlmerge.planner_linker import *
from demo_planners.nlmerge.runner import *
from demo_planners.nlmerge.reasoning_direct import *
from demo_planners.utils import get_tool_description, dictlist_to_markdown, standard_NL2LLM_agent
from demo_planners.simple_agentic import run as run_agentic

import time
import json
import traceback
import asyncio


def _iter_tool_nodes(node):
    """Yield tool-bearing nodes from an execution tree in preorder."""
    if not node or node.get("type") == "empty_input":
        return
    if node.get("tool"):
        yield node
    for child in node.get("inputs", []):
        yield from _iter_tool_nodes(child)


def _tool_names_from_tree(node):
    return sorted({n["tool"] for n in _iter_tool_nodes(node) if n.get("tool")})


def _single_markdown_table(data):
    if data is None:
        return "_No data_"
    if isinstance(data, list):
        if len(data) == 0:
            return "_Empty list_"
        if not all(isinstance(x, dict) for x in data):
            normalised = [{"index": idx, "value": item} for idx, item in enumerate(data)]
        else:
            normalised = data
    elif isinstance(data, dict):
        normalised = [data]
    else:
        normalised = [{"value": data}]

    try:
        return dictlist_to_markdown(normalised)
    except Exception:
        return f"```json\n{json.dumps(data, indent=2, default=str)}\n```"


def _data_to_tables(data):
    if isinstance(data, list) and len(data) > 0 and all(isinstance(x, list) for x in data):
        tables = []
        for sub in data:
            tables.extend(_data_to_tables(sub))
        return tables or ["_Empty list_"]
    return [_single_markdown_table(data)]


def _format_execution_sections(output_tree):
    sections = []
    for idx, node in enumerate(_iter_tool_nodes(output_tree), start=1):
        tool_name = node.get("tool")
        sections.append(f"### Step {idx}: {tool_name}")
        for inp_idx, child in enumerate(node.get("inputs", []), start=1):
            tables = _data_to_tables(child.get('result'))
            for table_idx, table in enumerate(tables, start=1):
                label = f"Input {inp_idx}" if len(tables) == 1 else f"Input {inp_idx} (part {table_idx})"
                sections.append(f"{label}:\n{table}")
        output_tables = _data_to_tables(node.get('result'))
        for table_idx, table in enumerate(output_tables, start=1):
            label = "Output" if len(output_tables) == 1 else f"Output (part {table_idx})"
            sections.append(f"{label}:\n{table}")
        attrs = node.get("attrs")
        if attrs:
            sections.append("Attributes:\n```json\n" + json.dumps(attrs, indent=2, default=str) + "\n```")
    return "\n\n".join(sections)


def _build_assessment_prompt(task, plan, output_tree, result, previous_errors, current_error):
    tool_names = _tool_names_from_tree(output_tree)
    tools_description = ""
    if tool_names:
        try:
            tools_description = get_tool_description(tool_names, level=['basic', 'linking'], type='themergeone')
        except Exception:
            tools_description = get_tool_description(tool_names)
    plan_text = ""
    try:
        plan_text = get_plan_text(plan)
    except Exception:
        plan_text = str(plan)

    execution_sections = _format_execution_sections(output_tree)
    final_output = "\n\n".join(_data_to_tables(result))
    prior_errors_text = "None"
    if previous_errors:
        formatted_errors = []
        for idx, err in enumerate(previous_errors, start=1):
            if not err:
                continue
            formatted_errors.append(f"{idx}. {err}")
        if formatted_errors:
            prior_errors_text = "\n".join(formatted_errors)

    if current_error:
        current_error_text = current_error
        current_error_status = "Execution reported an error."
    else:
        current_error_text = "None"
        current_error_status = "Execution completed without raising an error."

    prompt = f"""You are an execution reviewer helping decide whether another planning round is needed.\nTask: {task}\n\nPlan summary:\n{plan_text}\n\nTool descriptions (consider whether a different tool mix or attributes would resolve the issues observed):\n{tools_description}\n\nExecution details:\n{execution_sections}\n\nFinal output:\n{final_output}\n\nCurrent execution status: {current_error_status}\nCurrent execution error message: {current_error_text}\n\nPrevious errors or guidance from earlier rounds (for context only – do not assume they persist if the current execution resolves them):\n{prior_errors_text}\n\nProvide your analysis in JSON with the following keys: \n- NEED_ADDITIONAL_ROUND: Answer with 'True' or 'False'.\n- ASSESSMENT: Short justification of the current state.\n- NEXT_ROUND_COMMENTS: Detailed guidance for the planner and linker describing what went wrong in execution and how to adjust tool selection and/or attributes to resolve it (empty string otherwise).\nBefore recommending another round, verify that the current execution still exhibits an issue. If the present run succeeds, acknowledge the resolution even if previous rounds failed.\n"""
    return prompt


def _run_post_execution_assessment(task, plan, output_tree, result, previous_errors, current_error):
    try:
        prompt = _build_assessment_prompt(task, plan, output_tree, result, previous_errors, current_error)
        assessment = standard_NL2LLM_agent(prompt, ["NEED_ADDITIONAL_ROUND", "ASSESSMENT", "NEXT_ROUND_COMMENTS"])
        if assessment and assessment[0]:
            return assessment[0][0]
    except Exception as exc:
        return {"NEED_ADDITIONAL_ROUND": "False", "ASSESSMENT": f"Assessment failed: {exc}", "NEXT_ROUND_COMMENTS": ""}
    return {"NEED_ADDITIONAL_ROUND": "False", "ASSESSMENT": "Assessment unavailable.", "NEXT_ROUND_COMMENTS": ""}

def run(task, method='old', previous_outputs=[],tools_list=['JOIN_2', 'SELECT', 'NL2LLM', 'ROWWISE_NL2LLM', 'NL2SQL', 'COUNT']):
    result,output_tree,error_round,example_tree,plan='',[],'',[],''
    start_time = time.perf_counter()
    error_history = [log.get('error_round') for log in previous_outputs if log.get('error_round')]
    assessment_error_context = error_history[-1:] if error_history else []
    if method=='reasoning_direct':
        prev_rounds=[]
        for x in previous_outputs:
            prev_rounds+=[x['plan']]
            prev_rounds+=[x['error_round']+'\n Please propose a materially different version of the plan that addresses this issue and avoids repeating the same failing steps.']
        plan=run_planning(task,previous_rounds_text=prev_rounds,tools_list=tools_list)
        plan_time= time.perf_counter() - start_time   
        try:
            example_tree=json.loads(plan.replace("\"attributes\"","\"attrs\""))

        except Exception as e:
            error_round= f"Error was encountered while converting output to json:\n{traceback.format_exc()}"
            new_log={'result':result,'output_tree':output_tree, 'plan_time':plan_time,'plan':plan, 'example_tree':example_tree, 'error_round':error_round}
            return previous_outputs+[new_log]
    elif method=='linear_planner':

        logs=previous_outputs
        logs=from_task_to_result(task,logs, tools_list=tools_list)
        logs[-1]['error_round']=logs[-1]['issue_summary_next_step']

        logs[-1]['output_tree']=[]
        try:
            logs[-1]['plan_time']=logs[-1]['plan_time_plan']+logs[-1]['operators_linking'][1][-1]['TIME_LINKING'] 
        except:
            logs[-1]['plan_time']=-1
        logs[-1]['example_tree']= []

        index_last_step=logs[-1]['steps_results'].keys()
        index_last_step=[int(x) for x in index_last_step if str(x).isdigit()]
        index_last_step=max(index_last_step) if len(index_last_step)>0 else 0
        logs[-1]['result']=logs[-1]['steps_results'][index_last_step]['output']
        elapsed = time.perf_counter() - start_time
        logs[-1]["round_time"]= elapsed
        if logs[-1]['next_direction']=='termination':
            #was logs[-1]['error_round']=='':

            #means we are done, we switch keys
            # Switch keys if both 'raw_plan' and 'plan' exist in any log
            for log in logs:
                if 'raw_plan' in log and 'plan' in log:
                    log['plan'], log['raw_plan'] = log['raw_plan'], log['plan']
        return logs


    elif method == 'agentic':
        return run_agentic(task=task, tools_list=tools_list, previous_outputs=previous_outputs)
    elif method in ['new','old']: #method = new or old
        try:
            if len(previous_outputs)>0:
                try:
                    last_plan_text = get_plan_text(previous_outputs[-1]['plan'])
                except Exception:
                    last_plan_text = str(previous_outputs[-1]['plan'])
                if error_history:
                    formatted_history = "\n".join([f"- {err}" for err in error_history if err])
                else:
                    formatted_history = "- (no structured error message recorded)"
                error_mitigation = (
                    "***PAY ATTENTION***\n"
                    "The following errors or guidance were observed in earlier rounds:\n"
                    f"{formatted_history}\n"
                    "Your most recent plan was:\n"
                    f"{last_plan_text}\n"
                    "Design a revised plan that explicitly resolves these issues."
                    " Adjust tool choices or attributes as needed, and avoid repeating the same sequence of steps that previously failed."
                    " Highlight changes compared to the prior attempt so the approach is clearly different."
                )
                plan= get_plan(task, method=method, error_mitigation=error_mitigation,tools_list=tools_list)
            else:
                plan = get_plan(task, method=method,tools_list=tools_list)
            plan_time= time.perf_counter() - start_time
        except Exception as e:
            error_round= f"Error was encountered while planning:\n{traceback.format_exc()}"
            new_log={'result':result,'output_tree':output_tree, 'plan':plan, 'example_tree':example_tree, 'error_round':error_round}
            return previous_outputs+[new_log]
        try:
            example_tree = parse_chain(plan[-1])
        except Exception as e:
            error_round= f"Error was encountered while converting plan to tree, is the last output in a decodable format? Error:\n{traceback.format_exc()}"
            new_log={'result':result,'output_tree':output_tree, 'plan':plan,'plan_time':plan_time, 'example_tree':example_tree, 'error_round':error_round}
            return previous_outputs+[new_log]
    else:
        raise Exception(f"Method {method} not recognized")


    def mock_NL_to_RUN(tool, inp, attributes, properties):
        print(f"[{time.strftime('%H:%M:%S')}] {tool} processing with inputs and attributes {attributes}")
        try:
            return NL_to_RUN(tool, inp, attributes, properties)
        except Exception as e:
            error_txt = f"Error running {tool} with attributes {json.dumps(attributes)}:\n{traceback.format_exc()}"
            print(error_txt)  # optional logging
            raise RuntimeError(error_txt) from e
    assessment_details = {}
    try:
    # Run the tree asynchronously
        result, output_tree = asyncio.run(example_usage(example_tree, mock_NL_to_RUN))
        # if error_round == '':
        #     current_error_message = error_round if error_round else ""
        #     assessment_details = _run_post_execution_assessment(
        #         task,
        #         plan,
        #         output_tree,
        #         result,
        #         assessment_error_context,
        #         current_error_message,
        #     )
        #     decision_flag = str(assessment_details.get("NEED_ADDITIONAL_ROUND", "")).strip().lower()
        #     needs_round = decision_flag in {'true', 'yes', 'y', '1'}
        #     if needs_round:
        #         guidance = assessment_details.get("NEXT_ROUND_COMMENTS") or assessment_details.get("ASSESSMENT") or "Please refine the plan based on the review."
        #         error_round = guidance
    except Exception as e:
        error_round= f"Error was encountered at execution:\n{traceback.format_exc()}"
    elapsed = time.perf_counter() - start_time
    new_log={'result':result,'output_tree':output_tree, "round_time": elapsed, 'plan_time':plan_time,'plan':plan, 'example_tree':example_tree, 'error_round':error_round}
    if assessment_details:
        new_log['assessment'] = assessment_details
    return previous_outputs+[new_log]



def run_iterative(task, method='old',tools_list=['JOIN_2', 'SELECT', 'NL2LLM', 'ROWWISE_NL2LLM', 'NL2SQL', 'COUNT']):
    previous_outputs=[]
    round=0
    while len(previous_outputs)==0 or len(previous_outputs[-1]['error_round'])>0:
        if round>10: break
        logging.critical('@@@@@@@@@@@@@@@ Round '+str(round))
        
        previous_outputs=run(task, method, previous_outputs,tools_list=tools_list)
        
        
        if len(previous_outputs)>0 and 'plan' in previous_outputs[-1]:
            print('Last plan:\n',previous_outputs[-1]['plan'], '\n last error:',previous_outputs[-1]['error_round'])
        round+=1
        if method=='agentic':
            break
    return previous_outputs



