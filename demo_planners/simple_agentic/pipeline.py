import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from demo_planners.simple_agents_runnable import NL_to_RUN
from demo_planners.utils import dictlist_to_markdown, get_tool_description
from demo_planners.utils import get_answer_gpt_advanced  # type: ignore


DEFAULT_TOOLS = ['JOIN_2', 'SELECT', 'NL2LLM', 'ROWWISE_NL2LLM', 'NL2SQL', 'COUNT']
MAX_STEPS = 15
MAX_SUMMARY_CHARS = 1500
OUTPUT_SUMMARY_LIMIT = 400
LOG_PREFIX = "[AgenticPlanner]"


@dataclass
class AgentStep:
    index: int
    tool: str
    inputs: List[int] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    properties: Dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    thought: str = ""
    observation: str = ""
    raw_result: Any = None
    error: Optional[str] = None

    def for_summary(self) -> str:
        base = f"Step {self.index}: {self.tool}"
        if self.status == "error" and self.error:
            return base + f" (error: {self.error})"
        if self.status == "completed":
            return base + " (completed)"
        return base + f" ({self.status})"


def _gather_previous_feedback(previous_outputs: List[Dict[str, Any]]) -> str:
    if not previous_outputs:
        return ""
    feedback_bits = []
    for round_idx, log in enumerate(previous_outputs[-3:], start=1):
        error_text = (log or {}).get("error_round") or ""
        if error_text:
            feedback_bits.append(f"- Prior attempt {round_idx}: {error_text}")
    return "\n".join(feedback_bits)


def _truncate(text: str, limit: int = 400) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


def _format_result_for_prompt(result: Any) -> str:
    if result is None:
        return "_No result_"
    try:
        if isinstance(result, list):
            if not result:
                return "_Empty list_"
            if all(isinstance(x, dict) for x in result):
                return dictlist_to_markdown(result[:15])
            if all(isinstance(x, list) for x in result):
                subparts = [_format_result_for_prompt(sub) for sub in result[:5]]
                return "\n\n".join(subparts)
        if isinstance(result, dict):
            return dictlist_to_markdown([result])
        text = json.dumps(result, indent=2, default=str)
    except Exception:
        text = str(result)
    if len(text) > MAX_SUMMARY_CHARS:
        return text[:MAX_SUMMARY_CHARS] + "\n...(truncated)..."
    return text


def _build_system_prompt(tools_text: str) -> str:
    return (
        "You are an autonomous planner that must solve the user's task by executing tools one step at a time.\n"
        "Never draft the full plan in advance. Decide on at most one action per turn, execute it, observe the result, "
        "and only then decide on the next action.\n\n"
        "Available tools:\n"
        f"{tools_text}\n\n"
        "When you respond you MUST output valid JSON with the following schema:\n"
        "{\n"
        '  "thought": string,\n'
        '  "action_type": "use_tool" | "finish" | "fail",\n'
        '  "tool": string (required when action_type == "use_tool"),\n'
        '  "inputs": [step_numbers...] (only integers, optional when no inputs),\n'
        '  "attributes": {...} (dict, defaults to {}),\n'
        '  "properties": {...} (dict, defaults to {}),\n'
        '  "final_answer": string (required when action_type == "finish"),\n'
        '  "error": string (required when action_type == "fail")\n'
        "}\n\n"
        "Guidelines:\n"
        "- Use only one tool per turn.\n"
        "- Refer to previous tool outputs exclusively by their step number (e.g., 1, 2). They will be listed for you as reusable outputs.\n"
        "- To build an input from several tool runs, list each required step number in `inputs` in the order that matches the tool signature.\n"
        "- Keep attributes explicit and complete so the tool can run without clarification.\n"
        "- If a tool execution fails, analyse the reason and try a different approach.\n"
        "- To produce the final answer, call the special OUTPUT tool with the desired step number in `inputs` (e.g., inputs: [3])."
    )

def _build_initial_user_message(task: str, feedback_text: str) -> str:
    message = [
        f"Task: {task}",
        "Plan and execute one action at a time.",
        "Remember: respond with JSON only, following the prescribed schema.",
        "Reusable outputs: - None yet.",
    ]
    if feedback_text:
        message.append(
            "Previous rounds reported the following issues:\n" + feedback_text + "\nPlease avoid repeating them."
        )
    return "\n".join(message)


def _summarise_reusable_outputs(steps: List[AgentStep]) -> str:
    summaries = []
    for s in steps:
        if s.status == "completed":
            summary = s.observation.replace("\n", " ")
            summaries.append(f"- [{s.index}] {s.tool}: {_truncate(summary, OUTPUT_SUMMARY_LIMIT)}")
    return "\n".join(summaries) or "- None yet."


def _build_step_observation(step: AgentStep, available_steps: List[AgentStep]) -> str:
    header = f"Observation from step {step.index} ({step.tool}):"
    if step.status == "error":
        body = step.error or "Tool execution reported an error."
    else:
        body = _format_result_for_prompt(step.raw_result)
    reusable_outputs = _summarise_reusable_outputs(available_steps)
    return (
        f"{header}\n{body}\n\nReusable outputs:\n{reusable_outputs}\n\n"
        "Decide on the **next** action and respond with JSON only."
    )


def _parse_agent_response(raw: str) -> Dict[str, Any]:
    cleaned = raw.strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to recover JSON object if extra text is present
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(cleaned[start : end + 1])
            except json.JSONDecodeError:
                pass
        raise ValueError("Agent response is not valid JSON.")


def _normalise_inputs(inputs: Optional[Any]) -> List[int]:
    if inputs is None:
        return []
    if not isinstance(inputs, list):
        raise ValueError("inputs must be an array of integers.")
    normalised: List[int] = []
    for item in inputs:
        if isinstance(item, int):
            normalised.append(item)
        elif isinstance(item, str) and item.isdigit():
            normalised.append(int(item))
        else:
            raise ValueError(f"Invalid step reference: {item!r}")
    return normalised


def _safe_dict(value: Optional[Any]) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    raise ValueError("attributes/properties must be dictionaries when provided.")


def _prepare_tool_inputs(steps: Dict[int, AgentStep], indices: List[int]) -> List[Any]:
    prepared: List[Any] = []
    for idx in indices:
        step = steps.get(idx)
        if not step:
            raise ValueError(f"Unknown step reference {idx}.")
        if step.status != "completed":
            raise ValueError(f"Referenced step {idx} has status {step.status}, cannot use as input.")
        prepared.append(step.raw_result)
    return prepared


def _normalise_final_result(value: Any) -> Any:
    """
    Transform the final value into a structure compatible with existing
    consumers that expect list-of-dict tables (e.g., dictlist_to_markdown).
    """
    if isinstance(value, list) or isinstance(value, dict):
        return value
    if value is None:
        return []
    return [[{'value': value}]]


def run_agentic_round(
    task: str,
    tools_list: Optional[List[str]] = None,
    previous_outputs: Optional[List[Dict[str, Any]]] = None,
    max_steps: int = MAX_STEPS,
    model_name: str = "gpt-5",
) -> Dict[str, Any]:
    tools_list = tools_list or DEFAULT_TOOLS
    allowed_tools = set(tools_list)
    allowed_tools.add("OUTPUT")
    previous_outputs = previous_outputs or []
    start_time = time.perf_counter()
    base_tool_desc = get_tool_description(tools_list, level=['basic', 'linking'], type='themergeone')
    tool_desc = (
        base_tool_desc
        + "\nOUTPUT\n"
        + "Special tool to finish the round. Provide the step number of the desired output inside the inputs array (e.g., inputs: [5]) to return that step's result as the final answer."
    )
    system_prompt = _build_system_prompt(tool_desc)
    feedback_text = _gather_previous_feedback(previous_outputs)
    conversation: List[str] = []
    conversation.append(_build_initial_user_message(task, feedback_text))

    steps: Dict[int, AgentStep] = {}
    ordered_steps: List[AgentStep] = []
    final_payload: Any = None
    error_round = ""

    logging.critical(f"{LOG_PREFIX} Starting new round for task: {task}")
    if feedback_text:
        logging.critical(f"{LOG_PREFIX} Previous feedback:\n{feedback_text}")

    for turn in range(1, max_steps + 1):
        logging.critical(f"{LOG_PREFIX} Turn {turn}: requesting next action (conversation turns={len(conversation)})")
        try:
            raw_response = get_answer_gpt_advanced(system_prompt, conversation, model_name=model_name)
        except Exception as exc:
            error_round = f"Failed to contact language model: {exc}"
            logging.critical(f"{LOG_PREFIX} Turn {turn}: LLM request failed -> {exc}")
            break

        conversation.append(raw_response)
        logging.critical(f"{LOG_PREFIX} Turn {turn}: LLM raw response:\n{_truncate(raw_response)}")
        try:
            parsed = _parse_agent_response(raw_response)
        except ValueError as exc:
            observation = (
                "The previous response could not be parsed as JSON. Please reply with JSON only, following the schema."
            )
            conversation.append(observation)
            error_round = f"Agent output parse error: {exc}"
            logging.critical(f"{LOG_PREFIX} Turn {turn}: parse error -> {exc}")
            continue

        action_type = parsed.get("action_type")
        thought = parsed.get("thought", "")
        logging.critical(f"{LOG_PREFIX} Turn {turn}: parsed action -> {action_type}, tool={parsed.get('tool')}")

        if action_type == "finish":
            message = (
                "Use the OUTPUT tool with the desired step number in `inputs` to end the round."
            )
            conversation.append(message)
            logging.critical(f"{LOG_PREFIX} Turn {turn}: finish action rejected, requested OUTPUT usage.")
            error_round = "Finish action not supported; use OUTPUT tool instead."
            continue

        if action_type == "fail":
            error_round = parsed.get("error", "Agent returned failure without details.")
            logging.critical(f"{LOG_PREFIX} Turn {turn}: agent signalled failure -> {error_round}")
            break

        if action_type != "use_tool":
            observation = (
                "Invalid action_type received. You must provide one of: use_tool, finish, fail. "
                "Respond again with a valid JSON object."
            )
            conversation.append(observation)
            error_round = f"Unsupported action_type: {action_type}"
            logging.critical(f"{LOG_PREFIX} Turn {turn}: unsupported action type {action_type}")
            continue

        tool_name = parsed.get("tool")
        if not tool_name or not isinstance(tool_name, str):
            conversation.append("Tool name missing or invalid. Please respond with a valid tool invocation in JSON.")
            error_round = "Tool name missing."
            logging.critical(f"{LOG_PREFIX} Turn {turn}: missing tool in response")
            continue

        if tool_name not in allowed_tools:
            conversation.append(
                f"Tool '{tool_name}' is not available. Choose from: {', '.join(tools_list)}."
            )
            error_round = f"Unknown tool requested: {tool_name}"
            logging.critical(f"{LOG_PREFIX} Turn {turn}: tool {tool_name} not allowed")
            continue

        try:
            inputs = _normalise_inputs(parsed.get("inputs"))
            attributes = _safe_dict(parsed.get("attributes"))
            properties = _safe_dict(parsed.get("properties"))
        except ValueError as exc:
            conversation.append(f"{exc} Please provide the action again as valid JSON.")
            error_round = str(exc)
            logging.critical(f"{LOG_PREFIX} Turn {turn}: invalid tool payload -> {exc}")
            continue

        if tool_name == "OUTPUT":
            if len(inputs) != 1:
                conversation.append(
                    "OUTPUT tool expects exactly one completed step number in `inputs`, e.g., inputs: [3]."
                )
                error_round = "OUTPUT tool received invalid inputs."
                logging.critical(f"{LOG_PREFIX} Turn {turn}: OUTPUT invalid inputs {inputs}")
                continue

            try:
                prepared_inputs = _prepare_tool_inputs(steps, inputs)
            except ValueError as exc:
                conversation.append(str(exc))
                error_round = str(exc)
                logging.critical(f"{LOG_PREFIX} Turn {turn}: OUTPUT reference error -> {exc}")
                continue

            target_step_idx = inputs[0]
            target_result = prepared_inputs[0]

            step_index = len(ordered_steps) + 1
            step = AgentStep(
                index=step_index,
                tool=tool_name,
                inputs=inputs,
                attributes=attributes,
                properties=properties,
                thought=thought,
            )
            step.raw_result = target_result
            step.status = "completed"
            step.observation = f"Returned output from step {target_step_idx}."
            steps[step_index] = step
            ordered_steps.append(step)
            final_payload = target_result
            error_round = ""
            logging.critical(
                f"{LOG_PREFIX} Turn {turn}: OUTPUT tool returned step {target_step_idx} result and finished."
            )
            break

        step_index = len(ordered_steps) + 1
        step = AgentStep(
            index=step_index,
            tool=tool_name,
            inputs=inputs,
            attributes=attributes,
            properties=properties,
            thought=thought,
        )

        try:
            prepared_inputs = _prepare_tool_inputs(steps, inputs)
            result = NL_to_RUN(tool_name, prepared_inputs, attributes, properties)
            step.raw_result = result
            step.status = "completed"
            step.observation = _format_result_for_prompt(result)
            logging.critical(
                f"{LOG_PREFIX} Turn {turn}: executed step {step_index} "
                f"{tool_name} -> success"
            )
        except Exception as exc:
            step.status = "error"
            step.error = f"Tool execution failed: {exc}"
            step.observation = step.error
            logging.critical(
                f"{LOG_PREFIX} Turn {turn}: executed step {step_index} "
                f"{tool_name} -> error {exc}"
            )

        steps[step_index] = step
        ordered_steps.append(step)

        observation_message = _build_step_observation(step, ordered_steps)
        conversation.append(observation_message)

        if step.status == "error":
            error_round = step.error or "Tool execution failed."
        else:
            error_round = ""

    else:
        if final_payload is None:
            error_round = error_round or f"Agent stopped without final answer after {max_steps} steps."

    elapsed = time.perf_counter() - start_time
    plan_text = "\n".join(step.for_summary() for step in ordered_steps) or "No steps executed."
    logging.critical(f"{LOG_PREFIX} Round finished in {elapsed:.2f}s | error='{error_round}'")

    normalised_result = _normalise_final_result(final_payload)

    if final_payload is None:
        normalised_result = []

    return {
        "result": normalised_result,
        "output_tree": [step.__dict__ for step in ordered_steps],
        "plan": plan_text,
        "example_tree": [],
        "error_round": error_round,
        "plan_time": elapsed,
        "round_time": elapsed,
        "agent_trace": [step.__dict__ for step in ordered_steps],
        "raw_conversation": conversation,
    }


def run(task: str,
        tools_list: Optional[List[str]] = None,
        previous_outputs: Optional[List[Dict[str, Any]]] = None,
        max_steps: int = MAX_STEPS,
        model_name: str = "gpt-5") -> List[Dict[str, Any]]:
    previous_outputs = previous_outputs or []
    round_log = run_agentic_round(
        task=task,
        tools_list=tools_list,
        previous_outputs=previous_outputs,
        max_steps=max_steps,
        model_name=model_name,
    )
    return previous_outputs + [round_log]
