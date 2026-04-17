import argparse
import json
import re
import sys
from typing import Any, Dict, List, Optional

from demo_planners.config import DEFAULT_SERVICE_URL
from demo_planners.nlmerge.pipeline import run_iterative


DEFAULT_TOOLS = ['JOIN_2', 'SELECT', 'NL2LLM', 'ROWWISE_NL2LLM', 'NL2SQL', 'COUNT']
METHOD_INTERNAL_TO_PUBLIC = {
    "old": "single_shot_tree_planning_guided_inner_outer",
    "new": "single_shot_tree_planning_guided_outer_inner",
    "reasoning_direct": "single_shot_tree_planning_reasoning",
    "agentic": "iterative_planning",
    "linear_planner": "cascade_planning",
}
METHOD_PUBLIC_TO_INTERNAL = {public: internal for internal, public in METHOD_INTERNAL_TO_PUBLIC.items()}
METHOD_CHOICES = list(METHOD_PUBLIC_TO_INTERNAL.keys())
METHOD_LABELS = {
    "single_shot_tree_planning_guided_inner_outer": "Single-Shot Tree Planning (guided NLMerge, inner -> outer)",
    "single_shot_tree_planning_guided_outer_inner": "Single-Shot Tree Planning (guided NLMerge, outer -> inner)",
    "single_shot_tree_planning_reasoning": "Single-Shot Tree Planning (reasoning)",
    "iterative_planning": "Iterative Planning",
    "cascade_planning": "Cascade Planning",
}
DEFAULT_METHOD = "single_shot_tree_planning_guided_inner_outer"


def _prompt(text: str, default: Optional[str] = None) -> str:
    try:
        value = input(text)
    except EOFError:
        value = ""
    value = value.strip()
    if not value and default is not None:
        return default
    return value


def _parse_tools(raw: str) -> List[str]:
    if not raw:
        return list(DEFAULT_TOOLS)
    text = raw.strip()
    if text.lower() in {"default", "defaults"}:
        return list(DEFAULT_TOOLS)
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except json.JSONDecodeError:
            pass
    parts = [p for p in re.split(r"[,\s]+", text) if p]
    return [p.strip() for p in parts if p.strip()]


def _normalize_method(method: str) -> str:
    value = (method or "").strip()
    if value in METHOD_PUBLIC_TO_INTERNAL:
        return value
    if value in METHOD_INTERNAL_TO_PUBLIC:
        return METHOD_INTERNAL_TO_PUBLIC[value]
    raise ValueError(f"Invalid method '{value}'. Choose from {METHOD_CHOICES}.")


def _strip_results_from_tree(node: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not node:
        return None
    if node.get("type") == "empty_input":
        return {"type": "empty_input"}
    inputs = [_strip_results_from_tree(child) for child in node.get("inputs", [])]
    cleaned = {
        "tool": node.get("tool"),
        "attrs": node.get("attrs"),
    }
    if inputs:
        cleaned["inputs"] = inputs
    return cleaned


def _steps_from_output_tree(output_tree: Dict[str, Any]) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []

    def walk(node: Dict[str, Any]) -> None:
        if not node or node.get("type") == "empty_input":
            return
        for child in node.get("inputs", []):
            walk(child)
        steps.append(
            {
                "tool": node.get("tool"),
                "attrs": node.get("attrs"),
                "result": node.get("result"),
            }
        )

    walk(output_tree)
    for idx, step in enumerate(steps, start=1):
        step["index"] = idx
    return steps


def _steps_from_linear(log: Dict[str, Any]) -> List[Dict[str, Any]]:
    steps_results = log.get("steps_results") or {}
    plan_steps = log.get("plan") or []
    items: List[Dict[str, Any]] = []
    for key in sorted(steps_results, key=lambda x: int(x)):
        entry = steps_results[key]
        idx = int(key)
        tool_name = plan_steps[idx]["name"] if idx < len(plan_steps) else None
        items.append(
            {
                "index": idx,
                "tool": tool_name,
                "result": entry.get("output"),
                "tool_input_and_attributes": entry.get("tool_input_and_attributes"),
            }
        )
    return items


def _steps_from_agentic(output_tree: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    for step in output_tree:
        steps.append(
            {
                "index": step.get("index"),
                "tool": step.get("tool"),
                "attrs": step.get("attributes"),
                "result": step.get("raw_result"),
                "inputs": step.get("inputs"),
                "status": step.get("status"),
            }
        )
    return steps


def _build_agentic_dag(output_tree: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not output_tree:
        return None
    by_index = {step.get("index"): step for step in output_tree if step.get("index")}
    if not by_index:
        return None
    root_idx = None
    for idx, step in by_index.items():
        if step.get("tool") == "OUTPUT":
            root_idx = idx
    if root_idx is None:
        root_idx = max(by_index.keys())

    def build(idx: int, seen: set) -> Dict[str, Any]:
        step = by_index.get(idx, {})
        if idx in seen:
            return {"tool": step.get("tool"), "id": idx}
        seen.add(idx)
        node = {
            "tool": step.get("tool"),
            "attrs": step.get("attributes"),
            "id": idx,
        }
        inputs = []
        for dep in step.get("inputs", []) or []:
            if isinstance(dep, int):
                inputs.append(build(dep, seen.copy()))
        if inputs:
            node["inputs"] = inputs
        return node

    return build(root_idx, set())


def _normalise_ancestor_dico(ancestor_dico: Dict[Any, Any]) -> Dict[int, List[int]]:
    normalised: Dict[int, List[int]] = {}
    for key, deps in ancestor_dico.items():
        try:
            idx = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(deps, list):
            continue
        clean_deps: List[int] = []
        for dep in deps:
            try:
                clean_deps.append(int(dep))
            except (TypeError, ValueError):
                continue
        normalised[idx] = clean_deps
    return normalised


def _build_linear_dag(log: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    plan_steps = log.get("plan") or []
    ancestor_dico = log.get("ancestor_dico") or {}
    steps_results = log.get("steps_results") or {}
    if not plan_steps or not ancestor_dico:
        return None
    deps_map = _normalise_ancestor_dico(ancestor_dico)
    if not deps_map:
        return None
    if steps_results:
        try:
            root_idx = max(int(k) for k in steps_results.keys())
        except ValueError:
            root_idx = max(deps_map.keys())
    else:
        root_idx = max(deps_map.keys())

    def build(idx: int, seen: set) -> Dict[str, Any]:
        if idx in seen:
            tool_name = plan_steps[idx]["name"] if idx < len(plan_steps) else None
            return {"tool": tool_name, "id": idx}
        seen.add(idx)
        tool_name = plan_steps[idx]["name"] if idx < len(plan_steps) else None
        node: Dict[str, Any] = {"tool": tool_name, "id": idx}
        if idx < len(plan_steps) and plan_steps[idx].get("description"):
            node["description"] = plan_steps[idx]["description"]
        inputs = []
        for dep in deps_map.get(idx, []):
            if dep == 0 and plan_steps and plan_steps[0].get("name") == "START":
                inputs.append({"type": "empty_input"})
            else:
                inputs.append(build(dep, seen.copy()))
        if inputs:
            node["inputs"] = inputs
        return node

    return build(root_idx, set())


def _build_plan_dag(log: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    example_tree = log.get("example_tree")
    if isinstance(example_tree, dict) and example_tree:
        return example_tree
    output_tree = log.get("output_tree")
    if isinstance(output_tree, dict) and output_tree:
        return _strip_results_from_tree(output_tree)
    if isinstance(output_tree, list) and output_tree:
        agentic_dag = _build_agentic_dag(output_tree)
        if agentic_dag:
            return agentic_dag
    linear_dag = _build_linear_dag(log)
    if linear_dag:
        return linear_dag
    return None


def _build_steps(log: Dict[str, Any]) -> List[Dict[str, Any]]:
    if log.get("steps_results"):
        return _steps_from_linear(log)
    output_tree = log.get("output_tree")
    if isinstance(output_tree, dict) and output_tree:
        return _steps_from_output_tree(output_tree)
    if isinstance(output_tree, list) and output_tree:
        return _steps_from_agentic(output_tree)
    return []


def _patch_service_url(service_url: str) -> None:
    if not service_url:
        return
    from blue.utils import service_utils

    if getattr(service_utils.ServiceClient, "_demo_service_url_patched", False):
        service_utils.ServiceClient._demo_service_url_override = service_url
        return

    original = service_utils.ServiceClient.get_service_address

    def _get_service_address(self, properties=None):
        url = original(self, properties=properties)
        override = getattr(self.__class__, "_demo_service_url_override", "")
        return url or override

    service_utils.ServiceClient.get_service_address = _get_service_address
    service_utils.ServiceClient._demo_service_url_patched = True
    service_utils.ServiceClient._demo_service_url_override = service_url


def run_demo_question(
    question: str,
    tools_list: Optional[List[str]] = None,
    method: str = DEFAULT_METHOD,
    service_url: str = DEFAULT_SERVICE_URL,
) -> Dict[str, Any]:
    if not question or not question.strip():
        raise ValueError("question must be a non-empty string")
    if not tools_list:
        tools_list = list(DEFAULT_TOOLS)
    method = _normalize_method(method)
    internal_method = METHOD_PUBLIC_TO_INTERNAL[method]

    _patch_service_url(service_url)

    logs = run_iterative(question, method=internal_method, tools_list=tools_list)
    if not logs:
        raise RuntimeError("No output returned by pipeline.")

    last_log = logs[-1]
    return {
        "question": question,
        "method": method,
        "method_label": METHOD_LABELS.get(method, method),
        "internal_method": internal_method,
        "tools": tools_list,
        "service_url": service_url,
        "final_answer": last_log.get("result"),
        "steps": _build_steps(last_log),
        "plan_dag": _build_plan_dag(last_log),
        "error": last_log.get("error_round") or "",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a demo NL pipeline and output JSON results.")
    parser.add_argument("--question", "--query", dest="question", help="Question/query to run.")
    parser.add_argument("--tools", help="Comma-separated list or JSON array of tools.")
    parser.add_argument("--method", help="Planner variant to use.")
    parser.add_argument("--output", help="Optional path to write the JSON output.")
    parser.add_argument(
        "--service-url",
        default=DEFAULT_SERVICE_URL,
        help=f"WebSocket service URL (default: {DEFAULT_SERVICE_URL}).",
    )
    args = parser.parse_args()

    question = args.question or _prompt("Enter query/question: ")
    if not question:
        print("No question provided.", file=sys.stderr)
        return 1

    tools_raw = args.tools or _prompt(
        f"Tools to use (comma-separated or JSON list). Press enter for default {DEFAULT_TOOLS}: ",
        default="",
    )
    tools_list = _parse_tools(tools_raw)
    if not tools_list:
        tools_list = list(DEFAULT_TOOLS)

    method = args.method or _prompt(
        f"Planner variant {METHOD_CHOICES} (default: {DEFAULT_METHOD}): ",
        default=DEFAULT_METHOD,
    )
    try:
        method = _normalize_method(method)
    except ValueError:
        print(f"Invalid method '{method}'. Choose from {METHOD_CHOICES}.", file=sys.stderr)
        return 1

    try:
        payload = run_demo_question(
            question=question,
            tools_list=tools_list,
            method=method,
            service_url=args.service_url,
        )
    except Exception as exc:
        payload = {
            "question": question,
            "method": method,
            "method_label": METHOD_LABELS.get(method, method),
            "internal_method": METHOD_PUBLIC_TO_INTERNAL.get(method),
            "tools": tools_list,
            "service_url": args.service_url,
            "final_answer": "",
            "steps": [],
            "plan_dag": None,
            "error": str(exc),
        }

    output_json = json.dumps(payload, indent=2, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_json)
        print(args.output)
    else:
        print(output_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
