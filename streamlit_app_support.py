from typing import Any, Dict, Mapping, Sequence


def in_streamlit_context() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except Exception:
        return False
    return get_script_run_ctx(suppress_warning=True) is not None


def normalize_payload(
    payload: Any,
    *,
    question: str,
    method: str,
    tools: Sequence[str],
    service_url: str,
    method_labels: Mapping[str, str],
) -> Dict[str, Any]:
    method_value = str(method or "")
    base: Dict[str, Any] = {
        "question": question,
        "method": method_value,
        "method_label": method_labels.get(method_value, method_value),
        "tools": list(tools),
        "service_url": service_url,
        "final_answer": None,
        "steps": [],
        "plan_dag": None,
        "error": "",
    }

    if payload is None:
        base["error"] = "Pipeline returned no payload."
        return base
    if not isinstance(payload, dict):
        base["error"] = f"Pipeline returned unsupported payload type: {type(payload).__name__}."
        return base
    if not payload:
        base["error"] = "Pipeline returned an empty payload."
        return base

    base.update(payload)
    base["question"] = str(base.get("question") or question)

    method_value = str(base.get("method") or method)
    base["method"] = method_value
    method_label_value = payload.get("method_label") if isinstance(payload, dict) else None
    base["method_label"] = str(method_label_value or method_labels.get(method_value, method_value))

    tools_value = base.get("tools")
    if isinstance(tools_value, (list, tuple)):
        base["tools"] = [str(tool) for tool in tools_value]
    else:
        base["tools"] = list(tools)

    base["service_url"] = str(base.get("service_url") or service_url)
    base["error"] = str(base.get("error") or "")

    if not isinstance(base.get("steps"), list):
        base["steps"] = []

    return base
