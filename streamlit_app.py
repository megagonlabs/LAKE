import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from demo_pipeline_runner import (
    DEFAULT_METHOD,
    DEFAULT_SERVICE_URL,
    DEFAULT_TOOLS,
    METHOD_CHOICES,
    METHOD_LABELS,
    run_demo_question,
)
from streamlit_app_support import in_streamlit_context, normalize_payload

# Two common tool presets used across this repo.
TOOL_SET_SMART = ["JOIN_2", "NL2LLM", "ROWWISE_NL2LLM", "SMARTNL2SQL"]
TOOL_SET_DEFAULT = list(DEFAULT_TOOLS)
ALL_TOOLS = sorted(set(TOOL_SET_SMART + TOOL_SET_DEFAULT))


@dataclass
class DagNode:
    node_id: str
    tool: str
    attrs: Dict[str, Any]
    children: List["DagNode"]


def _is_table(value: Any) -> bool:
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(x, dict) for x in value)
    )


def _is_table_list(value: Any) -> bool:
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(_is_table(x) for x in value)
    )


def _render_result(value: Any, label: str = "Result") -> None:
    if value is None:
        st.caption(f"{label}: None")
        return
    if _is_table_list(value):
        if len(value) == 1:
            st.subheader(label)
            st.dataframe(value[0], use_container_width=True)
            return
        tabs = st.tabs([f"{label} {idx+1}" for idx in range(len(value))])
        for idx, tab in enumerate(tabs):
            with tab:
                st.dataframe(value[idx], use_container_width=True)
        return
    if _is_table(value):
        st.subheader(label)
        st.dataframe(value, use_container_width=True)
        return
    st.subheader(label)
    st.code(json.dumps(value, indent=2, default=str), language="json")


def _parse_plan_dag(plan_dag: Optional[Dict[str, Any]]) -> Optional[DagNode]:
    if not isinstance(plan_dag, dict) or not plan_dag:
        return None

    counter = {"n": 0}

    def build(node: Dict[str, Any]) -> Optional[DagNode]:
        if not isinstance(node, dict) or not node:
            return None
        if node.get("type") == "empty_input":
            return None
        counter["n"] += 1
        node_id = str(node.get("id") or f"n{counter['n']}")
        tool = str(node.get("tool") or "")
        attrs = node.get("attrs") or {}
        if not isinstance(attrs, dict):
            attrs = {"value": attrs}
        children: List[DagNode] = []
        for child in node.get("inputs", []) or []:
            child_node = build(child)
            if child_node is not None:
                children.append(child_node)
        return DagNode(node_id=node_id, tool=tool, attrs=attrs, children=children)

    return build(plan_dag)


def _postorder_nodes(root: DagNode) -> List[DagNode]:
    ordered: List[DagNode] = []

    def walk(n: DagNode) -> None:
        for c in n.children:
            walk(c)
        ordered.append(n)

    walk(root)
    return ordered


def _build_graph_data(root: DagNode) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
    nodes: List[Dict[str, Any]] = []
    edges: List[Tuple[str, str]] = []

    def walk(n: DagNode) -> None:
        nodes.append({"id": n.node_id, "label": n.tool or n.node_id})
        for c in n.children:
            # Reverse arrow direction so the graph shows dataflow: input -> operator.
            edges.append((c.node_id, n.node_id))
            walk(c)

    walk(root)
    seen = set()
    unique_nodes = []
    for n in nodes:
        if n["id"] in seen:
            continue
        seen.add(n["id"])
        unique_nodes.append(n)
    return unique_nodes, edges


def _render_graph(root: DagNode) -> Optional[str]:
    # Prefer interactive clicking if streamlit-agraph is installed.
    try:
        from streamlit_agraph import agraph, Node, Edge, Config  # type: ignore
    except Exception:
        st.info("Install `streamlit-agraph` for clickable graph: `pip install streamlit-agraph`.")
        st.caption("Fallback: select a node below.")
        return None

    ordered = _postorder_nodes(root)
    first_node_id = ordered[0].node_id if ordered else None
    last_node_id = ordered[-1].node_id if ordered else None

    nodes_data, edges_data = _build_graph_data(root)
    nodes = []
    for n in nodes_data:
        node_kwargs: Dict[str, Any] = {}
        if first_node_id and n["id"] == first_node_id:
            node_kwargs["font"] = {"bold": True}
        if last_node_id and n["id"] == last_node_id:
            node_kwargs["color"] = "#2ecc71"
        nodes.append(Node(id=n["id"], label=n["label"], **node_kwargs))
    edges = [Edge(source=src, target=dst) for (src, dst) in edges_data]
    config = Config(
        width="100%",
        height=500,
        directed=True,
        physics=True,
        hierarchical=False,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        collapsible=True,
    )
    selected = agraph(nodes=nodes, edges=edges, config=config)
    if isinstance(selected, dict):
        return selected.get("id")
    return None


def main() -> None:
    if not in_streamlit_context():
        print("This file is a Streamlit app. Run `streamlit run streamlit_app.py`.", file=sys.stderr)
        return

    st.set_page_config(page_title="L.A.K.E.", layout="wide")
    st.title("L.A.K.E.")

    with st.sidebar:
        st.header("Run")
        service_url = st.text_input("Service URL", value=DEFAULT_SERVICE_URL)
        method = st.selectbox(
            "Planner variant",
            METHOD_CHOICES,
            index=METHOD_CHOICES.index(DEFAULT_METHOD),
            format_func=lambda m: METHOD_LABELS.get(m, m),
        )
        tools = st.multiselect("Tools", ALL_TOOLS, default=TOOL_SET_DEFAULT)
        question = st.text_area(
            "Question",
            value="Give me 10 available jobs and for each of them, say if they concern restaurant",
            height=120,
        )
        run_clicked = st.button("Run", type="primary")

    if run_clicked:
        try:
            with st.spinner("Running pipeline..."):
                raw_payload = run_demo_question(
                    question=question,
                    tools_list=tools,
                    method=method,
                    service_url=service_url,
                )
            payload = normalize_payload(
                raw_payload,
                question=question,
                method=method,
                tools=tools,
                service_url=service_url,
                method_labels=METHOD_LABELS,
            )
        except Exception as exc:  # noqa: BLE001
            payload = normalize_payload(
                {"error": str(exc)},
                question=question,
                method=method,
                tools=tools,
                service_url=service_url,
                method_labels=METHOD_LABELS,
            )
        st.session_state["last_payload"] = payload

    raw_payload = st.session_state.get("last_payload")
    if raw_payload is None:
        st.caption("Run a question to see the plan DAG and step outputs.")
        return

    payload = normalize_payload(
        raw_payload,
        question=question,
        method=method,
        tools=tools,
        service_url=service_url,
        method_labels=METHOD_LABELS,
    )
    st.session_state["last_payload"] = payload

    error_text = payload.get("error") or ""
    if error_text:
        st.error(error_text)

    col_a, col_b = st.columns([1, 1])
    with col_a:
        _render_result(payload.get("final_answer"), label="Final Answer")
    with col_b:
        st.subheader("Run Info")
        st.code(
            json.dumps(
                {
                    "method": payload.get("method_label") or payload.get("method"),
                    "tools": payload.get("tools"),
                    "service_url": payload.get("service_url"),
                },
                indent=2,
            ),
            language="json",
        )

    st.divider()

    plan_root = _parse_plan_dag(payload.get("plan_dag"))
    steps: List[Dict[str, Any]] = payload.get("steps") or []

    if not plan_root:
        st.warning("No plan DAG available for this run.")
        return

    st.subheader("Operator Graph")
    clicked_node_id = _render_graph(plan_root)

    node_list = _postorder_nodes(plan_root)
    id_to_node = {n.node_id: n for n in node_list}

    # Steps in payload are in postorder with 1-based index.
    id_to_step_index: Dict[str, int] = {}
    for idx, n in enumerate(node_list, start=1):
        id_to_step_index[n.node_id] = idx

    st.subheader("Pipeline Steps")
    if node_list:
        items = []
        for idx, n in enumerate(node_list, start=1):
            text = f"{idx}. {n.tool}"
            if idx == 1:
                text = f"<b>{text}</b>"
            if idx == len(node_list):
                text = f"<span style='color:#2ecc71'>{text}</span>"
            items.append(f"<li>{text}</li>")
        st.markdown("<ol>" + "".join(items) + "</ol>", unsafe_allow_html=True)

    st.subheader("Operator Details")

    default_selected = clicked_node_id or node_list[-1].node_id
    node_ids = [n.node_id for n in node_list]
    selected_node_id = st.selectbox(
        "Select operator",
        options=node_ids,
        format_func=lambda nid: f"{id_to_step_index.get(nid, '?')}. {id_to_node[nid].tool}",
        index=node_ids.index(default_selected),
    )

    selected_node = id_to_node[selected_node_id]
    step_idx = id_to_step_index.get(selected_node_id)

    details_cols = st.columns([1, 2])
    with details_cols[0]:
        st.caption("Attributes")
        st.code(json.dumps(selected_node.attrs, indent=2, default=str), language="json")
    with details_cols[1]:
        st.caption("Step Output")
        step_result: Any = None
        if isinstance(step_idx, int) and 1 <= step_idx <= len(steps):
            step_result = steps[step_idx - 1].get("result")
        _render_result(step_result, label=f"Step {step_idx} Output" if step_idx else "Output")

    with st.expander("All Steps (raw)"):
        st.code(json.dumps(steps, indent=2, default=str), language="json")


if __name__ == "__main__":
    main()
