# L.A.K.E. Repository Overview

This folder documents the code that ships in the `LAKE` repository.

## Runtime Flow

1. `streamlit_app.py` collects the user question, selected planner variant, tool list, and service URL.
2. `demo_pipeline_runner.py` normalizes inputs, calls the selected planner pipeline, and reshapes outputs into a UI-friendly payload.
3. `demo_planners.nlmerge.pipeline` routes execution into one of the planner implementations.
4. `demo_planners.utils` wraps Blue operators, retry logic, and helper formatting utilities.

## Planner Families

- `demo_planners/linear_planner/`: linear planning and linking flow.
- `demo_planners/nlmerge/`: tree-style planning and execution helpers.
- `demo_planners/simple_agentic/`: iterative/agentic planner variant.
- `demo_planners/simple_agents_runnable.py`: dispatch layer from symbolic tool names to executable wrappers.

## External Dependencies

- Blue: required upstream framework for operators, planners, and service integration.
- OpenAI Python SDK: used for planner and helper paths that call `OpenAI()`.
- Streamlit and `streamlit-agraph`: used for the demo UI and graph visualization.

## Publication Notes

- The repo keeps Blue separate on purpose; the upstream install link lives in the top-level `README.md`.
- Local defaults such as `ws://localhost:8001` are for development only and should be overridden in non-local environments.
- The smoke tests in `tests/` are intentionally backend-free so reviewers can validate the repository without a running Blue service.
