# Demo Code Package (without BLUE source)

This folder contains the demo app, runner, and planner-related `demo_planners` code needed for the demo paper.

## Important

The `blue` source code is intentionally **not included** here.

Get it from:
- https://github.com/megagonlabs/blue

## Included

- `streamlit_app.py`: Streamlit UI for running and visualizing planner outputs.
- `demo_pipeline_runner.py`: CLI/API entrypoint used by the app.
- `demo_planners/`: planner implementations and helper modules for:
  - `single_shot_tree_planning_guided_inner_outer` (Single-Shot Tree Planning, guided NLMerge, inner -> outer)
  - `single_shot_tree_planning_guided_outer_inner` (Single-Shot Tree Planning, guided NLMerge, outer -> inner)
  - `single_shot_tree_planning_reasoning` (Single-Shot Tree Planning, reasoning)
  - `cascade_planning`
  - `iterative_planning`

## Setup

1. Clone and install BLUE (editable install recommended):

```bash
git clone https://github.com/megagonlabs/blue.git
cd blue
pip install -e ./lib
```

2. Install demo dependencies:

```bash
cd /path/to/demo_code
pip install -r requirements.txt
```

3. Ensure this folder and BLUE source are on `PYTHONPATH` when running:

```bash
export PYTHONPATH="/path/to/blue/lib/src:$(pwd):${PYTHONPATH}"
```

## Run

### Streamlit app

```bash
streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port 8501
```

### CLI runner

```bash
python demo_pipeline_runner.py \
  --question "Give me 10 available jobs and for each of them, say if they concern restaurant" \
  --method single_shot_tree_planning_guided_inner_outer \
  --tools "JOIN_2,SELECT,NL2LLM,ROWWISE_NL2LLM,NL2SQL,COUNT" \
  --service-url ws://localhost:8001
```

## Notes

- `streamlit-agraph` is used for clickable DAG visualization. If unavailable, the app falls back to a non-clickable flow.
- Runtime still requires a reachable BLUE service endpoint (default: `ws://localhost:8001`).
