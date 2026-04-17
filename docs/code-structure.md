# L.A.K.E. Code Structure

## Entry Points

- `streamlit_app.py`
  Renders the demo UI, launches pipeline runs, and displays DAG/operator details.

- `demo_pipeline_runner.py`
  Exposes the CLI/API entrypoint, method normalization, and payload shaping for UI consumption.

## Shared Package

- `demo_planners/config.py`
  Shared runtime defaults for the demo package. These defaults are intentionally neutral and safe for publication.

- `demo_planners/utils.py`
  Blue operator wrappers, retry logic, table/result formatting helpers, and OpenAI helper functions.

- `demo_planners/simple_agents_runnable.py`
  Maps symbolic tool names such as `NL2SQL` or `JOIN_2` to executable functions.

## Planner Implementations

- `demo_planners/linear_planner/`
  Linear planning prompts, operator-linking logic, and error-handling helpers.

- `demo_planners/nlmerge/`
  Nested/tree planner examples, planner-linking helpers, and execution pipeline utilities.

- `demo_planners/simple_agentic/`
  The iterative planner path used for agentic-style execution.

## Non-Code Assets

- `README.md`
  Publication-facing install, run, disclosure, dataset, and OSS information.

- `LICENSE.txt`
  License text for the repository.

- `NOTICE`
  Attribution/notice file referenced by the repo-level release materials.
