# L.A.K.E.

L.A.K.E. is the demo package for the associated demo paper. This repository contains the Streamlit UI, the CLI/API runner, and the planner implementations used to execute and visualize demo runs.

## Important

The Blue source code is intentionally **not** vendored into this repository. Install Blue from the upstream repository before running L.A.K.E.:

- https://github.com/megagonlabs/blue

## Repository Layout

- `streamlit_app.py`: Streamlit UI for running the demo and visualizing operator graphs.
- `demo_pipeline_runner.py`: CLI/API entrypoint that normalizes inputs and returns a structured payload for the UI.
- `demo_planners/`: planner implementations, Blue operator wrappers, and helper utilities.
- `docs/`: architecture, code-structure, and testing notes for this repository.
- `tests/`: lightweight smoke tests that do not require a running Blue backend.

## Installation

1. Clone and install Blue (editable install recommended):

```bash
git clone https://github.com/megagonlabs/blue.git
cd blue
pip install -e ./lib
```

For fuller environment setup, see the upstream Blue repository documentation.

2. Install L.A.K.E. dependencies:

```bash
cd /path/to/LAKE
pip install -r requirements.txt
```

3. Ensure both this repository and the Blue source tree are on `PYTHONPATH`:

```bash
export PYTHONPATH="/path/to/blue/lib/src:$(pwd):${PYTHONPATH}"
```

4. Set required credentials before running planner variants that call the OpenAI SDK:

```bash
export OPENAI_API_KEY="your-api-key"
```

## Run

### Streamlit App

```bash
streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port 8501
```

### CLI Runner

```bash
python demo_pipeline_runner.py \
  --question "Give me 10 available jobs and for each of them, say if they concern restaurant" \
  --method single_shot_tree_planning_guided_inner_outer \
  --tools "JOIN_2,SELECT,NL2LLM,ROWWISE_NL2LLM,NL2SQL,COUNT" \
  --service-url ws://localhost:8001
```

## Configuration Notes

- `ws://localhost:8001` is only the **local development default** for the Blue websocket service. Override it from the Streamlit sidebar or with `--service-url` for other environments.
- L.A.K.E. requires a reachable Blue service endpoint and a Blue environment with the relevant datasource/operator configuration already in place.
- `streamlit-agraph` is used for clickable DAG visualization. If it is unavailable, the UI falls back to a non-clickable view.
- The OpenAI Python SDK reads credentials from environment variables; `OPENAI_API_KEY` must be available when planner paths call `OpenAI()`.

## Testing

Run the repository smoke tests with:

```bash
python -m unittest discover -s tests -v
```

These tests intentionally avoid external services. They validate that tracked Python files compile, that release-critical documentation sections are present, and that the local docs scaffold exists.

## Documentation

- [Repository Overview](docs/README.md)
- [Code Structure](docs/code-structure.md)
- [Testing Notes](docs/testing.md)

## License

This project is licensed under the terms in `LICENSE.txt`.

## Citation

If you use this repository, please cite the associated L.A.K.E. demo paper. Replace the placeholder below with the final camera-ready citation before publication.

```bibtex
@inproceedings{lake_demo_2026,
  title     = {L.A.K.E.: Logic Agent for Knowledge Extraction in Data Planning},
  author    = {Jean-Flavien Bussotti and Naoki Otani and Eser Kandogan},
  booktitle = {Proceedings of the 2026 ACM Conference on AI and Agentic Systems (CAIS)},
  year      = {2026},
  publisher = {ACM},
  address   = {San Jose, CA, USA},
  note      = {System Demonstration}
}
```

## Contact

For questions or issues, please open an issue in this repository or contact `contact_oss@megagon.ai`.

## Disclosure
Embedded in, or bundled with, this product are open source software (OSS) components, datasets and other third party components identified below. The license terms respectively governing the datasets and third-party components continue to govern those portions, and you agree to those license terms, which, when applicable, specifically limit any distribution. You may receive a copy of, distribute and/or modify any open source code for the OSS component under the terms of their respective licenses, which may be CC license and Apache 2.0 license. In the event of conflicts between Megagon Labs, Inc., license conditions and the Open Source Software license conditions, the Open Source Software conditions shall prevail with respect to the Open Source Software portions of the software. You agree not to, and are not permitted to, distribute actual datasets used with the OSS components listed below. You agree and are limited to distribute only links to datasets from known sources by listing them in the datasets overview table below. You are permitted to distribute derived datasets of data sets from known sources by including links to original dataset source in the datasets overview table below. You agree that any right to modify datasets originating from parties other than Megagon Labs, Inc. are governed by the respective third party’s license conditions. All OSS components and datasets are distributed WITHOUT ANY WARRANTY, without even implied warranty such as for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE, and without any liability to or claim against any Megagon Labs, Inc. entity other than as explicitly documented in this README document. You agree to cease using any part of the provided materials if you do not agree with the terms or the lack of any warranty herein. While Megagon Labs, Inc., makes commercially reasonable efforts to ensure that citations in this document are complete and accurate, errors may occur. If you see any error or omission, please help us improve this document by sending information to contact_oss@megagon.ai.

## Datasets

All datasets used within the product are listed below (including their copyright holders and the license information).

For datasets having different portions released under different licenses, please refer to the included upstream link specified for each of the respective datasets for identifications of dataset files released under the identified licenses.

| ID | OSS Component Name | Modified | Copyright Holder | Upstream Link | License |
| --- | --- | --- | --- | --- | --- |
| 1 | JD2Skills-BERT-XMLC | Yes | Taehoon Kim | https://github.com/WING-NUS/JD2Skills-BERT-XMLC | MIT License |

## Megagon Components

The primary Megagon dependency used by this repository is listed below.

| Component | Role | Upstream Link | Notes |
| --- | --- | --- | --- |
| Blue | Required agent-orchestration framework used by the planners and operators | https://github.com/megagonlabs/blue | Blue source is intentionally not included in this repository; install it separately before running L.A.K.E. |

## Open Source Software (OSS) Components

All direct third-party OSS components used within this repository are listed below (including their copyright holders and license information).

| ID | OSS Component Name | Modified | Copyright Holder | Upstream Link | License |
| --- | --- | --- | --- | --- | --- |
| 1 | streamlit | No | Snowflake Inc. | https://streamlit.io | Apache-2.0 |
| 2 | streamlit-agraph | No | Christian Klose | https://pypi.org/project/streamlit-agraph/ | MIT License |
| 3 | openai | No | OpenAI | https://github.com/openai/openai-python | Apache-2.0 |
