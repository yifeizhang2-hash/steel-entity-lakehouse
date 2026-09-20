.DEFAULT_GOAL := help
UV ?= uv
RUN := $(UV) run

.PHONY: help install lint format test test-fast ingest geo geo-offline resolve dbt dbt-docs materialize dagster-ui queue label synthetic-labels evaluate evaluate-synthetic query clean clean-cache all

help:  ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Create the virtualenv and install everything, including dev tools.
	$(UV) sync --extra dev
	$(RUN) pre-commit install

lint:  ## Lint and check formatting. Must be clean.
	$(RUN) ruff check .
	$(RUN) ruff format --check .

format:  ## Apply formatting and autofixable lint rules.
	$(RUN) ruff check --fix .
	$(RUN) ruff format .

test:  ## Run the whole test suite, including the end-to-end lakehouse tests.
	$(RUN) pytest

test-fast:  ## Skip the tests that run full dlt pipelines.
	$(RUN) pytest -m "not slow"

ingest:  ## Build the four bronze Iceberg tables under lake/.
	$(RUN) python -m ingest.pipeline

geo:  ## Attach county FIPS to GSPT plants (downloads county boundaries once, then caches).
	$(RUN) python -m geo.enrich

geo-offline:  ## Same, but fail instead of downloading if the boundary cache is empty.
	$(RUN) python -m geo.enrich --offline

resolve:  ## Train the Splink linker and report blocking, calibration and weights.
	$(RUN) python -m resolution.report

dbt:  ## Point DuckDB at the Iceberg lake, then build and test every dbt model.
	$(RUN) python -m transform.bootstrap
	$(RUN) dbt deps --project-dir transform --profiles-dir transform
	$(RUN) dbt build --project-dir transform --profiles-dir transform

dbt-docs:  ## Generate and serve the dbt documentation site.
	$(RUN) dbt docs generate --project-dir transform --profiles-dir transform
	$(RUN) dbt docs serve --project-dir transform --profiles-dir transform

# Dagster refuses to start unless DAGSTER_HOME points at a directory that already
# exists, so it is created rather than assumed.
DAGSTER_HOME ?= $(PWD)/.dagster

materialize:  ## Run the whole pipeline through Dagster, with asset checks.
	@mkdir -p "$(DAGSTER_HOME)"
	DAGSTER_HOME="$(DAGSTER_HOME)" $(RUN) dagster asset materialize \
	    --select '*' -m orchestration.definitions --partition owner_filled_2025

dagster-ui:  ## Open the Dagster UI to browse the asset graph and check results.
	@mkdir -p "$(DAGSTER_HOME)"
	DAGSTER_HOME="$(DAGSTER_HOME)" $(RUN) dagster dev -m orchestration.definitions

queue:  ## Print the stratified labelling queue's composition.
	$(RUN) python -m labeling.sampling

label:  ## Open the Streamlit review UI. Decisions append to labeling/decisions.jsonl.
	$(RUN) streamlit run labeling/app.py

synthetic-labels:  ## Write SYNTHETIC labels to exercise the evaluation. Not ground truth.
	$(RUN) python -m labeling.synthetic

evaluate:  ## Score the model against the real labels (empty until a human labels).
	$(RUN) python -m labeling.evaluate

evaluate-synthetic:  ## Same, against the synthetic labels. Reports them AS synthetic.
	$(RUN) python -m labeling.evaluate --synthetic

query:  ## Print the row count of every table, read back through DuckDB.
	$(RUN) python -c "from lakehouse.duck import table_counts; \
	    [print(f'{k:24s} {v:>7d}') for k, v in sorted(table_counts().items())]"

clean:  ## Delete the lake. Everything in it is rebuildable.
	rm -rf lake

clean-cache:  ## Also delete the cached county boundaries, forcing a re-download.
	rm -rf geo/cache

all: lint ingest geo resolve dbt test query  ## Build the lake, then lint and test what landed.
