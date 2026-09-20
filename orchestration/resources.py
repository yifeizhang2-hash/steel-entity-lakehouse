"""Shared configuration for the Dagster assets.

[no DataFrame library] Nothing here imports Polars or pandas.
"""

from __future__ import annotations

import os

from dagster import ConfigurableResource

from lakehouse.paths import LAKE_ROOT, REPO_ROOT

DBT_PROJECT_DIR = REPO_ROOT / "transform"
DBT_PROFILES_DIR = REPO_ROOT / "transform"
DBT_DATABASE = LAKE_ROOT / "dbt.duckdb"

# dbt's profile takes the database path from `STEEL_DBT_DB`, defaulting to a path
# relative to the invocation directory -- which is the repo root when the Makefile runs
# it. Dagster's dbt resource runs dbt with cwd set to the project directory instead, so
# the relative default would resolve to `transform/lake/dbt.duckdb`. Setting it
# absolutely here makes both entry points land on the same file.
os.environ.setdefault("STEEL_DBT_DB", str(DBT_DATABASE))

# The bronze tables are partitioned by the vintage of the files they came from. The two
# vintages are not interchangeable: `gspt_2024` has intact facility names and no owners,
# `owner_filled_2025` has every owner and lost every non-ASCII character. See ADR-003.
SOURCE_VERSIONS = ("gspt_2024", "owner_filled_2025")


class LakeConfig(ConfigurableResource):
    """Where the lake lives. Defaults match the Makefile."""

    catalog_db: str = str(LAKE_ROOT / "catalog.db")
    warehouse: str = str(LAKE_ROOT / "warehouse")
