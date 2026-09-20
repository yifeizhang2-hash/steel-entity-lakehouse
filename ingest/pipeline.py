"""Run the bronze ingestion pipelines.

Each source gets its own dlt pipeline so that one failing source does not hold the
others hostage and so that each can be re-run on its own.

The output of this module is a set of **Iceberg tables**, registered in the local
SQLite catalog. It deliberately returns row counts and table locations, never a
DataFrame: see docs/adr/ADR-002-dataframe-boundary.md.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dlt
from dlt.destinations import filesystem

from ingest.config import (
    BRONZE_NAMESPACE,
    BRONZE_TABLES,
    DLT_PIPELINES_DIR,
    WAREHOUSE_ROOT,
)
from ingest.sources import (
    furnace_years_resource,
    gspt_plants_resource,
    gspt_production_resource,
    plantgen_resource,
)
from lakehouse.catalog import register_bronze_tables

DATASET_NAME = BRONZE_NAMESPACE

RESOURCE_FACTORIES = {
    "plantgen": plantgen_resource,
    "gspt_plants": gspt_plants_resource,
    "gspt_production": gspt_production_resource,
    "furnace_years": furnace_years_resource,
}


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows: int
    metadata_location: str


def _destination(warehouse: Path) -> Any:
    warehouse.mkdir(parents=True, exist_ok=True)
    return filesystem(bucket_url=f"file://{warehouse.resolve()}")


def run(
    tables: tuple[str, ...] = BRONZE_TABLES,
    warehouse: Path | None = None,
    pipelines_dir: Path | None = None,
    catalog_db: Path | None = None,
) -> list[LoadResult]:
    """Load the named bronze tables and register them in the Iceberg catalog."""
    wh = Path(warehouse or WAREHOUSE_ROOT)
    pipes = Path(pipelines_dir or DLT_PIPELINES_DIR)
    pipes.mkdir(parents=True, exist_ok=True)

    row_counts: dict[str, int] = {}
    for name in tables:
        pipeline = dlt.pipeline(
            pipeline_name=f"steel_bronze_{name}",
            destination=_destination(wh),
            dataset_name=DATASET_NAME,
            pipelines_dir=str(pipes),
        )
        info = pipeline.run(RESOURCE_FACTORIES[name]())
        row_counts[name] = _rows_loaded(info, name)

    registered = register_bronze_tables(
        tuple(tables), dataset_dir=wh / DATASET_NAME, db_path=catalog_db, warehouse=wh
    )
    return [
        LoadResult(
            table=name,
            rows=row_counts[name],
            metadata_location=registered[f"{BRONZE_NAMESPACE}.{name}"],
        )
        for name in tables
    ]


def _rows_loaded(info: Any, table: str) -> int:
    """Pull the row count for `table` out of a dlt LoadInfo."""
    total = 0
    for package in info.load_packages:
        for job in package.jobs.get("completed_jobs", []):
            metrics = getattr(job, "job_file_info", None)
            if metrics is not None and metrics.table_name == table:
                total += 1
    # dlt does not expose a row count per job in every destination; fall back to the
    # trace metrics when it is available.
    try:
        row_counts = info.pipeline.last_trace.last_normalize_info.row_counts
        return int(row_counts.get(table, total))
    except Exception:  # noqa: BLE001 - counts are reporting only, never correctness
        return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load the bronze Iceberg tables.")
    parser.add_argument(
        "--tables",
        nargs="*",
        default=list(BRONZE_TABLES),
        choices=list(BRONZE_TABLES),
        help="Subset of tables to load (default: all).",
    )
    args = parser.parse_args(argv)
    for result in run(tuple(args.tables)):
        print(f"{result.table:16s} rows={result.rows:>7d}  {result.metadata_location}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
