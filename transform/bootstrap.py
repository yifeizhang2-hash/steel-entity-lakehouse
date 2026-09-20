"""Expose the Iceberg lake to dbt as DuckDB views.

[no DataFrame library] Nothing here imports Polars, pandas or `ingest`.
See docs/adr/ADR-002-dataframe-boundary.md.

dbt-duckdb works against a DuckDB database file. The lake's tables live in Iceberg, and
the only supported way to read them is `iceberg_scan` against the exact metadata file
the catalog considers current. That location changes every time a pipeline re-runs, so
the views are **rebuilt from the catalog on every invocation** rather than created once
and left to go stale pointing at a snapshot nobody has written to in weeks.

This is the same storage boundary the rest of the project uses, just materialised as a
database file so that dbt can attach to it.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from lakehouse.catalog import metadata_locations
from lakehouse.paths import LAKE_ROOT

DEFAULT_DB = LAKE_ROOT / "dbt.duckdb"

# Schemas the views land in. dbt sources read from these.
SOURCE_SCHEMAS = ("raw", "geo", "resolution")


def bootstrap(
    db_file: Path | None = None,
    catalog_db: Path | None = None,
    warehouse: Path | None = None,
) -> dict[str, str]:
    """(Re)create a view over every Iceberg table. Returns what was created."""
    target = Path(db_file or DEFAULT_DB)
    target.parent.mkdir(parents=True, exist_ok=True)

    locations = metadata_locations(catalog_db, warehouse)
    if not locations:
        raise RuntimeError(
            "the lake is empty: run `make ingest` (and `make geo`, `make resolve`) first"
        )

    con = duckdb.connect(str(target))
    try:
        con.execute("INSTALL iceberg; LOAD iceberg;")
        created: dict[str, str] = {}
        for qualified, metadata in sorted(locations.items()):
            namespace, table = qualified.split(".", 1)
            if namespace not in SOURCE_SCHEMAS:
                continue
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{namespace}"')
            con.execute(
                f'CREATE OR REPLACE VIEW "{namespace}"."{table}" AS '
                f"SELECT * FROM iceberg_scan('{metadata}')"
            )
            created[qualified] = metadata
        con.commit()
        return created
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Point a DuckDB file at the Iceberg lake.")
    parser.add_argument("--db", type=Path, default=None)
    args = parser.parse_args(argv)
    created = bootstrap(args.db)
    for qualified in sorted(created):
        print(f"view {qualified}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
