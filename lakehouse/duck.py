"""DuckDB access to the local Iceberg tables.

This is the read side of the storage boundary in ADR-002. Downstream layers call
`connect()` and query the bronze views; they never import anything from `ingest/` and
never receive a Polars frame.

`iceberg_scan` is pointed at the exact metadata file recorded in the SQLite catalog
rather than at the table directory. DuckDB refuses directory scans unless
`unsafe_enable_version_guessing` is set, and rightly so: guessing the newest metadata
file off the filesystem can read an uncommitted snapshot. Going through the catalog
means the view always resolves to the snapshot the catalog considers current.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from lakehouse.catalog import metadata_locations


def connect(
    db_path: Path | None = None, warehouse: Path | None = None
) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB with every bronze Iceberg table exposed as a view.

    Views are named after the table (``plantgen``, ``gspt_plants``, ...) inside a
    ``raw`` schema, so a query reads ``select * from raw.plantgen``.
    """
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    for qualified, metadata in metadata_locations(db_path, warehouse).items():
        namespace, table = qualified.split(".", 1)
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{namespace}"')
        con.execute(
            f'CREATE OR REPLACE VIEW "{namespace}"."{table}" AS '
            f"SELECT * FROM iceberg_scan('{metadata}')"
        )
    return con


def table_counts(db_path: Path | None = None, warehouse: Path | None = None) -> dict[str, int]:
    """Row count of every table in the lake, read back through DuckDB."""
    con = connect(db_path, warehouse)
    try:
        counts: dict[str, int] = {}
        for qualified in metadata_locations(db_path, warehouse):
            namespace, table = qualified.split(".", 1)
            counts[qualified] = con.execute(
                f'SELECT count(*) FROM "{namespace}"."{table}"'
            ).fetchone()[0]
        return counts
    finally:
        con.close()
