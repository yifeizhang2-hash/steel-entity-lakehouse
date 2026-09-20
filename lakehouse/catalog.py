"""Local Iceberg catalog: a SQLite file plus a warehouse directory.

No DataFrame library is imported here. This module is the *storage boundary* that
ADR-002 describes: `ingest/` writes through it, everything downstream reads through it.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

import pyarrow
from pyiceberg.catalog.sql import SqlCatalog

from lakehouse.paths import BRONZE_NAMESPACE, CATALOG_DB, WAREHOUSE_ROOT

CATALOG_NAME = "steel_lakehouse"


def catalog_uri(db_path: Path | None = None) -> str:
    return f"sqlite:///{Path(db_path or CATALOG_DB).resolve()}"


def get_catalog(db_path: Path | None = None, warehouse: Path | None = None) -> SqlCatalog:
    """Open (creating if needed) the local SQLite-backed Iceberg catalog."""
    db = Path(db_path or CATALOG_DB)
    wh = Path(warehouse or WAREHOUSE_ROOT)
    db.parent.mkdir(parents=True, exist_ok=True)
    wh.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        CATALOG_NAME,
        **{"uri": catalog_uri(db), "warehouse": f"file://{wh.resolve()}"},
    )
    catalog.create_namespace_if_not_exists(BRONZE_NAMESPACE)
    return catalog


def latest_metadata_file(table_dir: Path) -> Path:
    """Return the newest ``*.metadata.json`` written for an Iceberg table.

    dlt writes the table files itself and manages its own transient catalog, so the
    persistent catalog is rebuilt by pointing it at whatever metadata dlt committed.
    Sorting is by the numeric version prefix Iceberg puts on the file name
    (``00003-<uuid>.metadata.json``), not by mtime, so the result is deterministic.
    """
    candidates = glob.glob(str(Path(table_dir) / "metadata" / "*.metadata.json"))
    if not candidates:
        raise FileNotFoundError(f"no Iceberg metadata under {table_dir}")

    def version(path: str) -> tuple[int, str]:
        stem = os.path.basename(path).split("-", 1)[0]
        return (int(stem) if stem.isdigit() else -1, os.path.basename(path))

    return Path(max(candidates, key=version))


def register_bronze_tables(
    table_names: tuple[str, ...],
    dataset_dir: Path,
    db_path: Path | None = None,
    warehouse: Path | None = None,
) -> dict[str, str]:
    """(Re-)register each loaded table in the SQLite catalog.

    Returns a mapping of ``"<namespace>.<table>" -> metadata location``.
    """
    catalog = get_catalog(db_path, warehouse)
    registered: dict[str, str] = {}
    for name in table_names:
        table_dir = Path(dataset_dir) / name
        metadata = latest_metadata_file(table_dir)
        identifier = (BRONZE_NAMESPACE, name)
        if catalog.table_exists(identifier):
            catalog.drop_table(identifier)
        table = catalog.register_table(identifier, str(metadata))
        registered[f"{BRONZE_NAMESPACE}.{name}"] = table.metadata_location
    return registered


def metadata_locations(
    db_path: Path | None = None,
    warehouse: Path | None = None,
    namespaces: tuple[str, ...] | None = None,
) -> dict[str, str]:
    """Current metadata location of every table, across every namespace by default.

    Derived layers write into their own namespace (`geo`), so listing only the bronze
    one would hide them from DuckDB.
    """
    catalog = get_catalog(db_path, warehouse)
    wanted = (
        namespaces if namespaces is not None else tuple(ns[0] for ns in catalog.list_namespaces())
    )
    locations: dict[str, str] = {}
    for namespace in wanted:
        for ns, name in catalog.list_tables(namespace):
            locations[f"{ns}.{name}"] = catalog.load_table((ns, name)).metadata_location
    return locations


def write_table(
    namespace: str,
    name: str,
    table: pyarrow.Table,
    db_path: Path | None = None,
    warehouse: Path | None = None,
) -> str:
    """Create or replace an Iceberg table from an Arrow table; return its metadata location.

    Used by derived layers such as `geo/`, which produce a table rather than ingest one,
    so they do not go through dlt. Replace rather than append keeps a re-run idempotent.
    """
    catalog = get_catalog(db_path, warehouse)
    catalog.create_namespace_if_not_exists(namespace)
    identifier = (namespace, name)
    if catalog.table_exists(identifier):
        catalog.drop_table(identifier)
    iceberg_table = catalog.create_table(identifier, schema=table.schema)
    iceberg_table.append(table)
    return iceberg_table.metadata_location
