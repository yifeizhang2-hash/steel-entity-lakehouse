"""Filesystem locations shared by every layer.

`lakehouse/` is the neutral storage boundary described in ADR-002: it imports no
DataFrame library, and both the Polars side and the pandas side may import from it.
Paths live here rather than in `ingest/` for exactly that reason -- `geo/` needs to know
where the repo root is, and it is not allowed to reach into the ingestion layer to find
out.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

RAW_ROOT = Path(os.environ.get("STEEL_RAW_ROOT", REPO_ROOT / "data" / "raw"))
LAKE_ROOT = Path(os.environ.get("STEEL_LAKE_ROOT", REPO_ROOT / "lake"))

WAREHOUSE_ROOT = LAKE_ROOT / "warehouse"
CATALOG_DB = LAKE_ROOT / "catalog.db"
DLT_PIPELINES_DIR = LAKE_ROOT / "dlt"

GEO_CACHE_DIR = Path(os.environ.get("STEEL_GEO_CACHE", REPO_ROOT / "geo" / "cache"))

# Iceberg namespaces. `raw` holds the faithful bronze tables; `geo` holds derived
# geographic facts. Named here so neither the ingest side nor the geo side owns them.
BRONZE_NAMESPACE = "raw"
GEO_NAMESPACE = "geo"
RESOLUTION_NAMESPACE = "resolution"
