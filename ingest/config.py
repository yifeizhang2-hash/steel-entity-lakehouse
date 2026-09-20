"""Source-file locations and constants for the ingestion layer.

Nothing here imports a DataFrame library. The generic filesystem paths it re-exports
come from `lakehouse.paths`; what belongs to this module is which source files exist and
what the bronze tables are called.
"""

from __future__ import annotations

from pathlib import Path

# Paths live in the neutral `lakehouse` layer so that `geo/` can import them without
# reaching into the ingestion layer. See docs/adr/ADR-002-dataframe-boundary.md.
from lakehouse.paths import (
    BRONZE_NAMESPACE,
    CATALOG_DB,
    DLT_PIPELINES_DIR,
    LAKE_ROOT,
    RAW_ROOT,
    REPO_ROOT,
    WAREHOUSE_ROOT,
)

__all__ = [
    "BRONZE_NAMESPACE",
    "BRONZE_TABLES",
    "CATALOG_DB",
    "DLT_PIPELINES_DIR",
    "FURNACE_FILES",
    "GSPT_DIR",
    "GSPT_XLSX",
    "LAKE_ROOT",
    "OWNER_FILLED_DIR",
    "PLANTGEN_CSV",
    "RAW_ROOT",
    "REPO_ROOT",
    "SHEET_STEEL_PLANTS",
    "SHEET_YEARLY_PRODUCTION",
    "WAREHOUSE_ROOT",
]

GSPT_DIR = RAW_ROOT / "source_version=gspt_2024"
OWNER_FILLED_DIR = RAW_ROOT / "source_version=owner_filled_2025"

PLANTGEN_CSV = GSPT_DIR / "PLANTGEN.csv"
GSPT_XLSX = GSPT_DIR / "Global-Steel-Plant-Tracker-April-2024-Standard-Copy-V1.xlsx"

SHEET_STEEL_PLANTS = "Steel Plants"
SHEET_YEARLY_PRODUCTION = "Yearly Production"

# The four furnace-level CSVs. `source_version` is carried into the table so that the
# two vintages of the same records stay distinguishable; `owner_filled_2025` is the
# authoritative one (it is the only vintage where `owner` is populated).
FURNACE_FILES: tuple[tuple[str, str, Path], ...] = (
    ("BOF", "gspt_2024", GSPT_DIR / "bof_mini.csv"),
    ("EAF", "gspt_2024", GSPT_DIR / "eaf_mini.csv"),
    ("BOF", "owner_filled_2025", OWNER_FILLED_DIR / "bof_owner_filled.csv"),
    ("EAF", "owner_filled_2025", OWNER_FILLED_DIR / "eaf_owner_filled.csv"),
)

BRONZE_TABLES: tuple[str, ...] = (
    "plantgen",
    "gspt_plants",
    "gspt_production",
    "furnace_years",
)
