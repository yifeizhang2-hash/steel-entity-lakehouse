"""Shared fixtures.

Tests that touch real data read the copies committed under `data/raw/`; tests that
need to mutate a source get a temporary copy so the originals stay untouched.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from ingest.config import (
    FURNACE_FILES,
    GSPT_DIR,
    GSPT_XLSX,
    OWNER_FILLED_DIR,
    PLANTGEN_CSV,
)
from lakehouse.paths import LAKE_ROOT


@pytest.fixture(scope="session")
def plantgen_csv() -> Path:
    if not PLANTGEN_CSV.exists():
        pytest.skip(f"raw data not present: {PLANTGEN_CSV}")
    return PLANTGEN_CSV


@pytest.fixture(scope="session")
def gspt_xlsx() -> Path:
    if not GSPT_XLSX.exists():
        pytest.skip(f"raw data not present: {GSPT_XLSX}")
    return GSPT_XLSX


@pytest.fixture(scope="session")
def furnace_files() -> tuple[tuple[str, str, Path], ...]:
    missing = [str(p) for _, _, p in FURNACE_FILES if not p.exists()]
    if missing:
        pytest.skip(f"raw data not present: {missing}")
    return FURNACE_FILES


@pytest.fixture(scope="session")
def raw_vintages() -> tuple[str, ...]:
    """The source_version directories, or skip.

    The sensor discovers vintages by listing `data/raw/`, so any test asserting on
    what it finds needs those directories to exist. They are not redistributed, so
    on a fresh clone -- and in CI -- this skips rather than fails.
    """
    missing = [str(d) for d in (GSPT_DIR, OWNER_FILLED_DIR) if not d.is_dir()]
    if missing:
        pytest.skip(f"raw data not present: {missing}")
    return ("gspt_2024", "owner_filled_2025")


@pytest.fixture
def eaf_owner_copy(tmp_path: Path) -> Path:
    source = OWNER_FILLED_DIR / "eaf_owner_filled.csv"
    if not source.exists():
        pytest.skip(f"raw data not present: {source}")
    target = tmp_path / source.name
    shutil.copy(source, target)
    return target


@pytest.fixture
def plantgen_copy(tmp_path: Path, plantgen_csv: Path) -> Path:
    target = tmp_path / plantgen_csv.name
    shutil.copy(plantgen_csv, target)
    return target


def rewrite_header(path: Path, replacements: dict[str, str]) -> Path:
    """Rename columns in a CSV's header row in place, then return the path."""
    lines = path.read_text().splitlines(keepends=True)
    header = lines[0]
    for old, new in replacements.items():
        if old not in header:
            raise AssertionError(f"header of {path.name} does not contain {old!r}")
        header = header.replace(old, new, 1)
    lines[0] = header
    path.write_text("".join(lines))
    return path


def drop_column(path: Path, column: str) -> Path:
    """Remove a column (header plus every value) from a CSV, then return the path."""
    import csv

    with path.open(newline="") as handle:
        rows = list(csv.reader(handle))
    if column not in rows[0]:
        raise AssertionError(f"header of {path.name} does not contain {column!r}")
    index = rows[0].index(column)
    trimmed = [row[:index] + row[index + 1 :] for row in rows]
    with path.open("w", newline="") as handle:
        csv.writer(handle).writerows(trimmed)
    return path


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip the `slow` tests when the pipeline's inputs are absent.

    `slow` means "runs the full pipeline end to end", and the full pipeline reads the
    source data, which is not redistributed. On a fresh clone -- and in CI -- these skip
    rather than fail. Guarding here rather than in each fixture keeps the rule in one
    place: it is a property of the marker, not of any one test.

    The lake is checked too because some tests build throwaway lakes, so its directory
    can exist while holding nothing a real run could use.
    """
    del config
    if PLANTGEN_CSV.exists() and (LAKE_ROOT / "catalog.db").exists():
        return
    reason = pytest.mark.skip(
        reason=f"raw data or built lake not present ({PLANTGEN_CSV.parent}); "
        "run `make ingest geo resolve` with the sources in place"
    )
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(reason)
