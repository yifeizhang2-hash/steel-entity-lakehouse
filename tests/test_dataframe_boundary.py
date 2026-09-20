"""Executable enforcement of ADR-002.

The rule is only worth writing down if something checks it. These tests parse the
import statements of every module in the repo and fail when a layer reaches for the
DataFrame library it is not allowed to use, or when `ingest/` hands a DataFrame to a
downstream caller.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from lakehouse.paths import REPO_ROOT

# Layers that sit downstream of ingestion in the DATA flow. They must receive their
# data through storage, so they may not import `ingest` at all.
DOWNSTREAM_PACKAGES = ("geo", "resolution", "labeling", "transform")

# Orchestration is not downstream; it is the layer that invokes the others. It has to
# import each layer's entry point to run it, so the rule for it is different -- and
# still enforced: it may call an entry point, but it may not reach into a layer's
# internals and it may not touch a DataFrame library itself.
ORCHESTRATION_PACKAGE = "orchestration"

INGEST_ENTRY_POINTS = {"ingest.pipeline", "ingest.config"}

# The single documented exception: the two-row-header parse of `Yearly Production`.
PANDAS_ALLOWED_IN_INGEST = {"excel.py"}


def _modules(package: str) -> list[Path]:
    directory = REPO_ROOT / package
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.rglob("*.py") if "__pycache__" not in p.parts)


def _imported_modules(path: Path) -> set[str]:
    """Fully-qualified module names this file imports."""
    tree = ast.parse(path.read_text(), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules.add(node.module)
    return modules


def _imported_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return roots


class TestIngestIsPolarsOnly:
    @pytest.mark.parametrize("path", _modules("ingest"), ids=lambda p: p.name)
    def test_pandas_only_in_the_documented_exception(self, path: Path) -> None:
        if "pandas" not in _imported_roots(path):
            return
        assert path.name in PANDAS_ALLOWED_IN_INGEST, (
            f"{path.name} imports pandas; only {sorted(PANDAS_ALLOWED_IN_INGEST)} may"
        )

    def test_the_exception_documents_itself(self) -> None:
        source = (REPO_ROOT / "ingest" / "excel.py").read_text()
        assert "WHY PANDAS HERE" in source
        assert "ADR-002" in source


class TestDownstreamIsPandasOnly:
    @pytest.mark.parametrize(
        "path",
        [p for package in DOWNSTREAM_PACKAGES for p in _modules(package)],
        ids=lambda p: f"{p.parent.name}/{p.name}",
    )
    def test_no_polars_downstream(self, path: Path) -> None:
        assert "polars" not in _imported_roots(path), (
            f"{path} imports polars; downstream layers read Iceberg through DuckDB"
        )

    @pytest.mark.parametrize(
        "path",
        [p for package in DOWNSTREAM_PACKAGES for p in _modules(package)],
        ids=lambda p: f"{p.parent.name}/{p.name}",
    )
    def test_no_reaching_into_ingest(self, path: Path) -> None:
        assert "ingest" not in _imported_roots(path), (
            f"{path} imports from ingest; the hand-off is the Iceberg table, not a function call"
        )


class TestPathsLiveInTheNeutralLayer:
    def test_geo_finds_its_cache_without_importing_ingest(self) -> None:
        """The reason `lakehouse.paths` exists: shared paths are not ingestion's to own."""
        from geo import download
        from lakehouse.paths import GEO_CACHE_DIR

        assert download.CACHE_DIR == GEO_CACHE_DIR


class TestOrchestrationCallsEntryPointsOnly:
    """The scheduler may start every layer; it may not reach inside one."""

    @pytest.mark.parametrize(
        "path", _modules(ORCHESTRATION_PACKAGE), ids=lambda p: f"{p.parent.name}/{p.name}"
    )
    def test_no_dataframe_library(self, path: Path) -> None:
        roots = _imported_roots(path)
        assert "polars" not in roots
        assert "pandas" not in roots

    @pytest.mark.parametrize(
        "path", _modules(ORCHESTRATION_PACKAGE), ids=lambda p: f"{p.parent.name}/{p.name}"
    )
    def test_only_ingest_entry_points_are_imported(self, path: Path) -> None:
        """`ingest.pipeline` is the published way to run a load. `ingest.sources` is not."""
        for module in _imported_modules(path):
            if module.split(".")[0] != "ingest":
                continue
            assert module in INGEST_ENTRY_POINTS, (
                f"{path} imports {module}; orchestration may only call "
                f"{sorted(INGEST_ENTRY_POINTS)}"
            )


class TestStorageLayerIsNeutral:
    @pytest.mark.parametrize("path", _modules("lakehouse"), ids=lambda p: p.name)
    def test_lakehouse_imports_no_dataframe_library(self, path: Path) -> None:
        roots = _imported_roots(path)
        assert "polars" not in roots
        assert "pandas" not in roots


class TestIngestReturnsNoDataFrames:
    """The public entry point must hand back table locations, not frames."""

    def test_pipeline_run_returns_load_results(self) -> None:
        import inspect

        from ingest.pipeline import LoadResult, run

        annotation = inspect.signature(run).return_annotation
        assert "LoadResult" in str(annotation)
        assert set(LoadResult.__annotations__) == {"table", "rows", "metadata_location"}
