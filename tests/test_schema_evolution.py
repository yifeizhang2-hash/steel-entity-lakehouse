"""Schema evolution: what must be absorbed, and what must stop the build.

The two behaviours are deliberately different.

* A source that changes how it *spells* a column is absorbed. This is not a
  hypothetical: the same furnace records ship as `Corrected C/L` in one vintage and
  `Corrected.C.L` in the other, and both must land in one column.
* A source that *loses* a required column halts the build. Silently loading a table
  with a missing key is worse than not loading at all.
"""

from __future__ import annotations

from pathlib import Path

import dlt
import polars as pl
import pyarrow as pa
import pytest
from dlt.common.schema.exceptions import DataValidationError
from dlt.destinations import filesystem
from pandera.errors import SchemaError, SchemaErrors

from ingest.contracts import PLANTGEN_SCHEMA, dlt_columns
from ingest.sources import (
    SCHEMA_CONTRACT,
    MissingSourceColumnError,
    read_furnace_file,
    read_plantgen,
)
from tests.conftest import drop_column, rewrite_header


class TestRenameIsAbsorbed:
    def test_r_mangled_and_original_spellings_produce_one_schema(
        self, furnace_files: tuple[tuple[str, str, Path], ...]
    ) -> None:
        """The two spellings that really exist in the repo already agree."""
        by_name = {path.name: (kind, version, path) for kind, version, path in furnace_files}
        r_style = read_furnace_file(*by_name["eaf_owner_filled.csv"])  # Corrected.C.L
        original = read_furnace_file(*by_name["eaf_mini.csv"])  # Corrected C/L
        assert r_style.columns == original.columns
        assert r_style.schema == original.schema
        assert "corrected_cl" in r_style.columns

    @pytest.mark.parametrize(
        ("old", "new"),
        [
            ("Corrected.C.L", "Corrected C/L"),
            ("Corrected.C.L", "Corrected-C-L"),  # a spelling neither vintage uses
            ("No..of.furnaces", "No. of furnaces"),
            ("Power..kWh..metric.ton.", "Power (kWh/metric ton)"),
            ("Year.List", "Year List"),
        ],
    )
    def test_renaming_a_column_changes_nothing_downstream(
        self, eaf_owner_copy: Path, old: str, new: str
    ) -> None:
        baseline = read_furnace_file("EAF", "owner_filled_2025", eaf_owner_copy)
        renamed = read_furnace_file(
            "EAF", "owner_filled_2025", rewrite_header(eaf_owner_copy, {old: new})
        )
        assert renamed.columns == baseline.columns
        assert renamed.equals(baseline)


class TestMissingColumnHaltsTheBuild:
    @pytest.mark.parametrize("column", ["ZipCode", "PlantID", "StCntyFIPS", "ShutdownYr"])
    def test_dropping_a_required_plantgen_column_raises(
        self, plantgen_copy: Path, column: str
    ) -> None:
        drop_column(plantgen_copy, column)
        with pytest.raises((SchemaError, SchemaErrors, MissingSourceColumnError)):
            read_plantgen(plantgen_copy)

    @pytest.mark.parametrize("column", ["ID", "Year", "Corrected.C.L"])
    def test_dropping_a_required_furnace_column_raises(
        self, eaf_owner_copy: Path, column: str
    ) -> None:
        drop_column(eaf_owner_copy, column)
        with pytest.raises((SchemaError, SchemaErrors, MissingSourceColumnError)):
            read_furnace_file("EAF", "owner_filled_2025", eaf_owner_copy)

    def test_the_contract_names_the_missing_column(self, plantgen_copy: Path) -> None:
        drop_column(plantgen_copy, "ZipCode")
        with pytest.raises(Exception, match="zip_code"):
            read_plantgen(plantgen_copy)


@pytest.mark.slow
class TestStoredTableIsFrozen:
    """dlt's contract is the second gate: it protects the Iceberg table itself.

    Pandera stops a bad *input*. This stops a bad *write* against a table that already
    exists, even if someone relaxes or bypasses the reader.

    Measured limitation, worth knowing before relying on it: with dlt 1.30 the
    ``"columns": "freeze"`` contract is evaluated against the *persisted* schema, so
    on the very first load of a table an undeclared column is accepted even though the
    resource declares its columns up front. From the second load on it is rejected.
    That is why the first line of defence is Pandera `strict=True` plus an explicit
    projection in `ingest.sources`, and the contract is the second.
    """

    def _pipeline(self, tmp_path: Path, name: str):
        return dlt.pipeline(
            pipeline_name=name,
            destination=filesystem(bucket_url=f"file://{(tmp_path / 'warehouse').resolve()}"),
            dataset_name="raw",
            pipelines_dir=str(tmp_path / "dlt"),
        )

    def _resource(self, table: pa.Table):
        @dlt.resource(
            name="plantgen",
            write_disposition="replace",
            table_format="iceberg",
            columns=dlt_columns(PLANTGEN_SCHEMA),
            schema_contract=SCHEMA_CONTRACT,
        )
        def resource():
            yield table

        return resource()

    def test_an_unexpected_column_is_rejected_on_reload(
        self, tmp_path: Path, plantgen_csv: Path
    ) -> None:
        frame = read_plantgen(plantgen_csv).head(5)
        pipeline = self._pipeline(tmp_path, "contract_extra_column")

        first = pipeline.run(self._resource(frame.to_arrow()))
        assert not first.has_failed_jobs
        stored = set(pipeline.default_schema.tables["plantgen"]["columns"])
        assert "undeclared_column" not in stored

        smuggled = frame.with_columns(pl.lit("surprise").alias("undeclared_column"))
        with pytest.raises(Exception) as excinfo:
            pipeline.run(self._resource(smuggled.to_arrow()))
        cause = excinfo.value
        message = f"{cause}{getattr(cause, '__cause__', '')}"
        assert "undeclared_column" in message
        assert "DataValidationError" in message or isinstance(
            getattr(cause, "__cause__", None), DataValidationError
        )

    def test_the_declared_schema_loads(self, tmp_path: Path, plantgen_csv: Path) -> None:
        frame = read_plantgen(plantgen_csv).head(5)
        pipeline = self._pipeline(tmp_path, "contract_happy_path")
        info = pipeline.run(self._resource(frame.to_arrow()))
        assert not info.has_failed_jobs
        assert not pipeline.run(self._resource(frame.to_arrow())).has_failed_jobs
