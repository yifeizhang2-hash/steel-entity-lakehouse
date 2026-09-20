"""Tests for the two-row-header parse of the `Yearly Production` sheet."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ingest.excel import YearlyProductionHeaderError, read_yearly_production_long


@pytest.fixture
def synthetic_sheet(tmp_path: Path) -> Path:
    """A miniature copy of the real sheet's two-row header shape."""
    rows = [
        [None, None, 2019, None, 2020, None],
        [
            "Plant ID",
            "Plant name (English)",
            "Crude steel production 2019 (ttpa)",
            "BOF steel production 2019 (ttpa)",
            "Crude steel production 2020 (ttpa)",
            "BOF steel production 2020 (ttpa)",
        ],
        ["P1", "Alpha works", 1000, "unknown", 1100.5, None],
        ["P2", "Beta works", None, 20, "unknown", ">0"],
    ]
    path = tmp_path / "synthetic.xlsx"
    pd.DataFrame(rows).to_excel(path, sheet_name="Yearly Production", index=False, header=False)
    return path


class TestUnpivot:
    def test_produces_plant_year_metric_value(self, synthetic_sheet: Path) -> None:
        records = read_yearly_production_long(synthetic_sheet)
        assert {tuple(sorted(r)) for r in records} == {
            (
                "metric",
                "plant_id",
                "plant_name_english",
                "value_raw",
                "value_ttpa",
                "year",
            )
        }

    def test_empty_cells_are_dropped_and_sentinels_are_kept(self, synthetic_sheet: Path) -> None:
        records = read_yearly_production_long(synthetic_sheet)
        keyed = {(r["plant_id"], r["year"], r["metric"]): r for r in records}
        # 8 cells, 2 of them empty.
        assert len(records) == 6
        assert ("P1", 2020, "bof_steel") not in keyed
        assert keyed[("P1", 2019, "crude_steel")]["value_ttpa"] == 1000.0
        assert keyed[("P1", 2019, "bof_steel")]["value_ttpa"] is None
        assert keyed[("P1", 2019, "bof_steel")]["value_raw"] == "unknown"
        assert keyed[("P2", 2020, "bof_steel")]["value_raw"] == ">0"
        assert keyed[("P2", 2020, "bof_steel")]["value_ttpa"] is None

    def test_year_comes_from_both_header_rows(self, synthetic_sheet: Path) -> None:
        years = {record["year"] for record in read_yearly_production_long(synthetic_sheet)}
        assert years == {2019, 2020}


class TestHeaderValidation:
    def _write(self, tmp_path: Path, rows: list[list]) -> Path:
        path = tmp_path / "broken.xlsx"
        pd.DataFrame(rows).to_excel(path, sheet_name="Yearly Production", index=False, header=False)
        return path

    def test_banner_year_disagreeing_with_the_label_is_an_error(self, tmp_path: Path) -> None:
        """A shifted merged banner would otherwise mis-date every value under it."""
        path = self._write(
            tmp_path,
            [
                [None, None, 2018],
                ["Plant ID", "Plant name (English)", "Crude steel production 2019 (ttpa)"],
                ["P1", "Alpha works", 10],
            ],
        )
        with pytest.raises(YearlyProductionHeaderError, match="year banner"):
            read_yearly_production_long(path)

    def test_missing_plant_id_column_is_an_error(self, tmp_path: Path) -> None:
        path = self._write(
            tmp_path,
            [
                [None, 2019],
                ["Plant name (English)", "Crude steel production 2019 (ttpa)"],
                ["Alpha works", 10],
            ],
        )
        with pytest.raises(YearlyProductionHeaderError, match="Plant ID"):
            read_yearly_production_long(path)

    def test_unrecognised_metric_is_an_error(self, tmp_path: Path) -> None:
        path = self._write(
            tmp_path,
            [
                [None, None, 2019],
                ["Plant ID", "Plant name (English)", "Tungsten production 2019 (ttpa)"],
                ["P1", "Alpha works", 10],
            ],
        )
        with pytest.raises(YearlyProductionHeaderError, match="metric"):
            read_yearly_production_long(path)


class TestRealSheet:
    def test_shape_of_the_real_unpivot(self, gspt_xlsx: Path) -> None:
        """The sheet holds 1004 plant rows, not 1005.

        Reading it with a single header row makes the second header row look like data,
        which is where the 1005 figure comes from. Of the 1004 real rows, 13 have every
        one of their 28 metric cells empty and so contribute no long-table record,
        leaving 991 plants with at least one reported value.
        """
        records = read_yearly_production_long(gspt_xlsx)
        assert len(records) == 12409
        assert len({r["plant_id"] for r in records}) == 991
        assert {r["year"] for r in records} == {2019, 2020, 2021, 2022}
        assert {r["metric"] for r in records} == {
            "crude_steel",
            "bof_steel",
            "eaf_steel",
            "ohf_steel",
            "iron",
            "bf",
            "dri",
        }
