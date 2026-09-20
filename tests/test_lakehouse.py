"""End-to-end: run the pipelines into a throwaway lake and query it with DuckDB.

Marked `slow` because it runs all four dlt pipelines. It is the only test that proves
the storage boundary actually holds: nothing here imports Polars or pandas, and every
assertion is made against what DuckDB reads back out of Iceberg.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingest.config import BRONZE_TABLES
from ingest.pipeline import run
from lakehouse.duck import connect, table_counts

pytestmark = pytest.mark.slow

EXPECTED_ROWS = {
    "raw.plantgen": 197,
    "raw.gspt_plants": 1848,
    "raw.gspt_production": 12409,
    "raw.furnace_years": 4352,
}


@pytest.fixture(scope="module")
def lake(tmp_path_factory, plantgen_csv, gspt_xlsx, furnace_files) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("lake")
    warehouse, catalog_db = root / "warehouse", root / "catalog.db"
    results = run(
        tables=BRONZE_TABLES,
        warehouse=warehouse,
        pipelines_dir=root / "dlt",
        catalog_db=catalog_db,
    )
    assert {r.table for r in results} == set(BRONZE_TABLES)
    return catalog_db, warehouse


def test_all_four_tables_are_registered(lake: tuple[Path, Path]) -> None:
    catalog_db, warehouse = lake
    assert table_counts(catalog_db, warehouse) == EXPECTED_ROWS


def test_duckdb_reads_every_table(lake: tuple[Path, Path]) -> None:
    catalog_db, warehouse = lake
    con = connect(catalog_db, warehouse)
    try:
        for qualified, expected in EXPECTED_ROWS.items():
            namespace, table = qualified.split(".")
            assert con.execute(f'select count(*) from "{namespace}"."{table}"').fetchone()[0] == (
                expected
            )
    finally:
        con.close()


def test_identifiers_survive_the_round_trip(lake: tuple[Path, Path]) -> None:
    """The whole point of the ingest layer: a ZIP is still text with its leading zero."""
    catalog_db, warehouse = lake
    con = connect(catalog_db, warehouse)
    try:
        types = dict(
            (row[0], row[1])
            for row in con.execute("describe select * from raw.plantgen").fetchall()
        )
        assert types["zip_code"] == "VARCHAR"
        assert types["st_cnty_fips"] == "VARCHAR"
        assert types["plant_id"] == "VARCHAR"
        assert types["shutdown_yr"] == "BIGINT"

        padded = con.execute(
            "select zip_code from raw.plantgen where zip_code like '0%' order by zip_code"
        ).fetchall()
        assert [row[0] for row in padded] == ["06607", "08077", "08862", "08872"]

        assert (
            con.execute("select st_cnty_fips from raw.plantgen where plant_id = '156'").fetchone()[
                0
            ]
            == "09001"
        )
    finally:
        con.close()


def test_missing_years_are_sql_nulls(lake: tuple[Path, Path]) -> None:
    catalog_db, warehouse = lake
    con = connect(catalog_db, warehouse)
    try:
        nulls = con.execute(
            "select count(*) from raw.plantgen where shutdown_yr is null"
        ).fetchone()[0]
        assert nulls == 126
        # A NaN would answer 0 here and quietly break every join and filter downstream.
        nans = con.execute(
            "select count(*) from raw.plantgen "
            "where shutdown_yr is not null and isnan(cast(shutdown_yr as double))"
        ).fetchone()[0]
        assert nans == 0
    finally:
        con.close()


def test_alias_lists_are_queryable_arrays(lake: tuple[Path, Path]) -> None:
    catalog_db, warehouse = lake
    con = connect(catalog_db, warehouse)
    try:
        row = con.execute(
            "select corrected_cl from raw.furnace_years "
            "where furnace_key = 'EAF:29' and year = 2012 and source_version = 'owner_filled_2025' "
            "limit 1"
        ).fetchone()
        assert isinstance(row[0], list)
        assert any("AltaSteel" in alias for alias in row[0])

        multi = con.execute(
            "select count(*) from raw.furnace_years where len(corrected_cl) > 1"
        ).fetchone()[0]
        assert multi > 0
    finally:
        con.close()


def test_production_is_long_and_joins_to_plants(lake: tuple[Path, Path]) -> None:
    catalog_db, warehouse = lake
    con = connect(catalog_db, warehouse)
    try:
        by_year = con.execute(
            "select year, count(*) from raw.gspt_production group by 1 order by 1"
        ).fetchall()
        assert by_year == [(2019, 3144), (2020, 3104), (2021, 3100), (2022, 3061)]

        # The two GSPT sheets share Plant ID, so this join needs no entity resolution.
        joined = con.execute(
            "select count(*) from raw.gspt_production p "
            "join (select distinct plant_id from raw.gspt_plants "
            "      where country_area in ('United States','Canada')) n using (plant_id)"
        ).fetchone()[0]
        assert joined == 764
    finally:
        con.close()


def test_rerun_is_idempotent(lake: tuple[Path, Path]) -> None:
    """`replace` disposition plus catalog re-registration must not double the rows."""
    catalog_db, warehouse = lake
    run(
        tables=("plantgen",),
        warehouse=warehouse,
        pipelines_dir=warehouse.parent / "dlt",
        catalog_db=catalog_db,
    )
    assert table_counts(catalog_db, warehouse)["raw.plantgen"] == 197
