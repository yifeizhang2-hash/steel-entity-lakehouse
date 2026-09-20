"""Behavioural tests for the four bronze readers, against the real source files.

These assertions double as the project's record of what the sources actually contain.
If a number here changes, the data changed.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from ingest.contracts import PRODUCTION_METRICS
from ingest.sources import (
    read_furnace_years,
    read_gspt_plants,
    read_gspt_production,
    read_plantgen,
)


@pytest.fixture(scope="module")
def plantgen(plantgen_csv: Path) -> pl.DataFrame:
    return read_plantgen(plantgen_csv)


@pytest.fixture(scope="module")
def plants(gspt_xlsx: Path) -> pl.DataFrame:
    return read_gspt_plants(gspt_xlsx)


@pytest.fixture(scope="module")
def production(gspt_xlsx: Path) -> pl.DataFrame:
    return read_gspt_production(gspt_xlsx)


@pytest.fixture(scope="module")
def furnaces(furnace_files) -> pl.DataFrame:
    return read_furnace_years(furnace_files)


class TestPlantgen:
    def test_shape_and_natural_key(self, plantgen: pl.DataFrame) -> None:
        assert plantgen.height == 197
        assert plantgen["plant_id"].n_unique() == 197

    def test_identifier_columns_are_text(self, plantgen: pl.DataFrame) -> None:
        for column in ("plant_id", "zip_code", "st_cnty_fips", "state_fips", "cnty_fips"):
            assert plantgen.schema[column] == pl.Utf8, column

    def test_zip_codes_are_five_characters(self, plantgen: pl.DataFrame) -> None:
        zips = plantgen["zip_code"].drop_nulls()
        assert zips.len() == 180
        assert zips.str.len_chars().unique().to_list() == [5]

    def test_leading_zero_zips_are_restored(self, plantgen: pl.DataFrame) -> None:
        """The PLANTGEN file itself stores these four without their leading zero."""
        padded = (
            plantgen.filter(pl.col("zip_code").str.starts_with("0"))
            .sort("plant_id")
            .select("plant_id", "zip_code")
            .to_dicts()
        )
        assert {row["zip_code"] for row in padded} == {"06607", "08077", "08862", "08872"}

    def test_county_fips_keeps_its_leading_zero(self, plantgen: pl.DataFrame) -> None:
        assert plantgen.filter(pl.col("plant_id") == "156")["st_cnty_fips"][0] == "09001"
        assert plantgen["st_cnty_fips"].null_count() == 0

    def test_missing_years_are_null_not_nan(self, plantgen: pl.DataFrame) -> None:
        """A NaN would be a float that is not equal to itself; a null joins correctly."""
        assert plantgen.schema["shutdown_yr"] == pl.Int64
        assert plantgen.schema["plant_start_yr"] == pl.Int64
        assert plantgen["shutdown_yr"].null_count() == 126
        assert plantgen["shutdown_yr"].drop_nulls().len() == 71
        assert plantgen["plant_start_yr"].drop_nulls().len() == 89
        assert (
            plantgen["shutdown_yr"].is_nan().sum() is None
            or not plantgen["shutdown_yr"].cast(pl.Float64).is_nan().any()
        )

    def test_plant_name_is_mostly_the_city_name(self, plantgen: pl.DataFrame) -> None:
        """PlantName is a place name, not a plant name -- it decides how ER must score it.

        Both measurements are pinned because they differ, and by exactly one row:
        PlantID 64 is `PlantName = "Laplace"` against `City = "LaPlace"`. Case-sensitive
        equality gives 167/197 (84.8%); case-insensitive gives 168/197 (85.3%). The
        case-insensitive figure is the one that matters for entity resolution, since no
        comparison there will be case-sensitive.
        """
        case_sensitive = plantgen.filter(pl.col("plant_name") == pl.col("city")).height
        case_insensitive = plantgen.filter(
            pl.col("plant_name").str.to_lowercase() == pl.col("city").str.to_lowercase()
        ).height
        assert case_sensitive == 167
        assert case_insensitive == 168
        assert round(case_insensitive / plantgen.height * 100, 1) == 85.3

        differing = plantgen.filter(
            (pl.col("plant_name") != pl.col("city"))
            & (pl.col("plant_name").str.to_lowercase() == pl.col("city").str.to_lowercase())
        )
        assert differing.select("plant_id", "plant_name", "city").to_dicts() == [
            {"plant_id": "64", "plant_name": "Laplace", "city": "LaPlace"}
        ]


class TestGsptPlants:
    def test_full_sheet_is_loaded(self, plants: pl.DataFrame) -> None:
        assert plants.height == 1848

    def test_north_american_grain_is_plant_times_unit(self, plants: pl.DataFrame) -> None:
        north_america = plants.filter(pl.col("country_area").is_in(["United States", "Canada"]))
        assert north_america.height == 111
        assert north_america["plant_id"].n_unique() == 93
        rows_per_id = north_america.group_by("plant_id").len().rename({"len": "rows"})
        distribution = dict(rows_per_id.group_by("rows").len().iter_rows())  # type: ignore[arg-type]
        assert distribution == {1: 80, 2: 10, 3: 1, 4: 2}

    def test_country_split(self, plants: pl.DataFrame) -> None:
        north_america = plants.filter(pl.col("country_area").is_in(["United States", "Canada"]))
        counts = dict(
            north_america.group_by("country_area").len().iter_rows()  # type: ignore[arg-type]
        )
        assert counts == {"United States": 97, "Canada": 14}

    def test_coordinate_coverage_in_north_america(self, plants: pl.DataFrame) -> None:
        """Coordinates are the strongest cross-source path, but they are not complete.

        One US plant ("Electra iron plant", P100000121251) has Coordinates = "unknown",
        so the usable coverage is 92 of 93 plants, not 93 of 93. `coordinate_accuracy`
        still reads "approximate" on that row, which is why accuracy cannot be used as
        a proxy for presence.
        """
        north_america = plants.filter(pl.col("country_area").is_in(["United States", "Canada"]))
        assert north_america["latitude"].null_count() == 1
        assert north_america["longitude"].null_count() == 1
        assert (~north_america["has_coordinates"]).sum() == 1
        assert north_america.filter(pl.col("latitude").is_not_null())["plant_id"].n_unique() == 92
        missing = north_america.filter(pl.col("latitude").is_null())
        assert missing["plant_id"].to_list() == ["P100000121251"]
        assert missing["coordinates"].to_list() == ["unknown"]
        assert missing["has_coordinates"].to_list() == [False]
        # Accuracy still claims "approximate", so it cannot stand in for presence.
        assert missing["coordinate_accuracy"].to_list() == ["approximate"]
        accuracy = dict(north_america.group_by("coordinate_accuracy").len().iter_rows())  # type: ignore[arg-type]
        assert accuracy == {"exact": 108, "approximate": 3}

    def test_owner_identifiers_are_complete(self, plants: pl.DataFrame) -> None:
        north_america = plants.filter(pl.col("country_area").is_in(["United States", "Canada"]))
        assert north_america["owner_permid"].null_count() == 0
        assert north_america["owner_gem_id"].null_count() == 0
        assert north_america["owner_gem_id"].n_unique() == 45

    def test_state_is_spelled_out(self, plants: pl.DataFrame) -> None:
        """GSPT says "Alabama" where PLANTGEN says "AL": the join needs normalising."""
        north_america = plants.filter(pl.col("country_area").is_in(["United States", "Canada"]))
        states = set(north_america["subnational_unit_province_state"].to_list())
        assert "Alabama" in states
        assert "AL" not in states

    def test_capacity_columns_stay_text_because_of_sentinels(self, plants: pl.DataFrame) -> None:
        assert plants.schema["nominal_crude_steel_capacity_ttpa"] == pl.Utf8
        values = set(plants["sinter_plant_capacity_ttpa"].drop_nulls().to_list())
        assert ">0" in values


class TestGsptProduction:
    def test_is_a_long_table(self, production: pl.DataFrame) -> None:
        assert production.columns == [
            "plant_id",
            "plant_name_english",
            "year",
            "metric",
            "value_ttpa",
            "value_raw",
            "source_file",
        ]

    def test_grain_is_unique(self, production: pl.DataFrame) -> None:
        assert production.select("plant_id", "year", "metric").unique().height == production.height

    def test_years_and_metrics(self, production: pl.DataFrame) -> None:
        assert sorted(production["year"].unique().to_list()) == [2019, 2020, 2021, 2022]
        assert set(production["metric"].unique().to_list()) <= set(PRODUCTION_METRICS)

    def test_row_count(self, production: pl.DataFrame) -> None:
        assert production.height == 12409
        by_year = dict(production.group_by("year").len().iter_rows())  # type: ignore[arg-type]
        assert by_year == {2019: 3144, 2020: 3104, 2021: 3100, 2022: 3061}

    def test_sentinels_are_null_but_not_lost(self, production: pl.DataFrame) -> None:
        """ "unknown" is a reported non-answer; an absent cell was dropped entirely."""
        assert production["value_ttpa"].is_not_null().sum() == 2770
        assert production["value_raw"].null_count() == 0
        sentinels = production.filter(pl.col("value_ttpa").is_null())["value_raw"]
        assert sentinels.str.contains("unknown").sum() == 9514


class TestFurnaceYears:
    def test_all_four_vintages_are_unioned(self, furnaces: pl.DataFrame) -> None:
        counts = {
            (row["source_version"], row["furnace_type"]): row["len"]
            for row in furnaces.group_by("source_version", "furnace_type").len().to_dicts()
        }
        assert counts == {
            ("gspt_2024", "BOF"): 228,
            ("gspt_2024", "EAF"): 1925,
            ("owner_filled_2025", "BOF"): 228,
            ("owner_filled_2025", "EAF"): 1971,
        }
        assert furnaces.height == 4352

    def test_owner_only_present_in_the_2025_vintage(self, furnaces: pl.DataFrame) -> None:
        by_version = {
            row["source_version"]: row["owner"]
            for row in furnaces.group_by("source_version")
            .agg(pl.col("owner").null_count())
            .to_dicts()
        }
        assert by_version["owner_filled_2025"] == 0
        assert by_version["gspt_2024"] == 2153

    def test_id_is_only_unique_within_a_furnace_type(self, furnaces: pl.DataFrame) -> None:
        """18 ids appear in both files and mean different facilities."""
        bof = set(furnaces.filter(pl.col("furnace_type") == "BOF")["facility_id"].drop_nulls())
        eaf = set(furnaces.filter(pl.col("furnace_type") == "EAF")["facility_id"].drop_nulls())
        assert len(bof & eaf) == 18
        assert "70" in bof & eaf

    def test_surrogate_key_separates_the_collision(self, furnaces: pl.DataFrame) -> None:
        authoritative = furnaces.filter(pl.col("source_version") == "owner_filled_2025")
        aliases = {
            row["furnace_key"]: row["corrected_cl"]
            for row in authoritative.filter(pl.col("facility_id") == "70")
            .select("furnace_key", "corrected_cl")
            .unique()
            .to_dicts()
        }
        assert set(aliases) == {"BOF:70", "EAF:70"}
        assert any("Dearborn" in alias for alias in aliases["BOF:70"])
        assert any("Birmingham" in alias for alias in aliases["EAF:70"])

    def test_real_grain_is_furnace_per_year(self, furnaces: pl.DataFrame) -> None:
        eaf = furnaces.filter(
            (pl.col("source_version") == "owner_filled_2025") & (pl.col("furnace_type") == "EAF")
        )
        duplicate_facility_year = eaf.height - eaf.select("facility_id", "year").unique().height
        with_fid = eaf.height - eaf.select("facility_id", "year", "fid").unique().height
        assert duplicate_facility_year == 529
        assert with_fid == 34

    def test_data_id_is_not_a_key(self, furnaces: pl.DataFrame) -> None:
        eaf = furnaces.filter(
            (pl.col("source_version") == "owner_filled_2025") & (pl.col("furnace_type") == "EAF")
        )
        assert eaf["data_id"].drop_nulls().n_unique() == 187
        assert eaf.height == 1971

    def test_aliases_are_parsed_into_a_list(self, furnaces: pl.DataFrame) -> None:
        eaf = furnaces.filter(
            (pl.col("source_version") == "owner_filled_2025") & (pl.col("furnace_type") == "EAF")
        )
        assert eaf.schema["corrected_cl"] == pl.List(pl.Utf8)
        lengths = dict(
            eaf.select(pl.col("corrected_cl").list.len().alias("n")).group_by("n").len().iter_rows()  # type: ignore[arg-type]
        )
        assert lengths == {1: 1733, 2: 214, 3: 24}

    def test_r_na_sentinel_becomes_null(self, furnaces: pl.DataFrame) -> None:
        """The files were written by R; "NA" is a null, not a value."""
        assert "NA" not in set(furnaces["facility_id"].drop_nulls().to_list())
        assert "NA" not in set(furnaces["fid"].drop_nulls().to_list())

    def test_unkeyed_rows_are_kept_and_countable(self, furnaces: pl.DataFrame) -> None:
        """22 South American rows carry no ID at all. Bronze keeps them; they cannot join."""
        unkeyed = furnaces.filter(pl.col("furnace_key").is_null())
        assert unkeyed.height == 22
        assert set(unkeyed["source_file"].to_list()) == {"eaf_owner_filled.csv"}
        assert set(unkeyed["owner"].to_list()) == {"a South American facility", "another South American facility"}

    def test_annotated_counts_are_split_not_dropped(self, furnaces: pl.DataFrame) -> None:
        annotated = furnaces.filter(pl.col("no_of_furnaces_raw").str.contains(r"\("))
        assert annotated.height > 0
        assert annotated["no_of_furnaces"].null_count() < annotated.height
        assert furnaces["not_melting"].sum() == 155
