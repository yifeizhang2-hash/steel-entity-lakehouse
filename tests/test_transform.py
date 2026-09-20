"""The dbt layer: scoping decisions, grain, SCD2, and the bridge's missing verdict.

`dbt build` is the primary gate; these tests cover what dbt cannot express -- that an
assertion actually fails when it should, and that the bridge has no verdict column no
matter how the model is rewritten.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from lakehouse.paths import REPO_ROOT

PROJECT_DIR = REPO_ROOT / "transform"
DB_PATH = REPO_ROOT / "lake" / "dbt.duckdb"

pytestmark = pytest.mark.slow

# Column names that would mean someone had binarised a probability into a decision.
VERDICT_NAMES = {
    "is_match",
    "matched",
    "is_matched",
    "match",
    "accepted",
    "is_accepted",
    "decision",
    "verdict",
    "link_confirmed",
    "is_link",
    "resolved_plant_id",
    "matched_plant_key",
}


def dbt(*args: str, cwd: Path | None = None, env: dict[str, str] | None = None):
    """Run dbt from the repo root, the way the Makefile does."""
    command = [
        "uv",
        "run",
        "dbt",
        *args,
        "--project-dir",
        str(PROJECT_DIR),
        "--profiles-dir",
        str(PROJECT_DIR),
    ]
    merged = {**os.environ, **(env or {})}
    return subprocess.run(
        command, cwd=str(cwd or REPO_ROOT), capture_output=True, text=True, env=merged
    )


@pytest.fixture(scope="module")
def built() -> Path:
    """The built dbt project, built here if the lake exists but dbt has not run.

    Keeps the tests independent of the order `make all` happens to run its targets in.
    """
    if DB_PATH.exists():
        return DB_PATH
    # The precondition is the source data, not the existence of a lake directory.
    # Other tests build throwaway lakes, so `lake/catalog.db` can exist while holding
    # nothing dbt can build against -- guarding on it let this fixture try a build that
    # could not succeed, and error where it should have skipped.
    from ingest.config import PLANTGEN_CSV

    if not PLANTGEN_CSV.exists():
        pytest.skip(f"raw data not present: {PLANTGEN_CSV}")
    if not (REPO_ROOT / "lake" / "catalog.db").exists():
        pytest.skip("no lake; run `make ingest geo resolve` first")
    from transform.bootstrap import bootstrap

    bootstrap()
    deps = dbt("deps")
    assert deps.returncode == 0, deps.stdout
    built = dbt("build", "--no-partial-parse")
    assert built.returncode == 0, built.stdout
    return DB_PATH


@pytest.fixture(scope="module")
def con(built: Path):
    connection = duckdb.connect(str(built), read_only=True)
    connection.execute("LOAD iceberg;")
    yield connection
    connection.close()


def count(con, table: str) -> int:
    return con.execute(f"select count(*) from {table}").fetchone()[0]


class TestScoping:
    """Every filter is a decision, and every decision is reconcilable against bronze."""

    def test_north_america_is_filtered_in_staging_not_bronze(self, con) -> None:
        assert count(con, "raw.gspt_plants") == 1848
        assert count(con, "main_staging.stg_gspt_plant_unit") == 111

    def test_plantgen_loses_nothing(self, con) -> None:
        assert count(con, "raw.plantgen") == count(con, "main_staging.stg_plantgen") == 197

    def test_furnace_reconciles_exactly(self, con) -> None:
        """4352 = 2153 old vintage + 22 South American + 24 exact duplicates + 2153 kept."""
        bronze = count(con, "raw.furnace_years")
        old = con.execute(
            "select count(*) from raw.furnace_years where source_version = 'gspt_2024'"
        ).fetchone()[0]
        south_american = con.execute(
            "select count(*) from raw.furnace_years "
            "where source_version = 'owner_filled_2025' "
            "and alignment_status = 'unaligned_no_key'"
        ).fetchone()[0]
        kept = count(con, "main_marts.fct_furnace_year")
        assert (bronze, old, south_american, kept) == (4352, 2153, 22, 2153)
        assert bronze - old - south_american - 24 == kept

    def test_south_american_rows_are_gone(self, con) -> None:
        assert (
            con.execute(
                "select count(*) from main_marts.fct_furnace_year where facility_id is null"
            ).fetchone()[0]
            == 0
        )

    def test_the_exact_duplicate_facility_appears_once_per_furnace_year(self, con) -> None:
        """EAF:79 is in the source 48 times and should be there 24; the other vintage
        holds exactly 24, which is what settles it."""
        staged = con.execute(
            "select count(*) from main_marts.fct_furnace_year "
            "where furnace_type = 'EAF' and facility_id = '79'"
        ).fetchone()[0]
        bronze = con.execute(
            "select count(*) from raw.furnace_years where furnace_type = 'EAF' "
            "and facility_id = '79' and source_version = 'owner_filled_2025'"
        ).fetchone()[0]
        clean_vintage = con.execute(
            "select count(*) from raw.furnace_years where furnace_type = 'EAF' "
            "and facility_id = '79' and source_version = 'gspt_2024'"
        ).fetchone()[0]
        assert (bronze, clean_vintage, staged) == (48, 24, 24)


class TestSentinels:
    def test_reported_unknown_is_not_the_same_as_never_reported(self, con) -> None:
        """The whole reason the status column exists."""
        statuses = dict(
            con.execute(
                "select value_status, count(*) from main_marts.fct_plant_production_year group by 1"
            ).fetchall()
        )
        assert statuses.get("reported", 0) > 0
        assert statuses.get("unknown", 0) > 0
        assert len(statuses) >= 2

    def test_a_sentinel_row_carries_no_number(self, con) -> None:
        leaked = con.execute(
            "select count(*) from main_marts.fct_plant_production_year "
            "where value_status = 'unknown' and value_ttpa is not null"
        ).fetchone()[0]
        assert leaked == 0

    def test_plantgen_9999_is_unknown_not_a_year(self, con) -> None:
        assert (
            con.execute(
                "select count(*) from main_staging.stg_plantgen where start_year_status = 'unknown'"
            ).fetchone()[0]
            == 8
        )
        assert (
            con.execute(
                "select count(*) from main_staging.stg_plantgen where start_year = 9999"
            ).fetchone()[0]
            == 0
        )


class TestKeysAndGrain:
    def test_dim_plant_key_is_unique(self, con) -> None:
        assert count(con, "main_marts.dim_plant") == 290
        assert (
            con.execute(
                "select count(*) from (select plant_key from main_marts.dim_plant "
                "group by 1 having count(*) > 1)"
            ).fetchone()[0]
            == 0
        )

    def test_owner_permid_is_unique_and_complete(self, con) -> None:
        assert count(con, "main_marts.dim_owner") == 45

    def test_furnace_key_carries_the_type(self, con) -> None:
        """18 ids collide across BOF and EAF; id 70 is two different plants."""
        both = con.execute(
            "select count(*) from main_marts.dim_furnace where facility_id = '70'"
        ).fetchone()[0]
        types = con.execute(
            "select count(distinct furnace_type) from main_marts.dim_furnace "
            "where facility_id = '70'"
        ).fetchone()[0]
        assert both >= 2
        assert types == 2

    def test_furnace_year_grain_holds(self, con) -> None:
        duplicates = con.execute(
            "select count(*) from (select furnace_type, facility_id, year, fid "
            "from main_marts.fct_furnace_year group by 1, 2, 3, 4 having count(*) > 1)"
        ).fetchone()[0]
        assert duplicates == 0


class TestOwnershipScd2:
    def test_exactly_one_current_span_per_facility(self, con) -> None:
        bad = con.execute(
            "select count(*) from (select facility_key from "
            "main_marts.dim_facility_ownership_scd2 where is_current group by 1 "
            "having count(*) <> 1)"
        ).fetchone()[0]
        assert bad == 0
        assert (
            con.execute(
                "select count(distinct facility_key) from main_marts.dim_facility_ownership_scd2"
            ).fetchone()[0]
            == con.execute(
                "select count(*) from main_marts.dim_facility_ownership_scd2 where is_current"
            ).fetchone()[0]
        )

    def test_intervals_never_overlap(self, con) -> None:
        overlaps = con.execute(
            """
            select count(*) from main_marts.dim_facility_ownership_scd2 a
            join main_marts.dim_facility_ownership_scd2 b
              on a.facility_key = b.facility_key and a.version_number < b.version_number
            where a.valid_from_year < coalesce(b.valid_to_year, 9999)
              and b.valid_from_year < coalesce(a.valid_to_year, 9999)
            """
        ).fetchone()[0]
        assert overlaps == 0

    def test_normalisation_collapses_formatting_variants(self, con) -> None:
        """87 raw owner strings, 83 normalised: the diff is not run on raw text."""
        raw, normalised = con.execute(
            "select count(distinct owner_raw), count(distinct owner_normalized) "
            "from main_marts.fct_furnace_year"
        ).fetchone()
        assert raw == 87
        assert normalised < raw

    def test_a_changed_string_is_not_reported_as_a_changed_owner(self, con) -> None:
        """18 span boundaries; only 2 survive as credible transfers."""
        classes = dict(
            con.execute(
                "select change_class, count(*) from main_marts.fct_ownership_change group by 1"
            ).fetchall()
        )
        assert classes["observation_gap"] == 8
        assert classes["round_trip"] == 4
        assert classes["likely_typo"] == 2
        assert classes["name_refinement"] == 2
        assert classes["credible_transfer"] == 2
        assert sum(classes.values()) == 18

    def test_both_legs_of_a_round_trip_are_suppressed(self, con) -> None:
        """Credibility is a sequence property, not a per-transition one.

        BOF-70 goes A -> B -> A. Judging each transition alone makes only
        the return leg look like a round trip, so the outbound leg survives as
        "credible" -- which cannot be right: if the record bounces back within two years,
        the outbound step is exactly as unreliable as the one that undoes it.
        """
        legs = con.execute(
            "select change_year, change_class from main_marts.fct_ownership_change "
            "where facility_key = 'BOF-70' and change_year in (2015, 2017) "
            "order by change_year"
        ).fetchall()
        assert legs == [(2015, "round_trip"), (2017, "round_trip")]

        both = con.execute(
            "select count(*) from main_marts.fct_ownership_change "
            "where facility_key = 'BOF-73' and change_class = 'credible_transfer'"
        ).fetchone()[0]
        assert both == 0

    def test_a_transfer_after_an_excursion_survives(self, con) -> None:
        """The thing most easily broken by fixing round-trip suppression.

        `BOF-70 2022 A -> C` happens after the excursion and is
        untouched by it. An implementation that collapsed the facility's sequence to its
        modal owner would delete it and conclude that every BOF ownership change is noise.
        """
        row = con.execute(
            "select change_class, in_round_trip from main_marts.fct_ownership_change "
            "where facility_key = 'BOF-70' and change_year = 2022"
        ).fetchone()
        assert row == ("credible_transfer", False)

    def test_the_noise_ladder_starts_before_deduplication(self, con) -> None:
        """Level 0 exists because the EAF:79 whitespace oscillation is removed by the
        DEDUP, not by normalisation, so it never reached the level-1 count."""
        levels = con.execute(
            "select stage, noisy_units from main_marts.fct_ownership_noise order by level, stage"
        ).fetchall()
        assert levels == [
            ("raw source, before dedup", 24),
            ("raw source: repeats disagreeing on owner", 22),
            ("after dedup", 10),
            ("after normalisation", 10),
            ("after classification", 2),
        ]

    def test_encoding_damaged_owners_still_normalise_stably(self, con) -> None:
        """No intact counterpart exists for these, so they must not flicker."""
        damaged = con.execute(
            "select count(distinct owner_normalized) from main_marts.fct_furnace_year "
            "where owner_has_replacement_char"
        ).fetchone()[0]
        assert damaged == 2
        assert (
            con.execute(
                "select count(*) from main_marts.fct_ownership_change "
                "where has_encoding_damage and change_class = 'credible_transfer'"
            ).fetchone()[0]
            == 0
        )


class TestBridgeCarriesNoVerdict:
    """The constraint that matters most. See ADR-004 and ADR-005."""

    def test_no_verdict_shaped_column_exists(self, con) -> None:
        columns = {
            row[0].lower()
            for row in con.execute("describe select * from main_marts.bridge_plant_xref").fetchall()
        }
        assert not (columns & VERDICT_NAMES), f"verdict column present: {columns & VERDICT_NAMES}"
        assert not [c for c in columns if c.startswith("is_match")]

    def test_the_model_source_declares_the_constraint(self) -> None:
        source = (PROJECT_DIR / "models" / "marts" / "bridge_plant_xref.sql").read_text()
        assert "NO MATCH/NO-MATCH VERDICT" in source

    def test_the_dbt_assertion_exists(self) -> None:
        assert (PROJECT_DIR / "tests" / "assert_bridge_has_no_verdict.sql").exists()

    def test_every_north_american_plant_has_a_status(self, con) -> None:
        plants = con.execute(
            "select count(distinct gspt_plant_id) from main_marts.bridge_plant_xref"
        ).fetchone()[0]
        assert plants == 93

    def test_canada_is_never_compared_not_scored_low(self, con) -> None:
        """8 plants with no counterpart. Recording them as low-probability matches would
        hide them inside an accuracy figure; they were never compared at all."""
        canada = con.execute(
            "select resolution_status, count(*), count(match_probability) "
            "from main_marts.bridge_plant_xref where country_area = 'Canada' group by 1"
        ).fetchall()
        assert canada == [("never_compared", 8, 0)]

    def test_unresolved_rows_carry_no_candidate_key(self, con) -> None:
        """Otherwise a downstream join picks up a link the model never endorsed."""
        leaked = con.execute(
            "select count(*) from main_marts.bridge_plant_xref "
            "where resolution_status = 'never_compared' and plantgen_plant_key is not null"
        ).fetchone()[0]
        assert leaked == 0


class TestAssertionsActuallyFail:
    """An assertion nobody has seen fail is a comment.

    Each test breaks the data or the model on a copy, runs dbt, and requires the build
    to go red.
    """

    @pytest.fixture
    def sandbox(self, tmp_path: Path, built: Path) -> Path:
        """A copy of the built database, so injections cannot touch the real one.

        The file keeps the name `dbt.duckdb`: DuckDB derives the catalog name from the
        filename, and dbt's compiled refs are qualified with it, so a copy called
        anything else fails with `Catalog "dbt" does not exist`.
        """
        target = tmp_path / "dbt.duckdb"
        shutil.copy(built, target)
        return target

    def test_an_unexpected_source_duplicate_is_caught(self, sandbox: Path) -> None:
        """Inject a duplicate furnace-year into the source and rebuild.

        The dedup guard catches this one, not the grain assertion -- and that ordering
        is the point. `stg_furnace_year` deduplicates on the business key, so a new
        duplicate would be absorbed silently and never reach the grain test. The guard
        pins the number of rows the dedup is allowed to remove, so a source that starts
        duplicating something else fails the build instead of disappearing into a
        `qualify`.
        """
        con = duckdb.connect(str(sandbox))
        con.execute("LOAD iceberg;")
        # Freeze the source view as a table so a row can be injected into it.
        con.execute("CREATE SCHEMA IF NOT EXISTS raw_backup")
        con.execute("CREATE TABLE raw_backup.furnace_years AS SELECT * FROM raw.furnace_years")
        con.execute("DROP VIEW raw.furnace_years")
        con.execute("CREATE TABLE raw.furnace_years AS SELECT * FROM raw_backup.furnace_years")
        con.execute(
            """
            INSERT INTO raw.furnace_years BY NAME
            SELECT * EXCLUDE (source_row), 99999 AS source_row
            FROM raw_backup.furnace_years
            WHERE source_version = 'owner_filled_2025'
              AND alignment_status <> 'unaligned_no_key'
              AND facility_id = '16'
            LIMIT 1
            """
        )
        con.close()

        result = dbt("build", "--no-partial-parse", env={"STEEL_DBT_DB": str(sandbox)})
        assert result.returncode != 0, "an unexpected source duplicate must fail the build"
        assert "FAIL 1 assert_furnace_dedup_removed_expected_rows" in result.stdout

    def test_a_grain_conflict_in_the_fact_is_caught(self, sandbox: Path) -> None:
        """Inject a duplicate straight into the fact table, past the dedup.

        This is the grain assertion itself: (furnace_type, facility_id, year, fid) must
        identify exactly one row.
        """
        con = duckdb.connect(str(sandbox))
        con.execute("LOAD iceberg;")
        con.execute(
            "INSERT INTO main_marts.fct_furnace_year "
            "SELECT * FROM main_marts.fct_furnace_year LIMIT 1"
        )
        con.close()

        result = dbt(
            "test",
            "--select",
            "fct_furnace_year",
            "--no-partial-parse",
            env={"STEEL_DBT_DB": str(sandbox)},
        )
        assert result.returncode != 0, "a duplicated furnace-year must fail the build"
        assert "FAIL 1 dbt_utils_unique_combination_of_columns_fct_furnace_year" in result.stdout

    def test_a_verdict_column_on_the_bridge_is_caught(self, sandbox: Path, tmp_path: Path) -> None:
        """Add `is_match` to the bridge and require the build to reject it."""
        model = PROJECT_DIR / "models" / "marts" / "bridge_plant_xref.sql"
        original = model.read_text()
        try:
            model.write_text(
                original.replace(
                    "    u.country_area\nfrom with_status w",
                    "    u.country_area,\n"
                    "    w.match_probability >= 0.99 as is_match\nfrom with_status w",
                )
            )
            result = dbt(
                "build", "--select", "bridge_plant_xref+", env={"STEEL_DBT_DB": str(sandbox)}
            )
            assert result.returncode != 0, "a verdict column must fail the build"
            assert "assert_bridge_has_no_verdict" in result.stdout
        finally:
            model.write_text(original)

    def test_an_overlapping_ownership_interval_is_caught(self, sandbox: Path) -> None:
        con = duckdb.connect(str(sandbox))
        con.execute("LOAD iceberg;")
        con.execute(
            """
            INSERT INTO main_marts.dim_facility_ownership_scd2
            SELECT
                ownership_key || '-injected', facility_key, furnace_type, facility_id,
                version_number + 100, owner_normalized, owner_raw,
                valid_from_year, valid_from_year + 5, last_observed_year, false,
                has_encoding_damage
            FROM main_marts.dim_facility_ownership_scd2
            LIMIT 1
            """
        )
        con.close()
        result = dbt(
            "test", "--select", "dim_facility_ownership_scd2", env={"STEEL_DBT_DB": str(sandbox)}
        )
        assert result.returncode != 0, "overlapping ownership spans must fail the build"
        assert "assert_scd2_intervals_do_not_overlap" in result.stdout
