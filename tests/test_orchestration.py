"""Dagster assets and the checks dbt cannot express.

The checks are exercised by degrading the warehouse on a copy and requiring each one to
go red. A check that has never been seen failing is a comment with a green tick on it.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pytest
from dagster import AssetKey

from lakehouse.paths import LAKE_ROOT
from orchestration import checks as C
from orchestration.assets import LAKE_TABLES, view_key
from orchestration.definitions import all_assets, all_checks, defs


def asset_specs():
    """Every asset spec in the code location, across plain assets and dbt assets."""
    specs = []
    for definition in all_assets:
        specs.extend(getattr(definition, "specs", []))
    return specs


def check_names() -> set[str]:
    return {spec.name for check in all_checks for spec in check.check_specs}


pytestmark = pytest.mark.slow

DBT_DB = LAKE_ROOT / "dbt.duckdb"


def run_check(check) -> object:
    """Call an asset check's body directly, without a Dagster run."""
    return check.op.compute_fn.decorated_fn()


@pytest.fixture(scope="module")
def warehouse() -> Path:
    if not DBT_DB.exists():
        pytest.skip("no built warehouse; run `make dbt` first")
    return DBT_DB


@pytest.fixture
def degraded(tmp_path: Path, warehouse: Path, monkeypatch) -> Path:
    """A copy of the warehouse the tests may corrupt."""
    target = tmp_path / "dbt.duckdb"
    shutil.copy(warehouse, target)
    monkeypatch.setattr(C, "DBT_DATABASE", target)
    return target


class TestDefinitions:
    def test_the_code_location_loads(self) -> None:
        assert defs is not None
        assert asset_specs()

    def test_bronze_geo_and_resolution_are_assets(self) -> None:
        keys = {spec.key for spec in asset_specs()}
        for key in (
            AssetKey(["raw", "plantgen"]),
            AssetKey(["raw", "gspt_plants"]),
            AssetKey(["raw", "gspt_production"]),
            AssetKey(["raw", "furnace_years"]),
            AssetKey(["geo", "gspt_plant_county"]),
            AssetKey(["resolution", "gspt_plantgen_candidates"]),
        ):
            assert key in keys, key

    def test_furnace_years_is_partitioned_by_source_version(self) -> None:
        spec = next(s for s in asset_specs() if s.key == AssetKey(["raw", "furnace_years"]))
        assert spec.partitions_def is not None
        assert set(spec.partitions_def.get_partition_keys()) == {
            "gspt_2024",
            "owner_filled_2025",
        }

    def test_bootstrap_is_its_own_asset_upstream_of_dbt(self) -> None:
        """A resource that fails is an unlocatable stack trace; an asset that fails has
        a position in the lineage."""
        keys = {spec.key for spec in asset_specs()}
        for namespace, table in LAKE_TABLES:
            assert view_key(namespace, table) in keys

    def test_dbt_models_depend_on_the_views_not_the_iceberg_tables(self) -> None:
        """dbt reads DuckDB views over Iceberg, and the lineage should say so."""
        staging = next(
            s for s in asset_specs() if s.key == AssetKey(["staging", "stg_furnace_year"])
        )
        upstream = {dep.asset_key for dep in staging.deps}
        assert view_key("raw", "furnace_years") in upstream
        assert AssetKey(["raw", "furnace_years"]) not in upstream


class TestChecksAreNotMirrorsOfDbt:
    def test_only_cross_asset_invariants_are_defined_here(self) -> None:
        """48 dbt assertions exist; duplicating them here would guarantee drift."""
        assert check_names() == {
            "bridge_row_count_reconciles_with_the_linker",
            "furnace_rows_reconcile_from_bronze_to_mart",
            "geographic_coverage_has_not_regressed",
            "ownership_noise_filtering_is_bounded_on_both_sides",
            "match_precision_against_human_labels",
        }

    def test_no_check_re_implements_a_dbt_uniqueness_or_null_test(self) -> None:
        source = Path(C.__file__).read_text()
        assert "not_null" not in source
        assert "unique_combination" not in source


class TestCrossAssetChecksPass:
    def test_bridge_reconciles_with_the_linker(self, warehouse: Path) -> None:
        result = run_check(C.bridge_reconciles)
        assert result.passed
        assert result.metadata["bridge_total_rows"].value == 1492
        assert result.metadata["linker_candidate_pairs"].value == 1484
        assert result.metadata["bridge_never_compared_rows"].value == 8

    def test_furnace_rows_reconcile(self, warehouse: Path) -> None:
        result = run_check(C.furnace_reconciles)
        assert result.passed
        assert result.metadata["bronze_rows"].value == 4352
        assert result.metadata["actual_mart_rows"].value == 2153

    def test_ownership_noise_sits_inside_the_band(self, warehouse: Path) -> None:
        result = run_check(C.ownership_noise_is_bounded)
        assert result.passed
        assert result.metadata["transitions"].value == 18
        # 2 after the round-trip fix, not 4: both legs of an A->B->A excursion are
        # suppressed, because credibility is a property of the sequence.
        assert result.metadata["credible_transfers"].value == 2


class TestChecksCatchDegradation:
    def test_coverage_regression_fails(self, degraded: Path, tmp_path: Path, monkeypatch) -> None:
        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps({"geo_county_assigned": 93}))
        monkeypatch.setattr(C, "BASELINE_PATH", baseline)
        result = run_check(C.geo_coverage_has_not_regressed)
        assert not result.passed
        assert result.metadata["change"].value < 0
        assert result.severity.value == "ERROR"

    def test_a_missing_baseline_warns_rather_than_silently_passing(
        self, tmp_path: Path, monkeypatch, warehouse: Path
    ) -> None:
        """`make clean` deletes the baseline, so the first run after it cannot compare."""
        monkeypatch.setattr(C, "BASELINE_PATH", tmp_path / "absent.json")
        result = run_check(C.geo_coverage_has_not_regressed)
        assert result.passed
        assert result.severity.value == "WARN"
        assert "none recorded" in str(result.metadata["baseline"].value)

    def test_bridge_rows_going_missing_fails(self, degraded: Path) -> None:
        con = duckdb.connect(str(degraded))
        con.execute("LOAD iceberg;")
        con.execute(
            "DELETE FROM main_marts.bridge_plant_xref "
            "WHERE resolution_status = 'candidate' AND rowid % 100 = 0"
        )
        con.close()
        result = run_check(C.bridge_reconciles)
        assert not result.passed
        assert result.metadata["bridge_candidate_rows"].value < 1484

    def test_noise_filtering_that_stops_filtering_fails(self, degraded: Path) -> None:
        """Upper side of the band: nothing rejected means the filters broke."""
        con = duckdb.connect(str(degraded))
        con.execute("LOAD iceberg;")
        con.execute("UPDATE main_marts.fct_ownership_change SET change_class = 'credible_transfer'")
        con.close()
        result = run_check(C.ownership_noise_is_bounded)
        assert not result.passed
        assert result.metadata["rejected_share"].value == 0.0

    def test_noise_filtering_that_eats_everything_fails(self, degraded: Path) -> None:
        """Lower side of the band. A one-sided bound would call this healthy."""
        con = duckdb.connect(str(degraded))
        con.execute("LOAD iceberg;")
        con.execute("UPDATE main_marts.fct_ownership_change SET change_class = 'likely_typo'")
        con.close()
        result = run_check(C.ownership_noise_is_bounded)
        assert not result.passed
        assert result.metadata["rejected_share"].value == 1.0
        assert result.metadata["credible_transfers"].value == 0

    def test_no_transitions_at_all_fails(self, degraded: Path) -> None:
        con = duckdb.connect(str(degraded))
        con.execute("LOAD iceberg;")
        con.execute("DELETE FROM main_marts.fct_ownership_change")
        con.close()
        result = run_check(C.ownership_noise_is_bounded)
        assert not result.passed
        assert "produced nothing" in result.metadata["reason"].value


@pytest.mark.slow
class TestLabelDependentCheck:
    """The check that could not run until a human labelled, and now can."""

    def test_it_evaluates_now_that_labels_exist(self) -> None:
        from labeling import store

        assert store.summary()["has_real_labels"], "this test assumes labels exist"
        result = run_check(C.precision_against_labels)
        assert result.passed is True
        assert result.metadata["precision"].value > 0.85

    def test_it_gates_on_one_stratum_not_a_pooled_precision(self) -> None:
        """Pooling would average over a sample the sampler composed, and would hide the
        finding that scoring and assignment fail separately."""
        result = run_check(C.precision_against_labels)
        assert result.metadata["gated_on"].value == "high_confidence stratum at p >= 0.99"
        assert "NOT a pooled precision" in result.metadata["note"].value

    def test_contested_is_reported_but_does_not_gate(self) -> None:
        """0.500 there is the arithmetic of an unsolved assignment problem. Gating on it
        would fail this check forever for a reason no threshold can fix. See ADR-009."""
        result = run_check(C.precision_against_labels)
        contested = result.metadata["contested_precision_reported_not_gated"].value
        assert contested == 0.5
        assert result.passed is True, "a 0.5 contested precision must not fail the gate"
        assert "ADR-009" in result.metadata["contested_note"].value

    def test_it_reports_false_negatives_across_every_stratum(self) -> None:
        """Recall is only meaningful because the queue sampled the low end on purpose."""
        result = run_check(C.precision_against_labels)
        assert result.metadata["false_negatives_across_all_strata"].value == 4

    def test_it_still_skips_when_the_label_file_is_absent(self, tmp_path, monkeypatch) -> None:
        """Regression guard on the empty case, which is no longer the default path.

        A green tick for a measurement nobody took is worse than a missing one, and now
        that labels exist that branch is easy to break without noticing.
        """
        from labeling import store

        monkeypatch.setattr(store, "DECISIONS_PATH", tmp_path / "absent.jsonl")
        result = run_check(C.precision_against_labels)
        assert result.passed is False
        assert result.metadata["status"].value == "SKIPPED - not evaluable"
        assert result.severity.value == "WARN"
        assert "make label" in result.metadata["next_step"].value

    def test_it_is_non_blocking(self) -> None:
        spec = next(
            spec
            for check in all_checks
            for spec in check.check_specs
            if spec.name == "match_precision_against_human_labels"
        )
        assert spec.blocking is False


class TestSchedulingIsEventDriven:
    """No cron. See ADR-008."""

    def test_no_schedules_are_defined(self) -> None:
        """The sources are annual file drops; a timer would re-read identical files."""
        assert not list(defs.schedules or [])

    def test_a_sensor_watches_for_new_vintages(self) -> None:
        names = {s.name for s in (defs.sensors or [])}
        assert "new_source_vintage_sensor" in names

    def test_the_sensor_is_not_running_by_default(self) -> None:
        """Starting a directory-watching daemon should be a deliberate act."""
        from dagster import DefaultSensorStatus

        sensor = next(s for s in defs.sensors if s.name == "new_source_vintage_sensor")
        assert sensor.default_status == DefaultSensorStatus.STOPPED

    def test_it_finds_the_vintages_on_disk(self, raw_vintages: tuple[str, ...]) -> None:
        from orchestration.sensors import discovered_vintages

        assert discovered_vintages() == list(raw_vintages)

    def test_it_skips_when_nothing_is_new(self, tmp_path: Path) -> None:
        import json

        from dagster import build_sensor_context

        from orchestration.sensors import new_source_vintage_sensor

        cursor = json.dumps(["gspt_2024", "owner_filled_2025"])
        result = new_source_vintage_sensor(build_sensor_context(cursor=cursor))
        assert "no new source vintage" in result.skip_message

    def test_it_requests_a_run_for_an_unseen_vintage(self, raw_vintages: tuple[str, ...]) -> None:
        from dagster import build_sensor_context

        from orchestration.sensors import new_source_vintage_sensor

        result = new_source_vintage_sensor(build_sensor_context(cursor=None))
        assert {r.run_key for r in result.run_requests} == {
            "vintage-gspt_2024",
            "vintage-owner_filled_2025",
        }


class TestBaselineIsTracked:
    def test_it_does_not_live_in_the_gitignored_lake(self) -> None:
        """A baseline one `make clean` can erase is not a baseline. See ADR-008."""
        assert "lake" not in C.BASELINE_PATH.parts
        assert C.BASELINE_PATH.parent.name == "baselines"

    def test_it_exists_in_the_repository(self) -> None:
        assert C.BASELINE_PATH.exists()
        assert json.loads(C.BASELINE_PATH.read_text())["geo_county_assigned"] > 0
