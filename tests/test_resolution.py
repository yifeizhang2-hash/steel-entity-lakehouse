"""Entity resolution: normalisation, blocking reach, and the trained model's behaviour.

The numbers asserted here are measurements. If the model's discrimination regresses,
these fail rather than the regression being noticed later in a chart.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from geo import download
from geo.enrich import build as build_geo
from ingest.pipeline import run as run_ingest
from resolution import normalize as nz
from resolution.features import build_frames, coverage
from resolution.model import ALL_BLOCKING_RULES, add_blocking_key_flag
from resolution.report import build as build_report

logging.getLogger("splink").setLevel(logging.ERROR)


class TestNormalisation:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("Alabama", "AL"), ("AL", "AL"), ("alabama", "AL"), ("Ontario", "ON"), ("ON", "ON")],
    )
    def test_states_fold_to_one_form(self, raw: str, expected: str) -> None:
        """Without this the two sources' state columns have an empty intersection."""
        assert nz.normalize_subdivision(raw) == expected

    @pytest.mark.parametrize("raw", ["unknown", "N/A", "", None, "Atlantis"])
    def test_unrecognised_subdivision_is_none(self, raw: str | None) -> None:
        assert nz.normalize_subdivision(raw) is None

    @pytest.mark.parametrize(
        ("address", "expected"),
        [
            ("16770 Rebar Road, Jacksonville, FL 32234, United States", "32234"),
            ("1 Steel Dr., Calvert, Alabama, 36513-1300, United States", "36513"),
            ("Pacific Northwest, United States", None),
            ("unknown", None),
            (None, None),
        ],
    )
    def test_zip_extraction(self, address: str | None, expected: str | None) -> None:
        """The *last* 5-digit run wins: a street number can also be five digits."""
        assert nz.extract_zip(address) == expected

    def test_street_abbreviations_fold(self) -> None:
        a = nz.normalize_street("6500 S Boundary Rd, Portage, IN 46368")
        b = nz.normalize_street("6500 South Boundary Road, Portage")
        assert a == b == "6500 s boundary rd"

    def test_street_drops_everything_after_the_first_comma(self) -> None:
        """City, state and ZIP are compared separately; leaving them in double-counts."""
        assert nz.normalize_street("1 Steel Dr., Calvert, Alabama, 36513") == "1 steel dr"

    def test_float_nan_is_absent_not_the_string_nan(self) -> None:
        """A null through a pandas float column stringifies to "nan" and would join rows."""
        assert nz.clean(float("nan")) is None
        assert nz.is_sentinel(float("nan"))
        assert nz.clean("nan") is None

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("1974", 1974), ("2022-06", 2022), (">2019", 2019), ("unknown", None), ("N/A", None)],
    )
    def test_year_parsing(self, raw: str, expected: int | None) -> None:
        assert nz.parse_year(raw) == expected


@pytest.fixture(scope="module")
def lake(tmp_path_factory, plantgen_csv, gspt_xlsx, furnace_files) -> tuple[Path, Path]:
    """A throwaway lake with the bronze tables and the geo enrichment on top.

    Built here so these tests do not depend on a lake already existing, and do not
    depend on the order `make all` happens to run its targets in.
    """
    if not download.is_cached():
        pytest.skip("county boundaries not cached; run `make geo` once with network access")
    root = tmp_path_factory.mktemp("resolution_lake")
    warehouse, catalog_db = root / "warehouse", root / "catalog.db"
    run_ingest(
        tables=("plantgen", "gspt_plants"),
        warehouse=warehouse,
        pipelines_dir=root / "dlt",
        catalog_db=catalog_db,
    )
    build_geo(db_path=catalog_db, warehouse=warehouse, write=True)
    return catalog_db, warehouse


@pytest.fixture(scope="module")
def frames(lake) -> tuple[pd.DataFrame, pd.DataFrame]:
    return build_frames(*lake)


@pytest.fixture(scope="module")
def keyed_frames(frames) -> tuple[pd.DataFrame, pd.DataFrame]:
    gspt, plantgen = frames
    return add_blocking_key_flag(gspt), add_blocking_key_flag(plantgen)


@pytest.fixture(scope="module")
def trained_and_report(lake):
    catalog_db, warehouse = lake
    return build_report(db_path=catalog_db, warehouse=warehouse, write=False)


@pytest.fixture(scope="module")
def report(trained_and_report):
    return trained_and_report[1]


@pytest.fixture(scope="module")
def model(trained_and_report):
    return trained_and_report[0]


@pytest.mark.slow
class TestFeatures:
    def test_one_row_per_plant(self, frames) -> None:
        gspt, plantgen = frames
        assert len(gspt) == 93
        assert len(plantgen) == 197
        assert not gspt["unique_id"].duplicated().any()
        assert not plantgen["unique_id"].duplicated().any()

    def test_evidence_coverage(self, frames) -> None:
        gspt, plantgen = frames
        assert coverage(gspt)["zip5"] == 80
        assert coverage(plantgen)["zip5"] == 180
        assert coverage(plantgen)["street"] == 126
        # 89 rows look populated, but 8 of them hold 9999 -- PLANTGEN's sentinel for
        # "unknown", not a year. Left in, they would reach the model as a real year and
        # turn an absence into a disagreement.
        assert coverage(plantgen)["start_year"] == 81

    def test_states_now_intersect(self, frames) -> None:
        gspt, plantgen = frames
        assert len(set(gspt["state"].dropna()) & set(plantgen["state"].dropna())) == 28

    def test_placeholder_coordinates_carry_no_geography(self, frames) -> None:
        """Decision: `approximate` is excluded outright, not down-weighted.

        Nucor Steel Pacific Northwest's recorded point is the geographic centre of the
        contiguous US, which resolves to Kansas. A weight cannot repair a fabricated
        value, so the geographic columns are null instead.
        """
        gspt, _ = frames
        for plant in ("P100000120995", "P100000121221", "P100000121251"):
            row = gspt[gspt["unique_id"] == plant].iloc[0]
            assert pd.isna(row["county_fips"])
            assert pd.isna(row["county_fips_effective"])

    def test_other_names_excludes_the_primary_name(self, frames) -> None:
        """Otherwise the alias comparator would restate the place comparator."""
        gspt, _ = frames
        for name, others in zip(gspt["plant_name"], gspt["other_names"], strict=True):
            assert name not in others

    def test_plantgen_has_no_owner_or_capacity_to_compare(self, frames) -> None:
        """Both are absent from PLANTGEN entirely, so neither can be evidence."""
        _, plantgen = frames
        assert "owner" not in plantgen.columns
        assert not any("capacity" in c for c in plantgen.columns)


@pytest.mark.slow
class TestBlocking:
    def test_a_fallback_rule_exists(self) -> None:
        """Three GSPT plants match no blocking key; without a fallback they vanish."""
        assert any("has_blocking_key" in str(rule) for rule in ALL_BLOCKING_RULES)

    def test_exactly_the_placeholder_plants_need_the_fallback(self, keyed_frames) -> None:
        gspt, plantgen = keyed_frames
        unreachable = set(gspt.loc[~gspt["has_blocking_key"], "unique_id"])
        assert unreachable == {"P100000120995", "P100000121221", "P100000121251"}
        assert plantgen["has_blocking_key"].all()


@pytest.mark.slow
class TestTrainedModel:
    def test_blocking_reduction_counts_the_fallback(self, report) -> None:
        blocking = report.blocking
        assert blocking.cartesian_pairs == 93 * 197 == 18321
        assert blocking.blocked_pairs == 1484
        assert blocking.fallback_records == 3
        assert blocking.fallback_pairs == 3 * 197 == 591
        assert round(blocking.reduction_ratio, 4) == 0.9190
        # The ratio must be computed on a pair count that includes the fallback pairs.
        assert blocking.fallback_pairs < blocking.blocked_pairs

    def test_every_em_pass_ran(self, model) -> None:
        assert model.em_sessions == ["zip5", "county_fips", "state"]
        assert model.em_failures == []

    def test_probabilities_are_bimodal(self, report) -> None:
        """A useful model concentrates mass at both ends, not in the middle."""
        counts = dict(
            zip(report.histogram["match_probability"], report.histogram["pairs"], strict=True)
        )
        assert counts["[0, 0.01)"] > 1000
        assert counts["[0.99, 1.01)"] > 40
        middle = counts["[0.1, 0.5)"] + counts["[0.5, 0.9)"]
        assert middle < 50

    def test_no_weight_was_set_by_hand(self, model) -> None:
        """Every trained parameter must be free; a fixed one would be a hand-set weight."""
        settings = model.linker.misc.save_model_to_json()
        for comparison in settings["comparisons"]:
            for level in comparison["comparison_levels"]:
                assert not level.get("fix_m_probability", False)
                assert not level.get("fix_u_probability", False)

    def test_unestimable_levels_are_reported_not_filled_in(self, report) -> None:
        """The three levels no candidate pair fell into stay unset."""
        unestimated = {f"{r.comparison}.{r.level}" for r in report.comparators.unestimated}
        assert unestimated == {
            "place.place_jw_92",
            "alias_place.alias_exact_element",
            "county.county_near_5km",
        }
        for row in report.comparators.unestimated:
            assert row.pairs == 0
            assert row.match_weight is None

    def test_low_support_levels_are_flagged(self, report) -> None:
        """A weight resting on three pairs is arithmetic, not evidence."""
        low = {f"{r.comparison}.{r.level}" for r in report.comparators.rows if r.low_support}
        assert "county.county_near_500m" in low

    def test_zip_is_the_strongest_single_field(self, report) -> None:
        frame = report.comparators.as_frame().set_index(["comparison", "level"])
        assert frame.loc[("zip5", "Exact match on zip5"), "match_weight"] > 6
        assert frame.loc[("county", "county_exact"), "match_weight"] > 6
        # Disagreement on place is strong negative evidence, which is what lets the model
        # reject same-county pairs.
        assert frame.loc[("place", "All other comparisons"), "match_weight"] < -3

    def test_the_alias_comparator_actually_fires(self, report) -> None:
        """The exact-array-intersect version fired on 0 of 18,321 pairs; this one works."""
        frame = report.comparators.as_frame().set_index(["comparison", "level"])
        assert frame.loc[("alias_place", "alias_contains_token"), "match_weight"] > 5

    def test_shared_county_plants_keep_their_discrimination(self, report) -> None:
        """County blocking fails on 92 plants; the question is what is left after it."""
        crowding = report.by_county_crowding.set_index("county_crowding")
        assert set(crowding.index) == {"alone", "shared county"}
        assert crowding.loc["shared county", "plants"] == 92
        assert crowding.loc["alone", "plants"] == 105
        shared_rate = crowding.loc["shared county", "above_0_9"] / 92
        alone_rate = crowding.loc["alone", "above_0_9"] / 105
        assert shared_rate > 0.3
        assert abs(shared_rate - alone_rate) < 0.1

    def test_canadian_plants_are_never_compared_not_wrongly_matched(self, report) -> None:
        """The expected outcome. A high score here would mean the model forces matches."""
        canada = report.canada
        assert len(canada) == 8
        assert (canada["best_match_probability"] == 0.0).all()
        assert set(canada["outcome"]) == {"never compared"}
        assert (canada["candidate_pairs"] == 0).all()

    def test_the_near_miss_plant_is_recovered_by_the_geography_level(self, model) -> None:
        """Indiana Harbor: 255 m outside its county, and the whole point of ADR decision.

        It reaches a reference record, which is literally named "indiana harbor", through
        the `county_near_500m` level -- and it also reaches a second reference record in the same county
        at nearly the same score, which is the shared-county ambiguity made concrete.
        """
        pairs = model.predictions[model.predictions["unique_id_l"] == "P100000120926"]
        assert len(pairs) > 0
        best = pairs.sort_values("match_probability", ascending=False).head(2)
        assert set(best["unique_id_r"]) == {"97", "142"}
        assert (best["match_probability"] > 0.99).all()
        assert (best["gamma_county"] == 2).all()


@pytest.mark.slow
class TestPersistence:
    def test_candidates_land_in_iceberg(self, lake: tuple[Path, Path]) -> None:
        from lakehouse.duck import table_counts

        catalog_db, warehouse = lake
        _, report = build_report(db_path=catalog_db, warehouse=warehouse, write=True)
        counts = table_counts(catalog_db, warehouse)
        assert counts["resolution.gspt_plantgen_candidates"] == report.blocking.blocked_pairs
