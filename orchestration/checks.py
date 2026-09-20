"""Asset checks for the invariants dbt cannot express.

[no DataFrame library] Nothing here imports Polars or pandas.

WHAT IS *NOT* HERE
------------------
The 48 dbt assertions are not mirrored. dbt owns them, Dagster runs dbt and surfaces
what it found, and there is exactly one copy of each rule. Re-implementing them here
would create two definitions of the same invariant that drift apart until nobody knows
which is authoritative.

What is here are the three kinds of thing dbt genuinely cannot see:

1. **Cross-asset arithmetic.** dbt can test `bridge_plant_xref` on its own; it cannot
   check that the bridge's row count equals the linker's candidate count plus the
   never-compared plants, because the linker's output is upstream of dbt's world.
2. **Comparisons against the previous run.** A test that passes on today's data tells
   you nothing about whether coverage just fell off a cliff. These read the last
   materialisation's metadata.
3. **Checks that depend on data that does not exist yet.** The label set is empty, so
   the precision check cannot run -- and it must say so rather than pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    asset_check,
)

from lakehouse.paths import REPO_ROOT
from orchestration.resources import DBT_DATABASE

# The reference point the drift checks compare against.
#
# It lives in a TRACKED directory, not in `lake/`. `lake/` is gitignored build output
# that `make clean` deletes, and a baseline that disappears with one `make clean` is not
# a baseline -- the first run afterwards would silently re-establish it from whatever
# the degraded pipeline produced and report success. The baseline is the reference, not
# the artifact, so it is committed and it survives a rebuild.
BASELINE_PATH = REPO_ROOT / "baselines" / "check_baseline.json"

# How far a coverage number may fall before a check fails. Any drop is worth knowing
# about; this is the point at which it stops being noise.
COVERAGE_TOLERANCE = 0


def _is_number(value: object) -> bool:
    """True for a real number. Avoids importing pandas just to call `notna`.

    `tests/test_dataframe_boundary.py` forbids a DataFrame library in `orchestration/`:
    the scheduler calls the layers that own DataFrames, it does not become one of them.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == value


def _connect_lake():
    """Read the Iceberg lake only.

    Used by checks that never look at a mart, so that they do not acquire a dependency
    on dbt having run.
    """
    from lakehouse.duck import connect

    return connect()


def _connect_warehouse():
    """Open the database that has BOTH the lake views and the dbt marts.

    `lakehouse.duck.connect()` builds a fresh in-memory DuckDB holding only the Iceberg
    views, which is right for reading bronze and wrong here: these checks compare a
    bronze count against a mart count, and the marts live in the dbt database file. A
    connection that can only see one side of the comparison fails with a missing schema
    rather than a failed assertion, which is a much less useful error.
    """
    import duckdb

    con = duckdb.connect(str(DBT_DATABASE), read_only=True)
    con.execute("LOAD iceberg;")
    return con


def _read_baseline() -> dict[str, float]:
    if not BASELINE_PATH.exists():
        return {}
    try:
        return json.loads(BASELINE_PATH.read_text())
    except json.JSONDecodeError:
        return {}


def _write_baseline(updates: dict[str, float]) -> None:
    baseline = _read_baseline()
    baseline.update(updates)
    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    BASELINE_PATH.write_text(json.dumps(baseline, indent=2, sort_keys=True))


# --------------------------------------------------------------------------------------
# 1. cross-asset arithmetic
# --------------------------------------------------------------------------------------


@asset_check(
    asset=AssetKey(["resolution", "gspt_plantgen_candidates"]),
    name="bridge_row_count_reconciles_with_the_linker",
    # Reads a dbt mart, so it cannot run until dbt has built it. The key carries the
    # `marts` schema prefix that dagster-dbt gives dbt models; without it the dependency
    # silently points at nothing and the check races the model it is validating.
    additional_deps=[AssetKey(["marts", "bridge_plant_xref"])],
    description=(
        "bridge_plant_xref must hold exactly one row per scored candidate plus one per "
        "plant blocking never reached. dbt cannot check this: the candidate count lives "
        "upstream of dbt's world."
    ),
)
def bridge_reconciles() -> AssetCheckResult:
    con = _connect_warehouse()
    try:
        candidates = con.execute(
            "select count(*) from resolution.gspt_plantgen_candidates"
        ).fetchone()[0]
        bridge_total, bridge_candidates, never_compared = con.execute(
            """
            select
                count(*),
                count(*) filter (where resolution_status = 'candidate'),
                count(*) filter (where resolution_status = 'never_compared')
            from main_marts.bridge_plant_xref
            """
        ).fetchone()
    finally:
        con.close()

    passed = bridge_candidates == candidates and bridge_total == candidates + never_compared
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "linker_candidate_pairs": candidates,
            "bridge_candidate_rows": bridge_candidates,
            "bridge_never_compared_rows": never_compared,
            "bridge_total_rows": bridge_total,
            "expected_total": candidates + never_compared,
        },
    )


@asset_check(
    asset=AssetKey(["raw", "furnace_years"]),
    name="furnace_rows_reconcile_from_bronze_to_mart",
    additional_deps=[AssetKey(["marts", "fct_furnace_year"])],
    description=(
        "Every row dropped between bronze and the fact must be accounted for by a named "
        "filter: superseded vintage, unkeyable South American rows, or the EAF:79 "
        "repeats. An unexplained drop means a filter widened silently."
    ),
)
def furnace_reconciles() -> AssetCheckResult:
    con = _connect_warehouse()
    try:
        bronze, old_vintage, south_american = con.execute(
            """
            select
                count(*),
                count(*) filter (where source_version = 'gspt_2024'),
                count(*) filter (
                    where source_version = 'owner_filled_2025'
                      and alignment_status = 'unaligned_no_key'
                )
            from raw.furnace_years
            """
        ).fetchone()
        mart = con.execute("select count(*) from main_marts.fct_furnace_year").fetchone()[0]
    finally:
        con.close()

    repeats = 24
    expected = bronze - old_vintage - south_american - repeats
    return AssetCheckResult(
        passed=mart == expected,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "bronze_rows": bronze,
            "superseded_vintage": old_vintage,
            "south_american_unkeyable": south_american,
            "eaf79_repeats": repeats,
            "expected_mart_rows": expected,
            "actual_mart_rows": mart,
        },
    )


# --------------------------------------------------------------------------------------
# 2. run-over-run drift
# --------------------------------------------------------------------------------------


@asset_check(
    asset=AssetKey(["geo", "gspt_plant_county"]),
    name="geographic_coverage_has_not_regressed",
    description=(
        "The number of plants with a county FIPS must not fall below the previous run. "
        "A green build on freshly-degraded data is the failure this catches."
    ),
)
def geo_coverage_has_not_regressed() -> AssetCheckResult:
    # Lake only: this check must not wait on dbt, and dbt does not own the answer.
    con = _connect_lake()
    try:
        assigned, total = con.execute(
            "select count(county_fips), count(*) from geo.gspt_plant_county"
        ).fetchone()
    finally:
        con.close()

    baseline = _read_baseline()
    previous = baseline.get("geo_county_assigned")
    if previous is None:
        _write_baseline({"geo_county_assigned": assigned})
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "assigned": assigned,
                "of_plants": total,
                "baseline": "none recorded; this run establishes it",
            },
        )

    passed = assigned >= previous - COVERAGE_TOLERANCE
    if passed:
        _write_baseline({"geo_county_assigned": max(assigned, previous)})
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "assigned": assigned,
            "previous_best": previous,
            "of_plants": total,
            "change": assigned - previous,
        },
    )


@asset_check(
    asset=AssetKey(["raw", "furnace_years"]),
    name="ownership_noise_filtering_is_bounded_on_both_sides",
    additional_deps=[AssetKey(["marts", "fct_ownership_change"])],
    description=(
        "The share of ownership transitions rejected as noise must sit inside a band. "
        "Too low means the filters stopped working; too high means they are eating real "
        "transfers. A one-sided bound would only catch the first."
    ),
)
def ownership_noise_is_bounded() -> AssetCheckResult:
    con = _connect_warehouse()
    try:
        total, credible = con.execute(
            """
            select count(*), count(*) filter (where change_class = 'credible_transfer')
            from main_marts.fct_ownership_change
            """
        ).fetchone()
    finally:
        con.close()

    if total == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "no ownership transitions at all; the model produced nothing"},
        )

    rejected_share = 1 - (credible / total)
    # Both bounds are hand-set from the current data (14 of 18 rejected, 0.78) and are
    # tripwires, not tuned parameters: outside this band something structural changed.
    low, high = 0.30, 0.95
    passed = low <= rejected_share <= high
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "transitions": total,
            "credible_transfers": credible,
            "rejected_as_noise": total - credible,
            "rejected_share": round(rejected_share, 4),
            "lower_bound": low,
            "upper_bound": high,
            "note": "bounds are hand-set tripwires, not validated thresholds",
        },
    )


# --------------------------------------------------------------------------------------
# 3. the check that cannot run yet
# --------------------------------------------------------------------------------------


@asset_check(
    asset=AssetKey(["resolution", "gspt_plantgen_candidates"]),
    name="match_precision_against_human_labels",
    description=(
        "Precision against the human label set, gated on the high_confidence stratum "
        "only. Never a pooled figure: strata are sampled at different rates, and the "
        "labels showed that scoring and assignment fail separately. When no labels "
        "exist this reports a skip, never a pass."
    ),
    blocking=False,
)
def precision_against_labels() -> AssetCheckResult:
    from labeling import store

    counts = store.summary()
    if not counts["has_real_labels"]:
        # NOT `passed=True`. There is nothing to be right about yet, and a passing check
        # here would show up in exactly the same green column as a measured one.
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "status": "SKIPPED - not evaluable",
                "reason": (
                    "the human label set is empty, so precision is undefined. This is "
                    "not a failure of the model and not a pass; it is a measurement "
                    "that cannot be taken yet."
                ),
                "labels_written": counts["lines_written"],
                "synthetic_labels_present": counts["synthetic_only"],
                "next_step": "run `make label` and have a person decide some pairs",
            },
        )

    import logging

    logging.getLogger("splink").setLevel(logging.ERROR)
    from labeling.evaluate import build as build_evaluation

    report = build_evaluation()
    confusion = report.confusion_by_stratum.set_index("stratum")

    # The gate is the `high_confidence` stratum ONLY, and deliberately not a pooled
    # precision across all five.
    #
    # Two reasons. First, the strata are sampled at completely different rates -- three
    # exhaustively, two as 30-pair draws -- so a pooled figure averages over a sample
    # the sampler composed rather than over the world. Second, and more important, it
    # would hide the thing the labels actually revealed: scoring and assignment fail
    # separately, and only one of them is a threshold problem.
    #
    # `contested` is measured and reported here, but it does NOT gate. A contested pair
    # is by definition one where the model put several candidates for one plant above
    # the bar, so at most one can be right and precision near 0.5 is the arithmetic of
    # a coin flip, not a scoring defect. Gating on it would fail this check forever for
    # a reason no threshold can fix. See ADR-009.
    gated_stratum = "high_confidence"
    floor = 0.85
    if gated_stratum not in confusion.index:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "status": "SKIPPED - not evaluable",
                "reason": f"no labelled pairs in the {gated_stratum!r} stratum",
            },
        )

    gated = confusion.loc[gated_stratum]
    precision = float(gated["precision"]) if _is_number(gated["precision"]) else None
    contested = confusion.loc["contested"] if "contested" in confusion.index else None

    metadata: dict[str, Any] = {
        "gated_on": f"{gated_stratum} stratum at p >= 0.99",
        "precision": precision,
        "floor": floor,
        "tp": int(gated["tp"]),
        "fp": int(gated["fp"]),
        "fn": int(gated["fn"]),
        "labels_total": counts["pairs_decided"],
        "annotators": counts["annotators"],
        "note": (
            "NOT a pooled precision. Strata are sampled at different rates, and "
            "scoring and assignment fail separately."
        ),
    }
    if contested is not None and _is_number(contested["precision"]):
        metadata["contested_precision_reported_not_gated"] = float(contested["precision"])
        metadata["contested_note"] = (
            "one-to-one assignment is an open problem, not a threshold problem; see ADR-009"
        )
    # Recall across the whole labelled sample is meaningful because the queue sampled
    # the low-probability end on purpose.
    false_negatives = int(report.confusion_by_stratum["fn"].sum())
    metadata["false_negatives_across_all_strata"] = false_negatives

    return AssetCheckResult(
        passed=precision is not None and precision >= floor,
        severity=AssetCheckSeverity.ERROR,
        metadata=metadata,
    )


def baseline_path() -> Path:
    return BASELINE_PATH
