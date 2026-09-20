"""Stratified sampling of the pairs a human should look at.

[pandas] Reads the scored candidates out of Iceberg through DuckDB. Nothing here
imports Polars or `ingest`.

The queue is not "the top of the ranking". Labelling only the confident end produces a
precision number and no recall number at all, and it cannot correct the feature
correlation the model is known to have. Every stratum below exists for a reason, and the
two that are easiest to skip -- the confident matches and the confident non-matches --
are the two that make recall computable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from lakehouse.duck import connect
from resolution.report import MATCH_TABLE, WATCHED_COUNTIES

COOK_COUNTY_FIPS = "17031"

# A pair is "contested" when one side has several confident claims on it. This is a
# *sampling* parameter, not an acceptance threshold: no pair is accepted anywhere in
# this repository. It exists to find the cases the machine cannot separate.
CONTESTED_PROBABILITY = 0.9

AMBIGUOUS_BAND = (0.1, 0.9)
HIGH_BAND = 0.99
LOW_BAND = 0.01

DEFAULT_SEED = 20260919


@dataclass(frozen=True)
class Quotas:
    """How many pairs to draw from each stratum.

    `None` means "every pair in the stratum", which is what the three diagnostic strata
    use: they are small, and they are the ones worth exhausting.
    """

    ambiguous: int | None = None
    contested: int | None = None
    cook_county: int | None = None
    high_confidence: int = 30
    low_confidence: int = 30


# Assignment order. A pair belongs to exactly one stratum -- the first it qualifies for
# -- so the queue has no duplicates and the composition table adds up. The overlaps are
# reported separately rather than silently dropped.
STRATUM_ORDER = (
    "ambiguous",
    "contested",
    "cook_county",
    "high_confidence",
    "low_confidence",
)

STRATUM_PURPOSE = {
    "ambiguous": "the model is genuinely unsure; highest information per label",
    "contested": "several confident claims on one record; the machine cannot separate these",
    "cook_county": "county AND ZIP both fail here; exhaustive, so recall is computable",
    "high_confidence": "check for false positives",
    "low_confidence": "check for false negatives -- without these there is no recall",
}


def load_candidates(db_path: Path | None = None, warehouse: Path | None = None) -> pd.DataFrame:
    """Scored candidate pairs, with the identifying fields a human needs to judge them."""
    con = connect(db_path, warehouse)
    try:
        return con.execute(
            f"""
            select
                c.*,
                g.plant_name_english          as gspt_name,
                g.other_plant_names_english   as gspt_other_names,
                g.municipality                as gspt_city,
                g.subnational_unit_province_state as gspt_state,
                g.location_address            as gspt_address,
                g.owner                       as gspt_owner,
                g.start_date                  as gspt_start,
                g.country_area                as gspt_country,
                p.plant_name                  as plantgen_name,
                p.city                        as plantgen_city,
                p.county                      as plantgen_county,
                p.state                       as plantgen_state,
                p.zip_code                    as plantgen_zip,
                p.street_address              as plantgen_address,
                p.plant_start_yr              as plantgen_start,
                p.st_cnty_fips                as plantgen_fips
            from resolution.{MATCH_TABLE} c
            left join (
                select plant_id, any_value(plant_name_english) as plant_name_english,
                       any_value(other_plant_names_english) as other_plant_names_english,
                       any_value(municipality) as municipality,
                       any_value(subnational_unit_province_state)
                           as subnational_unit_province_state,
                       any_value(location_address) as location_address,
                       any_value(owner) as owner,
                       min(start_date) as start_date,
                       any_value(country_area) as country_area
                from raw.gspt_plants group by plant_id
            ) g on g.plant_id = c.unique_id_l
            left join raw.plantgen p on p.plant_id = c.unique_id_r
            """
        ).df()
    finally:
        con.close()


def pair_id(left: str, right: str) -> str:
    """Stable identifier for a pair, so a decision survives a model re-run."""
    return f"{left}|{right}"


def _cook_county_ids(db_path: Path | None, warehouse: Path | None) -> set[str]:
    con = connect(db_path, warehouse)
    try:
        rows = con.execute(
            "select plant_id from raw.plantgen where st_cnty_fips = ?", [COOK_COUNTY_FIPS]
        ).fetchall()
    finally:
        con.close()
    return {row[0] for row in rows}


def contested_pairs(
    candidates: pd.DataFrame, threshold: float = CONTESTED_PROBABILITY
) -> pd.Series:
    """Boolean mask: either side of this pair has more than one confident claim.

    Indiana Harbor is the motivating case. It reaches two PLANTGEN plants that share its
    county, its ZIP *and* its city, so every geographic signal carries zero information
    and only the plant name itself differs. No rule can resolve that, which is exactly
    why it goes to a human rather than to a tie-break heuristic.
    """
    confident = candidates[candidates["match_probability"] >= threshold]
    busy_left = confident["unique_id_l"].value_counts()
    busy_right = confident["unique_id_r"].value_counts()
    return candidates["match_probability"].ge(threshold) & (
        candidates["unique_id_l"].isin(busy_left[busy_left > 1].index)
        | candidates["unique_id_r"].isin(busy_right[busy_right > 1].index)
    )


def assign_strata(
    candidates: pd.DataFrame, cook_ids: set[str], contested_threshold: float = CONTESTED_PROBABILITY
) -> pd.Series:
    """Label every candidate pair with the first stratum it qualifies for."""
    probability = candidates["match_probability"]
    masks = {
        "ambiguous": probability.between(AMBIGUOUS_BAND[0], AMBIGUOUS_BAND[1], inclusive="left"),
        "contested": contested_pairs(candidates, contested_threshold),
        "cook_county": candidates["unique_id_r"].isin(cook_ids),
        "high_confidence": probability >= HIGH_BAND,
        "low_confidence": probability < LOW_BAND,
    }
    stratum = pd.Series("unsampled", index=candidates.index, dtype="object")
    for name in STRATUM_ORDER:
        stratum.loc[masks[name] & (stratum == "unsampled")] = name
    return stratum


def build_queue(
    db_path: Path | None = None,
    warehouse: Path | None = None,
    seed: int = DEFAULT_SEED,
    quotas: Quotas | None = None,
    candidates: pd.DataFrame | None = None,
    cook_ids: set[str] | None = None,
) -> pd.DataFrame:
    """Draw the labelling queue. Deterministic given `seed`."""
    quotas = quotas or Quotas()
    pairs = (candidates if candidates is not None else load_candidates(db_path, warehouse)).copy()
    ids = cook_ids if cook_ids is not None else _cook_county_ids(db_path, warehouse)

    pairs["pair_id"] = [
        pair_id(left, right)
        for left, right in zip(pairs["unique_id_l"], pairs["unique_id_r"], strict=True)
    ]
    pairs["stratum"] = assign_strata(pairs, ids)

    drawn: list[pd.DataFrame] = []
    for name in STRATUM_ORDER:
        stratum = pairs[pairs["stratum"] == name]
        if stratum.empty:
            continue
        quota = getattr(quotas, name)
        if quota is None or quota >= len(stratum):
            drawn.append(stratum)
            continue
        # Sort before sampling so the draw does not depend on the row order the
        # warehouse happened to return.
        drawn.append(stratum.sort_values("pair_id").sample(n=quota, random_state=seed))

    queue = pd.concat(drawn, ignore_index=True)
    queue["queue_seed"] = seed
    # Present the hardest strata first, and make the order itself reproducible.
    queue["stratum_rank"] = queue["stratum"].map({s: i for i, s in enumerate(STRATUM_ORDER)})
    return (
        queue.sort_values(["stratum_rank", "pair_id"])
        .drop(columns="stratum_rank")
        .reset_index(drop=True)
    )


def composition(queue: pd.DataFrame, candidates: pd.DataFrame | None = None) -> pd.DataFrame:
    """What the queue is made of, and what fraction of each stratum it covers."""
    rows = []
    for name in STRATUM_ORDER:
        sampled = int((queue["stratum"] == name).sum())
        available = (
            int((candidates["stratum"] == name).sum())
            if candidates is not None and "stratum" in candidates.columns
            else sampled
        )
        rows.append(
            {
                "stratum": name,
                "in_queue": sampled,
                "available": available,
                "exhaustive": sampled == available,
                "purpose": STRATUM_PURPOSE[name],
            }
        )
    rows.append(
        {
            "stratum": "TOTAL",
            "in_queue": int(len(queue)),
            "available": int(len(candidates)) if candidates is not None else int(len(queue)),
            "exhaustive": False,
            "purpose": "",
        }
    )
    return pd.DataFrame(rows)


def contested_groups(queue: pd.DataFrame) -> dict[str, list[str]]:
    """GSPT record -> every queued candidate for it.

    The UI must show all of a record's candidates together. Presenting them one at a
    time would ask a labeller to judge "is this the one?" without showing the
    alternatives, which is not the same question.
    """
    grouped = queue.groupby("unique_id_l")["pair_id"].apply(list)
    return {left: pairs for left, pairs in grouped.items() if len(pairs) > 1}


def watched_county_note() -> str:
    return f"{WATCHED_COUNTIES[COOK_COUNTY_FIPS]} is sampled exhaustively."


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Show the labelling queue's composition.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--out", type=Path, default=None, help="Optionally write the queue as CSV.")
    args = parser.parse_args(argv)

    candidates = load_candidates()
    candidates["stratum"] = assign_strata(candidates, _cook_county_ids(None, None))
    queue = build_queue(seed=args.seed, candidates=candidates.drop(columns="stratum"))

    print(f"Labelling queue, seed {args.seed}")
    print("-" * 72)
    print(composition(queue, candidates).to_string(index=False))
    print()
    print(f"{len(contested_groups(queue))} records have more than one queued candidate;")
    print("the UI shows each record's candidates side by side rather than one at a time.")
    if args.out:
        queue.to_csv(args.out, index=False)
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
