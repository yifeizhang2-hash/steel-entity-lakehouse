"""The Splink link model: blocking, comparisons, and EM-estimated weights.

[pandas] Splink runs on its own DuckDB backend over the two pandas frames built by
`resolution.features`. Nothing here imports Polars or `ingest`.

Nothing in this module sets a match weight by hand. Every m and u probability is
estimated from the data; where estimation fails or produces something implausible, it is
reported rather than patched. The two numbers that *are* stated up front are stated
because they follow from the structure of the problem, and both are documented where
they appear.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from resolution.features import build_frames

logger = logging.getLogger(__name__)

# Blocking. Each rule is a separate pass; Splink takes the union of the pairs they
# generate. County first (the P2 key), then ZIP (the highest-cardinality shared field),
# then state (broad, but the only thing that catches a plant whose county and ZIP are
# both missing).
BLOCKING_RULES: tuple[Any, ...] = (
    block_on("county_fips"),
    block_on("zip5"),
    block_on("state"),
)

# The fallback. A record with no county, no ZIP and no state matches none of the rules
# above and would silently never be compared to anything -- it would not appear as a
# non-match, it would simply be absent. Three GSPT plants are in exactly that state
# (the placeholder-coordinate ones), so they are compared against the whole other side.
# The pairs this generates are counted in the reduction ratio; a reduction ratio that
# quietly excludes the records it failed to block is not a reduction ratio.
FALLBACK_RULE = "(NOT l.has_blocking_key) OR (NOT r.has_blocking_key)"

ALL_BLOCKING_RULES: tuple[Any, ...] = (*BLOCKING_RULES, FALLBACK_RULE)


def add_blocking_key_flag(frame: pd.DataFrame) -> pd.DataFrame:
    """Mark records that at least one blocking rule can reach."""
    frame = frame.copy()
    frame["has_blocking_key"] = (
        frame["county_fips"].notna() | frame["zip5"].notna() | frame["state"].notna()
    )
    return frame


def _near_county(metres: int) -> str:
    """SQL for "same county once each side's nearest county is allowed, within `metres`".

    Splink resolves a custom level's input columns from the `"<col>_l"` / `"<col>_r"`
    suffix form, so the conditions are written that way rather than as `l.col` / `r.col`;
    the latter parses at predict time but fails when the same SQL is re-used to
    aggregate m and u counts.
    """
    return (
        '"county_fips_effective_l" = "county_fips_effective_r" '
        'AND greatest(coalesce("county_distance_m_l", 1e9), '
        f'coalesce("county_distance_m_r", 1e9)) <= {metres}'
    )


def _geography_comparison() -> cl.CustomComparison:
    """County agreement as an *ordered* category, not a boolean.

    P2 found a plant whose coordinates fall 255 m outside the generalised county
    boundary it belongs to, because the Census 1:500k shoreline cuts inland of it. That
    is not an exact county match and it is not nothing. Rather than deciding here what a
    255 m miss is worth, the levels are laid out in order and EM estimates a weight for
    each one from the data. This is the point of using a probabilistic model instead of
    a hand-tuned threshold.

    Plants whose coordinates are `approximate` arrive with every geographic column null
    (see `resolution.features`) and so fall to the null level: no evidence, rather than
    false evidence.
    """
    return cl.CustomComparison(
        output_column_name="county",
        comparison_description="County FIPS, exact then near-miss by distance",
        comparison_levels=[
            cll.And(
                cll.NullLevel("county_fips_effective"),
                cll.NullLevel("county_fips_effective"),
            ),
            {
                "sql_condition": '"county_fips_l" = "county_fips_r"',
                "label_for_charts": "county_exact",
            },
            {
                "sql_condition": _near_county(500),
                "label_for_charts": "county_near_500m",
            },
            {
                "sql_condition": _near_county(5000),
                "label_for_charts": "county_near_5km",
            },
            cll.ElseLevel(),
        ],
    )


def _place_comparison() -> cl.CustomComparison:
    """All place-name agreement, in one comparator.

    PLANTGEN's `PlantName` equals its own `City` on 168 of 197 rows: it is a place name
    under a plant name's heading. So the useful question is not "do the plant names
    agree" but "do these two records name the same place", and the answer can come from
    any of four column pairings -- name/name, name/city, city/name, city/city.

    They are folded into **one** comparator rather than two on purpose. Splink assumes
    comparisons are conditionally independent given match status; a separate `city`
    comparator alongside a name-against-city one would be strongly correlated (because
    PLANTGEN's name *is* its city most of the time) and the same agreement would be
    counted twice, inflating every match weight.
    """
    exact = (
        '"city_l" = "city_r" OR "plant_name_l" = "plant_name_r" '
        'OR "plant_name_l" = "city_r" OR "city_l" = "plant_name_r"'
    )
    fuzzy = (
        'jaro_winkler_similarity("city_l", "city_r") >= {t} '
        'OR jaro_winkler_similarity("plant_name_l", "plant_name_r") >= {t} '
        'OR jaro_winkler_similarity("plant_name_l", "city_r") >= {t} '
        'OR jaro_winkler_similarity("city_l", "plant_name_r") >= {t}'
    )
    return cl.CustomComparison(
        output_column_name="place",
        comparison_description="Same place, across name and city in either direction",
        comparison_levels=[
            cll.And(cll.NullLevel("plant_name"), cll.NullLevel("city")),
            {"sql_condition": exact, "label_for_charts": "place_exact"},
            {"sql_condition": fuzzy.format(t="0.92"), "label_for_charts": "place_jw_92"},
            {"sql_condition": fuzzy.format(t="0.85"), "label_for_charts": "place_jw_85"},
            cll.ElseLevel(),
        ],
    )


# GSPT's alias vocabulary and PLANTGEN's share **zero** strings: 167 distinct against
# 190, measured over all 93 x 197 pairs. GSPT writes company-plus-plant strings
# ("gerdau midlothian steel mill"), PLANTGEN writes bare place names ("dearborn").
# An exact array intersection therefore fires on 0 pairs and neither its m nor its u can
# be estimated at all. Token containment -- does one of GSPT's *other* names contain the
# PLANTGEN place as a whole word -- does fire, and separates correctly
# ("ak steel butler works" against "butler"). The comparator was changed because the
# exact-match form was measured to be empty; the weights remain EM's to decide.
#
# `other_names` excludes GSPT's primary plant name, which keeps this comparator
# independent of the place comparator above rather than a restatement of it.
_ALIAS_ELEMENT = (
    'list_contains("other_names_l", "plant_name_r") '
    'OR list_contains("other_names_l", "city_r") '
    'OR list_contains("other_names_r", "plant_name_l") '
    'OR list_contains("other_names_r", "city_l")'
)

_ALIAS_TOKEN_TEMPLATE = (
    'len(list_filter("other_names_{a}", x -> '
    "list_contains(string_split(x, ' '), \"plant_name_{b}\") "
    "OR list_contains(string_split(x, ' '), \"city_{b}\"))) > 0"
)

_ALIAS_TOKEN = " OR ".join(
    (
        _ALIAS_TOKEN_TEMPLATE.format(a="l", b="r"),
        _ALIAS_TOKEN_TEMPLATE.format(a="r", b="l"),
    )
)


def _alias_comparison() -> cl.CustomComparison:
    """Does either source's *secondary* names name the place the other source records?"""
    return cl.CustomComparison(
        output_column_name="alias_place",
        comparison_description="Secondary plant names containing the other source's place",
        comparison_levels=[
            cll.And(cll.NullLevel("other_names"), cll.NullLevel("other_names")),
            {"sql_condition": _ALIAS_ELEMENT, "label_for_charts": "alias_exact_element"},
            {"sql_condition": _ALIAS_TOKEN, "label_for_charts": "alias_contains_token"},
            cll.ElseLevel(),
        ],
    )


def build_settings() -> SettingsCreator:
    """The full model specification.

    `probability_two_random_records_match` is not tuned. It is the structural upper
    bound for a one-to-one link between 93 and 197 records: at most min(93, 197) = 93
    pairs out of 93 x 197 can be true matches, so the prior is 93 / 18,321 = 1 / 197.
    Any larger value would assert that a GSPT plant matches more than one PLANTGEN
    plant, which the problem forbids.
    """
    return SettingsCreator(
        link_type="link_only",
        probability_two_random_records_match=1 / 197,
        blocking_rules_to_generate_predictions=list(ALL_BLOCKING_RULES),
        comparisons=[
            cl.ExactMatch("zip5").configure(term_frequency_adjustments=True),
            cl.LevenshteinAtThresholds("street", [1, 3]),
            _place_comparison(),
            _alias_comparison(),
            cl.CustomComparison(
                output_column_name="start_year",
                comparison_description="Start year, forward evidence only",
                comparison_levels=[
                    # PLANTGEN's start year is missing on 108 of 197 rows. A missing year
                    # must count as no evidence, never as disagreement, or the model would
                    # penalise correct pairs for a gap in the reference data.
                    cll.Or(cll.NullLevel("start_year"), cll.NullLevel("start_year")),
                    {
                        "sql_condition": '"start_year_l" = "start_year_r"',
                        "label_for_charts": "same_year",
                    },
                    {
                        "sql_condition": 'abs("start_year_l" - "start_year_r") <= 3',
                        "label_for_charts": "within_3_years",
                    },
                    cll.ElseLevel(),
                ],
            ),
            _geography_comparison(),
        ],
        retain_intermediate_calculation_columns=True,
    )


@dataclass
class TrainedModel:
    linker: Linker
    predictions: pd.DataFrame
    gspt: pd.DataFrame
    plantgen: pd.DataFrame
    em_sessions: list[str]
    em_failures: list[str]


# EM training passes. Each fixes one field by blocking on it and estimates the rest;
# between them every comparison gets estimated at least once.
EM_TRAINING_RULES = ("zip5", "county_fips", "state")


def train(
    db_path: Path | None = None,
    warehouse: Path | None = None,
    frames: tuple[pd.DataFrame, pd.DataFrame] | None = None,
    max_pairs: float = 1e6,
    seed: int = 17,
) -> TrainedModel:
    """Fit the model with EM and return it together with its predictions."""
    gspt, plantgen = frames if frames is not None else build_frames(db_path, warehouse)
    gspt, plantgen = add_blocking_key_flag(gspt), add_blocking_key_flag(plantgen)

    linker = Linker(
        [gspt, plantgen],
        build_settings(),
        db_api=DuckDBAPI(),
        input_table_aliases=["gspt", "plantgen"],
    )

    # u is estimated from random pairs, which are overwhelmingly non-matches. This needs
    # no labels and no assumptions beyond that.
    linker.training.estimate_u_using_random_sampling(max_pairs=max_pairs, seed=seed)

    sessions: list[str] = []
    failures: list[str] = []
    for column in EM_TRAINING_RULES:
        try:
            linker.training.estimate_parameters_using_expectation_maximisation(block_on(column))
            sessions.append(column)
        except Exception as error:  # noqa: BLE001 - a failed pass is reported, not patched
            logger.warning("EM training blocked on %s failed: %s", column, error)
            failures.append(f"{column}: {error}")

    predictions = linker.inference.predict(threshold_match_probability=0.0).as_pandas_dataframe()
    return TrainedModel(
        linker=linker,
        predictions=predictions,
        gspt=gspt,
        plantgen=plantgen,
        em_sessions=sessions,
        em_failures=failures,
    )
