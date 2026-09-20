"""Build the two comparison frames, one row per plant, from the lake.

[pandas] Reads Iceberg through DuckDB. Nothing here imports Polars or `ingest`.
See docs/adr/ADR-002-dataframe-boundary.md.

Both sides are projected onto one schema so that Splink can compare them column by
column. Only evidence that *both* sources actually carry is included. Two fields that
look obvious are deliberately absent:

* **owner** -- PLANTGEN has no owner column at all.
* **capacity** -- PLANTGEN has no capacity column at all.

Neither is a low-priority signal; neither exists. `PhoneNumber` is likewise one-sided
(PLANTGEN only) and so is unusable.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakehouse.duck import connect
from resolution import normalize as nz


def _texts(*values: object) -> list[str]:
    """Keep only real strings.

    A null coming back through pandas can be `None` or a float `NaN`, and `NaN` is
    truthy, so a plain falsy check lets it into an alias list and then blows up on sort.
    """
    return [v for v in values if isinstance(v, str) and v]


# Order matters: this is the ranking of evidence that survives inside a shared county.
# ZIP first, because it is the only field with enough cardinality to split a county --
# PLANTGEN has 168 distinct ZIPs against 136 distinct counties.
COMPARISON_COLUMNS = (
    "unique_id",
    "source_dataset",
    "plant_name",
    "city",
    "aliases",
    "other_names",
    "state",
    "zip5",
    "street",
    "start_year",
    "county_fips",
    "county_fips_effective",
    "county_distance_m",
    "country_area",
)


def _gspt(con) -> pd.DataFrame:
    """One row per North American GSPT plant, joined to its county assignment.

    The sheet's grain is plant x production unit, so identity columns are collapsed with
    `any_value` (they are constant within a plant id) and `start_date` is taken as the
    earliest unit start, which is the plant's own start.
    """
    raw = con.execute(
        """
        select
            p.plant_id                                    as unique_id,
            any_value(p.plant_name_english)               as plant_name_raw,
            string_agg(distinct p.other_plant_names_english, '; ') as aliases_raw,
            any_value(p.municipality)                     as city_raw,
            any_value(p.subnational_unit_province_state)  as state_raw,
            any_value(p.location_address)                 as address_raw,
            min(p.start_date)                             as start_date_raw,
            any_value(p.coordinate_accuracy)              as coordinate_accuracy,
            any_value(p.country_area)                     as country_area,
            any_value(c.county_fips)                      as county_fips,
            any_value(c.nearest_county_fips)              as nearest_county_fips,
            any_value(c.nearest_county_distance_m)        as nearest_county_distance_m
        from raw.gspt_plants p
        left join geo.gspt_plant_county c using (plant_id)
        where p.country_area in ('United States', 'Canada')
        group by p.plant_id
        order by p.plant_id
        """
    ).df()

    frame = pd.DataFrame({"unique_id": raw["unique_id"], "source_dataset": "gspt"})
    frame["plant_name"] = raw["plant_name_raw"].map(nz.normalize_text)
    frame["city"] = raw["city_raw"].map(nz.normalize_text)
    # `other_names` deliberately EXCLUDES the primary plant name. It is the only
    # genuinely independent naming evidence GSPT has: "ak steel butler works" names the
    # place where the primary name "cleveland cliffs butler steel plant" also does, but
    # the two are different observations. Keeping the primary name in here as well would
    # make this comparator a restatement of the place comparator.
    # GSPT sometimes repeats the primary name inside `Other plant names (English)`
    # (e.g. "Liberty Steel Georgetown plant"), so it is subtracted explicitly rather
    # than assumed absent.
    frame["other_names"] = [
        sorted(set(nz.split_aliases(extra)) - {name})
        for name, extra in zip(frame["plant_name"], raw["aliases_raw"], strict=True)
    ]
    frame["aliases"] = [
        sorted(set(_texts(name, *others)))
        for name, others in zip(frame["plant_name"], frame["other_names"], strict=True)
    ]
    frame["state"] = raw["state_raw"].map(nz.normalize_subdivision)
    frame["zip5"] = raw["address_raw"].map(nz.extract_zip)
    frame["street"] = raw["address_raw"].map(nz.normalize_street)
    frame["start_year"] = raw["start_date_raw"].map(nz.parse_year)
    frame["country_area"] = raw["country_area"]

    # Decision: coordinates flagged `approximate` are excluded from every geographic
    # signal, rather than down-weighted. They are not imprecise observations; they are
    # placeholders. "Nucor Steel Pacific Northwest" carries 37.0902, -95.7129 -- the
    # geographic centre of the contiguous United States -- which resolves confidently to
    # Montgomery County, Kansas. A weight cannot fix a fabricated value.
    usable_geo = raw["coordinate_accuracy"] != "approximate"
    frame["county_fips"] = raw["county_fips"].where(usable_geo)
    frame["county_fips_effective"] = (
        raw["county_fips"].fillna(raw["nearest_county_fips"]).where(usable_geo)
    )
    frame["county_distance_m"] = (
        raw["nearest_county_distance_m"].where(raw["county_fips"].isna(), 0.0).where(usable_geo)
    )
    return frame[list(COMPARISON_COLUMNS)]


def _plantgen(con) -> pd.DataFrame:
    """One row per PLANTGEN plant.

    `PlantName` is a place name on 168 of 197 rows, so it is carried as both the name
    *and* an alias, and the model compares it against GSPT's city as well as its name.
    PLANTGEN has a county on every row, so its geographic distance is always zero.
    """
    raw = con.execute(
        """
        select
            plant_id       as unique_id,
            plant_name     as plant_name_raw,
            city           as city_raw,
            state          as state_raw,
            zip_code       as zip5,
            street_address as street_raw,
            plant_start_yr as start_year,
            st_cnty_fips   as county_fips
        from raw.plantgen
        order by plant_id
        """
    ).df()

    frame = pd.DataFrame({"unique_id": raw["unique_id"], "source_dataset": "plantgen"})
    frame["plant_name"] = raw["plant_name_raw"].map(nz.normalize_text)
    frame["city"] = raw["city_raw"].map(nz.normalize_text)
    # PLANTGEN's "plant name" is the city on 168 of 197 rows, so both strings go into the
    # alias list and the model gets to compare either against GSPT's name or its city.
    frame["aliases"] = [
        sorted(set(_texts(name, city)))
        for name, city in zip(frame["plant_name"], frame["city"], strict=True)
    ]
    # PLANTGEN has no "other names" column at all; the list exists so both frames share
    # a schema, and the comparator that uses it is therefore one-sided by construction.
    frame["other_names"] = [[] for _ in range(len(frame))]
    frame["state"] = raw["state_raw"].map(nz.normalize_subdivision)
    frame["zip5"] = raw["zip5"].map(nz.clean)
    frame["street"] = raw["street_raw"].map(nz.normalize_street)
    frame["start_year"] = raw["start_year"].map(nz.plausible_year).astype("Int64")
    frame["country_area"] = "United States"
    frame["county_fips"] = raw["county_fips"]
    frame["county_fips_effective"] = raw["county_fips"]
    frame["county_distance_m"] = 0.0
    return frame[list(COMPARISON_COLUMNS)]


def build_frames(
    db_path: Path | None = None, warehouse: Path | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(gspt, plantgen)`` comparison frames, read out of the lake."""
    con = connect(db_path, warehouse)
    try:
        gspt, plantgen = _gspt(con), _plantgen(con)
    finally:
        con.close()

    for frame, name in ((gspt, "gspt"), (plantgen, "plantgen")):
        if frame["unique_id"].duplicated().any():
            raise ValueError(f"{name} frame is not one row per plant")
        frame["start_year"] = frame["start_year"].astype("Int64")
    return gspt, plantgen


def coverage(frame: pd.DataFrame) -> dict[str, int]:
    """Non-null count per evidence column, for reporting what the model had to work with."""
    counts = {"rows": int(len(frame))}
    for column in ("plant_name", "city", "state", "zip5", "street", "start_year", "county_fips"):
        counts[column] = int(frame[column].notna().sum())
    counts["aliases"] = int(sum(1 for a in frame["aliases"] if len(a) > 0))
    counts["other_names"] = int(sum(1 for a in frame["other_names"] if len(a) > 0))
    return counts
