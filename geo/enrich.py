"""Attach a county FIPS to every GSPT plant, and measure what that path can actually do.

[pandas] Reads the bronze tables back out of Iceberg through DuckDB; writes one derived
Iceberg table, `geo.gspt_plant_county`. Nothing here imports Polars or `ingest`.
See docs/adr/ADR-002-dataframe-boundary.md.

Coordinates are the strongest link between GSPT and PLANTGEN: GSPT publishes lat/lon
and PLANTGEN publishes `StCntyFIPS`, so resolving a coordinate to a county puts both
sources in the same key space without needing a name match. This module builds that key
and then reports honestly on its ceiling -- a county that contains several PLANTGEN
plants cannot identify any one of them, and that residual is what P3 has to cover with
other evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow as pa

from geo.counties import assign_counties, load_counties
from lakehouse.catalog import write_table
from lakehouse.duck import connect
from lakehouse.paths import GEO_NAMESPACE

PLANT_COUNTY_TABLE = "gspt_plant_county"

NORTH_AMERICA = ("United States", "Canada")


def _read_gspt_plants(db_path: Path | None, warehouse: Path | None) -> pd.DataFrame:
    """One row per North American GSPT *plant*, not per production unit.

    The sheet's grain is plant x production unit (111 rows, 93 ids), but the identity
    columns -- name, owner, coordinates, address -- are constant within a plant id, so
    geography is a plant-level fact and is computed once per id.
    """
    con = connect(db_path, warehouse)
    try:
        return con.execute(
            """
            select
                plant_id,
                any_value(plant_name_english)  as plant_name_english,
                any_value(municipality)        as municipality,
                any_value(subnational_unit_province_state) as state_name,
                any_value(country_area)        as country_area,
                any_value(latitude)            as latitude,
                any_value(longitude)           as longitude,
                any_value(coordinate_accuracy) as coordinate_accuracy,
                bool_or(has_coordinates)       as has_coordinates
            from raw.gspt_plants
            where country_area in ('United States', 'Canada')
            group by plant_id
            order by plant_id
            """
        ).df()
    finally:
        con.close()


def _read_plantgen_counties(db_path: Path | None, warehouse: Path | None) -> pd.DataFrame:
    con = connect(db_path, warehouse)
    try:
        return con.execute(
            """
            select st_cnty_fips, count(*) as plantgen_plants
            from raw.plantgen
            group by st_cnty_fips
            order by st_cnty_fips
            """
        ).df()
    finally:
        con.close()


@dataclass(frozen=True)
class GeoCoverageReport:
    """What the coordinate -> county path delivers, and where it runs out."""

    plants_total: int
    plants_with_coordinates: int
    plants_without_coordinates: int
    plants_without_coordinates_ids: list[str]
    county_assigned: int
    county_unassigned: int
    county_unassigned_detail: dict[str, int]
    matched_plantgen_county: int
    unmatched_plantgen_county: int
    by_accuracy: dict[str, dict[str, int]] = field(default_factory=dict)
    plantgen_counties_total: int = 0
    plantgen_counties_with_one_plant: int = 0
    plantgen_counties_with_many_plants: int = 0
    plantgen_plants_in_shared_counties: int = 0
    plantgen_plants_alone_in_county: int = 0

    def render(self) -> str:
        lines = [
            "GSPT -> county FIPS coverage",
            "-" * 64,
            f"  North American GSPT plants                    {self.plants_total:>5d}",
            f"    with usable coordinates                     {self.plants_with_coordinates:>5d}",
            f"    without coordinates                         {self.plants_without_coordinates:>5d}"
            f"   {self.plants_without_coordinates_ids}",
            f"  assigned a county FIPS                        {self.county_assigned:>5d}",
            f"  not assigned                                  {self.county_unassigned:>5d}"
            f"   {self.county_unassigned_detail}",
            "",
            "  of the assigned, against PLANTGEN's county codes",
            f"    county present in PLANTGEN                  {self.matched_plantgen_county:>5d}",
            f"    county absent from PLANTGEN                 {self.unmatched_plantgen_county:>5d}",
            "",
            "  by coordinate accuracy (plants)",
        ]
        for accuracy, counts in sorted(self.by_accuracy.items()):
            lines.append(
                f"    {accuracy:<12s} plants {counts['plants']:>4d}"
                f"  assigned {counts['assigned']:>4d}"
                f"  matched PLANTGEN county {counts['matched']:>4d}"
            )
        lines += [
            "",
            "  ceiling of county-level blocking, measured on PLANTGEN",
            f"    distinct counties                           {self.plantgen_counties_total:>5d}",
            f"      holding exactly one plant                 {self.plantgen_counties_with_one_plant:>5d}",
            f"      holding more than one                     {self.plantgen_counties_with_many_plants:>5d}",
            f"    plants alone in their county                {self.plantgen_plants_alone_in_county:>5d}",
            f"    plants sharing a county                     {self.plantgen_plants_in_shared_counties:>5d}",
        ]
        return "\n".join(lines)


def build(
    db_path: Path | None = None,
    warehouse: Path | None = None,
    cache_dir: Path | None = None,
    allow_download: bool = True,
    write: bool = True,
) -> tuple[pd.DataFrame, GeoCoverageReport]:
    """Assign counties, persist the result, and measure the path's reach."""
    plants = _read_gspt_plants(db_path, warehouse)
    counties = load_counties(cache_dir=cache_dir, allow_download=allow_download)
    enriched = assign_counties(plants, counties)

    plantgen = _read_plantgen_counties(db_path, warehouse)
    plantgen_codes = set(plantgen["st_cnty_fips"])
    enriched["in_plantgen_county"] = (
        enriched["county_fips"].isin(plantgen_codes) & enriched["county_fips"].notna()
    )

    if write:
        write_table(
            GEO_NAMESPACE,
            PLANT_COUNTY_TABLE,
            pa.Table.from_pandas(enriched, preserve_index=False),
            db_path=db_path,
            warehouse=warehouse,
        )
    return enriched, summarise(enriched, plantgen)


def summarise(enriched: pd.DataFrame, plantgen: pd.DataFrame) -> GeoCoverageReport:
    """Turn the enriched frame into counts. Absolute numbers, not percentages."""
    with_coords = enriched[enriched["has_coordinates"]]
    without_coords = enriched[~enriched["has_coordinates"]]
    assigned = enriched[enriched["county_fips"].notna()]
    unassigned = with_coords[with_coords["county_fips"].isna()]

    by_accuracy: dict[str, dict[str, int]] = {}
    for accuracy, group in enriched.groupby("coordinate_accuracy", dropna=False):
        by_accuracy[str(accuracy)] = {
            "plants": int(len(group)),
            "assigned": int(group["county_fips"].notna().sum()),
            "matched": int(group["in_plantgen_county"].sum()),
        }

    counties_total = int(len(plantgen))
    shared = plantgen[plantgen["plantgen_plants"] > 1]
    alone = plantgen[plantgen["plantgen_plants"] == 1]

    return GeoCoverageReport(
        plants_total=int(len(enriched)),
        plants_with_coordinates=int(len(with_coords)),
        plants_without_coordinates=int(len(without_coords)),
        plants_without_coordinates_ids=sorted(without_coords["plant_id"].tolist()),
        county_assigned=int(len(assigned)),
        county_unassigned=int(len(unassigned)),
        county_unassigned_detail=(
            unassigned["country_area"].value_counts().to_dict() if len(unassigned) else {}
        ),
        matched_plantgen_county=int(enriched["in_plantgen_county"].sum()),
        unmatched_plantgen_county=int(len(assigned) - enriched["in_plantgen_county"].sum()),
        by_accuracy=by_accuracy,
        plantgen_counties_total=counties_total,
        plantgen_counties_with_one_plant=int(len(alone)),
        plantgen_counties_with_many_plants=int(len(shared)),
        plantgen_plants_in_shared_counties=int(shared["plantgen_plants"].sum()),
        plantgen_plants_alone_in_county=int(alone["plantgen_plants"].sum()),
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Attach county FIPS to GSPT plants.")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Fail rather than download if the boundary cache is empty.",
    )
    args = parser.parse_args(argv)
    _, report = build(allow_download=not args.offline)
    print(report.render())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
