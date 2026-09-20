"""Point-in-polygon assignment of plant coordinates to US counties.

[pandas / GeoPandas] This layer never imports Polars and never imports `ingest`. It
reads the bronze tables back out of Iceberg through DuckDB.
See docs/adr/ADR-002-dataframe-boundary.md.

The Census file is EPSG:4269 (NAD83); GSPT coordinates are plain WGS84 lat/lon. The two
datums differ by well under a metre in North America, but the frames are still assigned
and aligned explicitly rather than silently assumed to match, because an unset CRS is
the usual way a spatial join ends up quietly wrong.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

from geo.download import ensure_county_boundaries

# GSPT publishes plain lat/lon; the Census cartographic files are NAD83.
SOURCE_CRS = "EPSG:4326"

# NAD83 / Conus Albers: an equal-area projection in metres, used only to measure how far
# outside every county an unassigned point fell. Distances in degrees are meaningless.
DISTANCE_CRS = "EPSG:5070"

COUNTY_COLUMNS = ["GEOID", "STATEFP", "COUNTYFP", "NAME", "STUSPS", "STATE_NAME"]


def load_counties(
    shapefile: Path | None = None, cache_dir: Path | None = None, allow_download: bool = True
) -> gpd.GeoDataFrame:
    """Load the county boundaries, downloading them once if the cache is empty."""
    path = (
        Path(shapefile)
        if shapefile is not None
        else ensure_county_boundaries(cache_dir=cache_dir, allow_download=allow_download)
    )
    counties = gpd.read_file(path, columns=COUNTY_COLUMNS)
    if counties.crs is None:
        raise ValueError(f"{path} has no CRS; refusing to guess one")
    return counties


def assign_counties(
    plants: pd.DataFrame,
    counties: gpd.GeoDataFrame,
    id_column: str = "plant_id",
    lat_column: str = "latitude",
    lon_column: str = "longitude",
) -> pd.DataFrame:
    """Assign each plant a county GEOID by point-in-polygon.

    Rows without coordinates are kept, with a null GEOID, so the output has one row per
    input row and "no coordinates" stays distinguishable from "coordinates fell outside
    every county". A `predicate="within"` join is used rather than `"intersects"`, so a
    point exactly on a shared boundary does not produce two rows; the row count is
    asserted afterwards either way.
    """
    located = plants[plants[lat_column].notna() & plants[lon_column].notna()]
    points = gpd.GeoDataFrame(
        located,
        geometry=gpd.points_from_xy(located[lon_column], located[lat_column]),
        crs=SOURCE_CRS,
    ).to_crs(counties.crs)

    joined = gpd.sjoin(points, counties, how="left", predicate="within")
    if len(joined) != len(points):
        duplicated = joined[joined.duplicated(id_column, keep=False)][id_column].unique()
        raise ValueError(
            f"county assignment produced {len(joined)} rows for {len(points)} plants; "
            f"ambiguous: {sorted(duplicated)}"
        )

    assigned = joined[[id_column, "GEOID", "STATEFP", "NAME", "STUSPS"]].rename(
        columns={
            "GEOID": "county_fips",
            "STATEFP": "county_state_fips",
            "NAME": "county_name",
            "STUSPS": "county_state",
        }
    )
    result = plants.merge(assigned, on=id_column, how="left")
    if len(result) != len(plants):
        raise ValueError(f"merge changed the row count: {len(plants)} -> {len(result)}")

    outside = points[
        ~points[id_column].isin(assigned.loc[assigned["county_fips"].notna(), id_column])
    ]
    result = result.merge(nearest_county(outside, counties, id_column), on=id_column, how="left")
    result["county_assignment_status"] = _status(result)
    return result


def _status(result: pd.DataFrame) -> pd.Series:
    """Three distinguishable outcomes, so a null county never has to be interpreted."""
    return pd.Series(
        [
            "assigned"
            if pd.notna(fips)
            else ("no_coordinates" if not has else "outside_county_boundaries")
            for fips, has in zip(result["county_fips"], result["has_coordinates"], strict=True)
        ],
        index=result.index,
        dtype="object",
    )


def nearest_county(
    points: gpd.GeoDataFrame, counties: gpd.GeoDataFrame, id_column: str = "plant_id"
) -> pd.DataFrame:
    """For points that fell outside every county, record the nearest one and how far.

    This is *evidence, not a decision*. A coastal plant whose generalised shoreline puts
    it 255 m into the water and a placeholder coordinate 600 km from anywhere both come
    out of the spatial join with a null county; only the distance separates them. The
    number is recorded so the matching layer can draw its own line, and so nothing is
    silently snapped to a county it was never in.
    """
    columns = [id_column, "nearest_county_fips", "nearest_county_name", "nearest_county_distance_m"]
    if points.empty:
        return pd.DataFrame(columns=columns)

    metric_points = points.to_crs(DISTANCE_CRS)
    metric_counties = counties.to_crs(DISTANCE_CRS)
    rows = []
    for identifier, geometry in zip(metric_points[id_column], metric_points.geometry, strict=True):
        distances = metric_counties.distance(geometry)
        closest = distances.idxmin()
        rows.append(
            {
                id_column: identifier,
                "nearest_county_fips": metric_counties.loc[closest, "GEOID"],
                "nearest_county_name": metric_counties.loc[closest, "NAME"],
                "nearest_county_distance_m": float(distances.loc[closest]),
            }
        )
    return pd.DataFrame(rows, columns=columns)
