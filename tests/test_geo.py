"""Geo enrichment: caching, point-in-polygon, and the measured reach of the county path.

The coverage numbers here are assertions, not documentation. If the county assignment
regresses, these fail.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from geo import download
from geo.counties import DISTANCE_CRS, assign_counties, load_counties, nearest_county
from geo.enrich import build
from ingest.pipeline import run as run_ingest

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def counties() -> gpd.GeoDataFrame:
    if not download.is_cached():
        pytest.skip("county boundaries not cached; run `make geo` once with network access")
    return load_counties()


@pytest.fixture(scope="module")
def lake(tmp_path_factory, plantgen_csv, gspt_xlsx, furnace_files) -> tuple[Path, Path]:
    """A throwaway lake, so these tests do not depend on one already existing."""
    root = tmp_path_factory.mktemp("geo_lake")
    warehouse, catalog_db = root / "warehouse", root / "catalog.db"
    run_ingest(
        tables=("plantgen", "gspt_plants"),
        warehouse=warehouse,
        pipelines_dir=root / "dlt",
        catalog_db=catalog_db,
    )
    return catalog_db, warehouse


@pytest.fixture(scope="module")
def built(counties: gpd.GeoDataFrame, lake: tuple[Path, Path]):
    catalog_db, warehouse = lake
    return build(db_path=catalog_db, warehouse=warehouse, write=True)


@pytest.fixture(scope="module")
def enriched(built) -> pd.DataFrame:
    return built[0]


@pytest.fixture(scope="module")
def report(built):
    return built[1]


class TestCache:
    def test_a_cached_shapefile_is_not_re_downloaded(self, tmp_path: Path, counties) -> None:
        """`allow_download=False` proves the cache alone is enough."""
        cached = download.shapefile_path()
        assert download.ensure_county_boundaries(allow_download=False) == cached

    def test_missing_cache_without_network_gives_an_actionable_error(self, tmp_path: Path) -> None:
        with pytest.raises(download.BoundaryFileUnavailableError) as excinfo:
            download.ensure_county_boundaries(cache_dir=tmp_path, allow_download=False)
        message = str(excinfo.value)
        assert "make geo" in message
        assert download.COUNTY_VINTAGE in message

    def test_only_census_gov_is_contactable(self) -> None:
        assert download.ALLOWED_HOST == "www2.census.gov"
        assert download.COUNTY_URL.startswith("https://www2.census.gov/")
        with pytest.raises(ValueError, match="only www2.census.gov"):
            download._check_host("https://example.com/counties.zip")

    def test_the_small_cartographic_file_is_the_one_used(self) -> None:
        """~11.6 MB, not the ~80 MB TIGER file. See the module docstring for the cost."""
        assert download.COUNTY_VINTAGE == "cb_2023_us_county_500k"


class TestCountyGeometry:
    def test_boundaries_have_a_crs(self, counties: gpd.GeoDataFrame) -> None:
        assert counties.crs is not None
        assert len(counties) == 3235

    @pytest.mark.parametrize(
        ("name", "lat", "lon", "geoid", "county"),
        [
            ("downtown Pittsburgh", 40.4406, -79.9959, "42003", "Allegheny"),
            ("downtown Chicago", 41.8781, -87.6298, "17031", "Cook"),
            ("Butler, PA (a reference record)", 40.8612, -79.8953, "42019", "Butler"),
        ],
    )
    def test_known_points_land_in_the_right_county(
        self,
        counties: gpd.GeoDataFrame,
        name: str,
        lat: float,
        lon: float,
        geoid: str,
        county: str,
    ) -> None:
        plants = pd.DataFrame(
            {"plant_id": [name], "latitude": [lat], "longitude": [lon], "has_coordinates": [True]}
        )
        result = assign_counties(plants, counties)
        assert result.loc[0, "county_fips"] == geoid
        assert result.loc[0, "county_name"] == county
        assert result.loc[0, "county_assignment_status"] == "assigned"

    def test_rows_without_coordinates_survive_with_a_null_county(
        self, counties: gpd.GeoDataFrame
    ) -> None:
        plants = pd.DataFrame(
            {
                "plant_id": ["a", "b"],
                "latitude": [40.9034, None],
                "longitude": [-79.9198, None],
                "has_coordinates": [True, False],
            }
        )
        result = assign_counties(plants, counties)
        assert len(result) == 2
        assert result.loc[result["plant_id"] == "b", "county_fips"].isna().all()
        assert result.loc[result["plant_id"] == "b", "county_assignment_status"].tolist() == [
            "no_coordinates"
        ]


class TestNearestCounty:
    def test_distance_is_measured_in_metres(self, counties: gpd.GeoDataFrame) -> None:
        assert DISTANCE_CRS == "EPSG:5070"

    def test_empty_input_returns_an_empty_frame(self, counties: gpd.GeoDataFrame) -> None:
        empty = gpd.GeoDataFrame({"plant_id": []}, geometry=[], crs="EPSG:4326")
        assert nearest_county(empty, counties).empty


class TestCoverage:
    """Acceptance numbers for the coordinate -> county path, as absolute counts."""

    def test_plant_level_grain(self, report) -> None:
        assert report.plants_total == 93
        assert report.plants_with_coordinates == 92
        assert report.plants_without_coordinates == 1
        assert report.plants_without_coordinates_ids == ["P100000121251"]

    def test_how_many_got_a_county(self, report) -> None:
        assert report.county_assigned == 83
        assert report.county_unassigned == 9
        assert report.county_unassigned_detail == {"Canada": 8, "United States": 1}

    def test_how_many_land_in_a_plantgen_county(self, report) -> None:
        assert report.matched_plantgen_county == 68
        assert report.unmatched_plantgen_county == 15

    def test_accuracy_split(self, report) -> None:
        """`approximate` is not a worse `exact`; it matches nothing at all."""
        assert report.by_accuracy["exact"] == {"plants": 90, "assigned": 81, "matched": 68}
        assert report.by_accuracy["approximate"] == {"plants": 3, "assigned": 2, "matched": 0}

    def test_county_blocking_ceiling(self, report) -> None:
        assert report.plantgen_counties_total == 136
        assert report.plantgen_counties_with_one_plant == 105
        assert report.plantgen_counties_with_many_plants == 31
        assert report.plantgen_plants_alone_in_county == 105
        assert report.plantgen_plants_in_shared_counties == 92
        assert (
            report.plantgen_plants_alone_in_county + report.plantgen_plants_in_shared_counties
            == 197
        )


class TestKnownCases:
    def test_the_shoreline_plant_is_a_near_miss_not_a_wrong_county(
        self, enriched: pd.DataFrame
    ) -> None:
        """Indiana Harbor falls in Lake Michigan under the generalised boundary.

        It must come out as "outside, 255 m from Lake County IN", never silently snapped
        into a county it was not in.
        """
        row = enriched[enriched["plant_id"] == "P100000120926"].iloc[0]
        assert row["county_assignment_status"] == "outside_county_boundaries"
        assert pd.isna(row["county_fips"])
        assert row["nearest_county_fips"] == "18089"
        assert 200 < row["nearest_county_distance_m"] < 400

    def test_canadian_plants_are_far_outside_not_near_misses(self, enriched: pd.DataFrame) -> None:
        """The distance column separates a clipped shoreline from a different country."""
        canada = enriched[
            (enriched["country_area"] == "Canada")
            & (enriched["county_assignment_status"] == "outside_county_boundaries")
        ]
        assert len(canada) == 8
        assert canada["nearest_county_distance_m"].max() > 100_000

    def test_approximate_coordinates_are_country_level_placeholders(
        self, enriched: pd.DataFrame
    ) -> None:
        """The decisive finding: `approximate` does not mean "imprecise", it means "made up".

        "Nucor Steel Pacific Northwest", whose address says Pacific Northwest, carries
        37.0902, -95.7129 -- the conventional geographic centre of the contiguous United
        States -- and so resolves to Montgomery County, Kansas.
        """
        row = enriched[enriched["plant_id"] == "P100000121221"].iloc[0]
        assert row["coordinate_accuracy"] == "approximate"
        assert (round(row["latitude"], 4), round(row["longitude"], 4)) == (37.0902, -95.7129)
        assert row["county_state"] == "KS"
        assert not row["in_plantgen_county"]

        approximate = enriched[enriched["coordinate_accuracy"] == "approximate"]
        assert approximate["in_plantgen_county"].sum() == 0


class TestPersistence:
    def test_the_table_is_written_and_readable_through_duckdb(
        self, built, lake: tuple[Path, Path]
    ) -> None:
        from lakehouse.duck import connect, table_counts

        catalog_db, warehouse = lake
        assert table_counts(catalog_db, warehouse)["geo.gspt_plant_county"] == 93
        con = connect(catalog_db, warehouse)
        try:
            assert (
                con.execute(
                    "select count(*) from geo.gspt_plant_county where county_fips is not null"
                ).fetchone()[0]
                == 83
            )
        finally:
            con.close()

    def test_rebuilding_is_idempotent(self, counties, lake: tuple[Path, Path]) -> None:
        from lakehouse.duck import table_counts

        catalog_db, warehouse = lake
        build(db_path=catalog_db, warehouse=warehouse, write=True)
        assert table_counts(catalog_db, warehouse)["geo.gspt_plant_county"] == 93
