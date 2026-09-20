"""Fetch and cache the Census county boundary file.

[no DataFrame library] This module only moves bytes. See
docs/adr/ADR-002-dataframe-boundary.md.

WHICH FILE, AND WHAT IT COSTS
-----------------------------
The Census publishes county geography twice: the full TIGER/Line file (~80 MB,
survey-grade) and the cartographic boundary file at 1:500,000 (11.6 MB, generalised for
mapping). This project uses the 500k file.

The reasoning was that plants sit tens of kilometres from a county line, so a few
hundred metres of generalisation cannot change which county a plant is in.

**That reasoning is measurably wrong for one class of plant, and the exception is in
this data.** Cleveland-Cliffs Indiana Harbor (P100000120926) sits on the Lake Michigan
shore at 41.669553, -87.438429. The generalised Lake County, IN boundary cuts inland of
it, so the point falls in the lake: 255 m outside the nearest county polygon. Waterfront
and coastal plants -- and heavy industry is disproportionately waterfront -- are exactly
where generalisation bites.

The file is kept anyway, because the failure is visible rather than silent:
`geo.counties` records the nearest county and the distance to it for every point that
lands outside all of them, so a 255 m miss is reported as a 255 m miss instead of
becoming a wrong county. Whether to accept a near-miss as a match is a matching
decision, and it is left to the matching layer rather than hard-coded into the geometry
step. If that residual ever grows beyond a handful of plants, the full TIGER file is the
answer and only this module changes.

Only census.gov is contacted, and only when the cache is empty.
"""

from __future__ import annotations

import logging
import shutil
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

from lakehouse.paths import GEO_CACHE_DIR

logger = logging.getLogger(__name__)

CACHE_DIR = GEO_CACHE_DIR

COUNTY_VINTAGE = "cb_2023_us_county_500k"
COUNTY_URL = f"https://www2.census.gov/geo/tiger/GENZ2023/shp/{COUNTY_VINTAGE}.zip"
ALLOWED_HOST = "www2.census.gov"

DOWNLOAD_TIMEOUT_SECONDS = 120


class BoundaryFileUnavailableError(RuntimeError):
    """The county boundaries are neither cached nor reachable."""


def archive_path(cache_dir: Path | None = None) -> Path:
    return Path(cache_dir or CACHE_DIR) / f"{COUNTY_VINTAGE}.zip"


def shapefile_path(cache_dir: Path | None = None) -> Path:
    return Path(cache_dir or CACHE_DIR) / COUNTY_VINTAGE / f"{COUNTY_VINTAGE}.shp"


def is_cached(cache_dir: Path | None = None) -> bool:
    """True when the extracted shapefile is already on disk."""
    return shapefile_path(cache_dir).exists()


def _check_host(url: str) -> None:
    host = urllib.parse.urlsplit(url).hostname
    if host != ALLOWED_HOST:
        raise ValueError(f"refusing to download from {host!r}; only {ALLOWED_HOST} is allowed")


def _extract(archive: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as bundle:
        for member in bundle.namelist():
            # Reject absolute paths and traversal before writing anything.
            if member.startswith("/") or ".." in Path(member).parts:
                raise ValueError(f"refusing to extract unsafe archive member {member!r}")
        bundle.extractall(target_dir)


def ensure_county_boundaries(cache_dir: Path | None = None, allow_download: bool = True) -> Path:
    """Return the path to the county shapefile, downloading it only if not cached.

    Re-running never re-downloads: an existing extracted shapefile short-circuits
    immediately, and an existing archive is re-extracted rather than re-fetched. With
    `allow_download=False`, or with no network, a missing cache raises a message that
    says exactly what to do.
    """
    directory = Path(cache_dir or CACHE_DIR)
    shapefile = shapefile_path(directory)
    if shapefile.exists():
        logger.info("county boundaries already cached at %s", shapefile)
        return shapefile

    archive = archive_path(directory)
    if archive.exists():
        logger.info("extracting cached archive %s", archive)
        _extract(archive, directory / COUNTY_VINTAGE)
        if shapefile.exists():
            return shapefile

    if not allow_download:
        raise BoundaryFileUnavailableError(
            f"county boundaries are not cached at {shapefile} and downloading is disabled. "
            f"Run `make geo` with network access once, or place {COUNTY_VINTAGE}.zip in "
            f"{directory}."
        )

    _check_host(COUNTY_URL)
    directory.mkdir(parents=True, exist_ok=True)
    logger.info("downloading %s", COUNTY_URL)
    try:
        with urllib.request.urlopen(COUNTY_URL, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            partial = archive.with_suffix(".zip.part")
            with partial.open("wb") as handle:
                shutil.copyfileobj(response, handle)
            partial.replace(archive)
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        raise BoundaryFileUnavailableError(
            f"county boundaries are not cached at {shapefile} and {COUNTY_URL} could not be "
            f"reached ({error}). Connect to the network once, or place "
            f"{COUNTY_VINTAGE}.zip in {directory} by hand."
        ) from error

    _extract(archive, directory / COUNTY_VINTAGE)
    if not shapefile.exists():
        raise BoundaryFileUnavailableError(
            f"downloaded {archive} but it does not contain {shapefile.name}"
        )
    return shapefile
