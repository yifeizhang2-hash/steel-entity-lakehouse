"""dlt resources for the four bronze tables.

[Polars] Every reader in this module returns a `polars.DataFrame`, validates it
against its Pandera contract, and then hands dlt a **pyarrow table** — not a
DataFrame. Nothing in `geo/`, `resolution/` or `transform/` may import from here;
those layers read the Iceberg tables back out through DuckDB.
See docs/adr/ADR-002-dataframe-boundary.md.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dlt
import pandera.polars as pa
import polars as pl
import pyarrow as pa_arrow

from ingest import encoding as enc
from ingest import normalize as nz
from ingest.config import (
    FURNACE_FILES,
    GSPT_XLSX,
    PLANTGEN_CSV,
    SHEET_STEEL_PLANTS,
)
from ingest.contracts import (
    FURNACE_FILE_COLUMNS,
    FURNACE_YEARS_SCHEMA,
    GSPT_PLANTS_SCHEMA,
    GSPT_PRODUCTION_SCHEMA,
    PLANTGEN_SCHEMA,
    dlt_columns,
)
from ingest.excel import read_yearly_production_long

# dlt guards the *stored* Iceberg table: "freeze" on columns and data types means a
# source that grows a column, or retypes one, stops the load instead of mutating the
# table underneath the models that read it.
#
# Measured limitation (dlt 1.30, verified in tests/test_schema_evolution.py): the
# contract is evaluated against the *persisted* schema, so on the first load of a table
# an undeclared column slips through even though the resource declares its columns up
# front. It is rejected from the second load onwards. The first line of defence is
# therefore Pandera `strict=True` plus the explicit `_project(...)` projection that every
# reader below ends with; the contract is the second.
SCHEMA_CONTRACT = {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}


# --------------------------------------------------------------------------------------
# shared readers
# --------------------------------------------------------------------------------------


def _csv_schema_overrides(path: Path, dtype_by_canonical: dict[str, Any]) -> dict[str, Any]:
    """Build Polars `schema_overrides` keyed by the header spelling this file uses.

    The furnace CSVs ship under two different column-name spellings (``Corrected C/L``
    vs ``Corrected.C.L``), so the overrides cannot be hard-coded against one spelling.
    They are resolved through the canonical name instead, which makes the type
    declaration survive the rename.
    """
    # `infer_schema_length=0` keeps this probe from trying to type the file; all it
    # needs is the header spelling.
    header = pl.read_csv(path, n_rows=0, infer_schema_length=0).columns
    overrides: dict[str, Any] = {}
    for original in header:
        canonical = nz.canonical_column_name(original)
        if canonical in dtype_by_canonical:
            overrides[original] = dtype_by_canonical[canonical]
    return overrides


class MissingSourceColumnError(KeyError):
    """A source no longer carries a column the contract requires."""


def _require(frame: pl.DataFrame, required: tuple[str, ...], source: str) -> pl.DataFrame:
    """Fail fast if the source is missing a column, before any transformation runs.

    Called immediately after column canonicalisation, so `required` is expressed in
    canonical names and is therefore insensitive to which spelling the source used.
    Checking here rather than at the final projection means the error names the source
    and every missing column at once, instead of surfacing as Polars' generic
    "unable to find column" from whichever expression happened to touch one first.
    """
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise MissingSourceColumnError(
            f"{source} is missing required column(s) {missing}; got {sorted(frame.columns)}"
        )
    return frame


def _project(frame: pl.DataFrame, schema: pa.DataFrameSchema) -> pl.DataFrame:
    """Select exactly the contract's columns, in order.

    Checked explicitly first so that a source which dropped a column fails with a
    message naming the contract and every column that went missing, rather than with
    Polars' generic "unable to find column" on whichever one it hit first.
    """
    missing = [column for column in schema.columns if column not in frame.columns]
    if missing:
        raise MissingSourceColumnError(
            f"contract {schema.name!r} requires {missing}, which the source no longer provides; "
            f"got {sorted(frame.columns)}"
        )
    return frame.select(list(schema.columns))


def _validate(frame: pl.DataFrame, schema: pa.DataFrameSchema) -> pl.DataFrame:
    """Project onto the contract, run it, and return the frame.

    Any contract failure raises here, before dlt is given anything to load.
    """
    projected = _project(frame, schema)
    schema.validate(projected, lazy=False)
    return projected


def _arrow(frame: pl.DataFrame) -> pa_arrow.Table:
    """Hand off to dlt as Arrow, so identifier types survive the loader."""
    return frame.to_arrow()


# --------------------------------------------------------------------------------------
# plantgen
# --------------------------------------------------------------------------------------

# Identifier columns are declared Utf8 so the CSV text is never routed through a float.
# Without this, pandas and Polars both infer ZipCode as float64 and StCntyFIPS as int64,
# which turns "09001" into 9001.
PLANTGEN_OVERRIDES: dict[str, Any] = {
    "PlantID": pl.Utf8,
    "ZipCode": pl.Utf8,
    "StCntyFIPS": pl.Utf8,
    "StateFIPS": pl.Utf8,
    "CntyFIPS": pl.Utf8,
    "MSA83": pl.Utf8,
    "MSA93": pl.Utf8,
    "PMSA83": pl.Utf8,
    "PMSA93": pl.Utf8,
    "AISICode": pl.Utf8,
    "PhoneNumber": pl.Utf8,
    "PlantName": pl.Utf8,
    "County": pl.Utf8,
    "State": pl.Utf8,
    "StreetAddress": pl.Utf8,
    "City": pl.Utf8,
    # Genuine measures stay numeric; low-coverage ones must stay *null*, not NaN.
    "PlantStartYr": pl.Int64,
    "ShutdownYr": pl.Int64,
    "SpecialtyGrade": pl.Int64,
}

# Every column the contract needs, minus the ones this module derives (`source_file`).
PLANTGEN_REQUIRED: tuple[str, ...] = tuple(
    c for c in PLANTGEN_SCHEMA.columns if c not in {"source_file"}
)

# Derived here: latitude/longitude from `coordinates`, plus the bookkeeping columns.
GSPT_PLANTS_REQUIRED: tuple[str, ...] = tuple(
    c
    for c in GSPT_PLANTS_SCHEMA.columns
    if c not in {"latitude", "longitude", "has_coordinates", "source_row", "source_file"}
)

# The furnace files differ in which optional columns they carry (BOF has no FID and no
# power reading), so only the genuinely universal ones are required.
FURNACE_REQUIRED: tuple[str, ...] = ("year", "id", "corrected_c_l", "year_list")

_PLANTGEN_TEXT = (
    "plant_name",
    "county",
    "state",
    "msa83",
    "msa93",
    "pmsa83",
    "pmsa93",
    "phone_number",
    "street_address",
    "city",
    "aisi_code",
    "plant_id",
)


def read_plantgen(path: Path | None = None) -> pl.DataFrame:
    """Read PLANTGEN.csv into the canonical bronze shape."""
    csv_path = Path(path) if path is not None else PLANTGEN_CSV
    frame = pl.read_csv(csv_path, schema_overrides=PLANTGEN_OVERRIDES, null_values=[""])
    frame = nz.canonicalize_columns(frame, overrides=nz.PLANTGEN_COLUMN_MAP)
    frame = _require(frame, PLANTGEN_REQUIRED, csv_path.name)
    frame = frame.with_columns(
        [nz.clean_text(c) for c in _PLANTGEN_TEXT if c in frame.columns]
        + [
            nz.zip5("zip_code"),
            nz.pad_fips("st_cnty_fips", 5),
            nz.pad_fips("state_fips", 2),
            nz.pad_fips("cnty_fips", 3),
        ]
    ).with_columns(pl.lit(csv_path.name).alias("source_file"))
    return _validate(frame, PLANTGEN_SCHEMA)


# --------------------------------------------------------------------------------------
# gspt_plants
# --------------------------------------------------------------------------------------


def read_gspt_plants(path: Path | None = None) -> pl.DataFrame:
    """Read the `Steel Plants` sheet.

    Read with `infer_schema_length=0`, i.e. every cell as text. This is deliberate:
    the sheet mixes numbers with a sentinel vocabulary ("unknown", ">0") inside the
    same column, so inference would either fail or silently null out the sentinels and
    lose the distinction between "reported as unknown" and "not reported". The one
    derivation applied here is splitting `Coordinates` into real floats, because the
    coordinate -> county FIPS path is the primary cross-source join.

    The whole sheet is loaded, not just North America. Bronze stays faithful to the
    source; the 111-row US/Canada subset is a downstream filter on `country_area`.
    """
    xlsx_path = Path(path) if path is not None else GSPT_XLSX
    frame = pl.read_excel(xlsx_path, sheet_name=SHEET_STEEL_PLANTS, infer_schema_length=0)
    frame = nz.canonicalize_columns(frame)
    frame = _require(frame, GSPT_PLANTS_REQUIRED, f"{xlsx_path.name}:{SHEET_STEEL_PLANTS}")
    frame = (
        frame.with_columns([nz.clean_text(c) for c in frame.columns])
        .with_columns(nz.split_coordinates("coordinates"))
        .with_columns(pl.col("latitude").is_not_null().alias("has_coordinates"))
        .with_row_index("source_row", offset=1)
        .with_columns(
            pl.col("source_row").cast(pl.Int64),
            pl.lit(xlsx_path.name).alias("source_file"),
        )
    )
    return _validate(frame, GSPT_PLANTS_SCHEMA)


# --------------------------------------------------------------------------------------
# gspt_production
# --------------------------------------------------------------------------------------


def read_gspt_production(path: Path | None = None) -> pl.DataFrame:
    """Unpivot the `Yearly Production` sheet into a long table.

    The two-row-header parse itself lives in `ingest.excel` (the single documented
    pandas exception). It returns plain dicts; the frame is rebuilt in Polars here so
    that the contract check and the dlt hand-off stay on the Polars side.
    """
    xlsx_path = Path(path) if path is not None else GSPT_XLSX
    records = read_yearly_production_long(xlsx_path)
    frame = pl.DataFrame(
        records,
        schema={
            "plant_id": pl.Utf8,
            "plant_name_english": pl.Utf8,
            "year": pl.Int64,
            "metric": pl.Utf8,
            "value_ttpa": pl.Float64,
            "value_raw": pl.Utf8,
        },
        orient="row" if not records else None,
    ).with_columns(pl.lit(xlsx_path.name).alias("source_file"))
    return _validate(frame, GSPT_PRODUCTION_SCHEMA)


# --------------------------------------------------------------------------------------
# furnace_years
# --------------------------------------------------------------------------------------

# Keyed by canonical name, so one declaration covers both column-name spellings.
# `no_of_furnaces` is declared Utf8 on purpose: the source cell is annotated text
# ("1 (#5)", "Not melting"), so the integer is derived from it rather than cast.
FURNACE_OVERRIDES: dict[str, Any] = {
    "id": pl.Utf8,
    "fid": pl.Utf8,
    "data_id": pl.Utf8,
    "owner": pl.Utf8,
    "corrected_c_l": pl.Utf8,
    "main_product_line": pl.Utf8,
    "year_list": pl.Utf8,
    "year": pl.Int64,
    "no_of_furnaces": pl.Utf8,
    "power_kwh_metric_ton": pl.Float64,
}

# These files were written by R, which spells a missing value "NA". Left unlisted it
# would become the literal string "NA" in ID, FID, Data_ID and Power.
FURNACE_NULL_VALUES = ["", "NA", "N/A"]

_FURNACE_OPTIONAL_TEXT = ("fid", "data_id", "main_product_line", "owner")


def read_furnace_file(furnace_type: str, source_version: str, path: Path) -> pl.DataFrame:
    """Read one BOF or EAF CSV into the shared canonical shape.

    `furnace_key` is built as ``"<type>:<id>"`` because `ID` is only unique within a
    furnace type: 18 ids appear in both files and denote different facilities.
    """
    overrides = _csv_schema_overrides(path, FURNACE_OVERRIDES)
    frame = pl.read_csv(path, schema_overrides=overrides, null_values=FURNACE_NULL_VALUES)
    frame = nz.canonicalize_columns(frame)
    frame = _require(frame, FURNACE_REQUIRED, path.name)

    # Columns that a given vintage simply does not carry (BOF has no FID, no power).
    for missing, dtype in (
        ("fid", pl.Utf8),
        ("data_id", pl.Utf8),
        ("main_product_line", pl.Utf8),
        ("no_of_furnaces", pl.Utf8),
        ("power_kwh_metric_ton", pl.Float64),
    ):
        if missing not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(missing))

    frame = (
        # Captured before `clean_text` trims it. See the contract for why.
        frame.with_columns(pl.col("owner").cast(pl.Utf8).alias("owner_verbatim"))
        .with_columns([nz.clean_text(c) for c in _FURNACE_OPTIONAL_TEXT])
        .with_columns(
            nz.alias_list_expr("corrected_c_l", "corrected_cl"),
            nz.alias_list_expr("year_list", "year_list_parsed"),
            nz.leading_int("no_of_furnaces", "no_of_furnaces_int"),
            nz.not_melting_flag("no_of_furnaces", "not_melting"),
            pl.col("no_of_furnaces").alias("no_of_furnaces_raw"),
            pl.col("id").cast(pl.Utf8).str.strip_chars().alias("facility_id"),
            pl.lit(furnace_type).alias("furnace_type"),
            pl.lit(source_version).alias("source_version"),
            pl.lit(path.name).alias("source_file"),
            pl.col("power_kwh_metric_ton").alias("power_kwh_per_metric_ton"),
        )
        .with_columns(
            (pl.lit(furnace_type) + pl.lit(":") + pl.col("facility_id")).alias("furnace_key"),
            pl.col("year_list_parsed").alias("year_list"),
            pl.col("no_of_furnaces_int").alias("no_of_furnaces"),
            enc.replacement_char_flag("owner", "owner_has_replacement_char"),
            # The key the two vintages align on. BOF files have no FID at all, so it is
            # folded in as "-" rather than left null, which keeps one code path for both
            # furnace types. A null `facility_id` propagates to a null key: those rows
            # can never align, and that is the intended outcome, not a failure.
            pl.when(pl.col("facility_id").is_null())
            .then(None)
            .otherwise(
                pl.lit(f"{furnace_type}:")
                + pl.col("facility_id")
                + pl.lit(":")
                + pl.col("year").cast(pl.Utf8)
                + pl.lit(":")
                + pl.col("fid").fill_null("-")
            )
            .alias("align_key"),
        )
        .with_row_index("source_row", offset=1)
        .with_columns(pl.col("source_row").cast(pl.Int64))
    )
    missing = [c for c in FURNACE_FILE_COLUMNS if c not in frame.columns]
    if missing:
        raise MissingSourceColumnError(f"{path.name} did not produce {missing}")
    return frame.select(list(FURNACE_FILE_COLUMNS))


# The vintage whose facility names are intact. It carries no `owner` at all, which is
# exactly why the other vintage cannot simply be discarded.
CLEAN_NAME_VERSION = "gspt_2024"


def align_vintages(frame: pl.DataFrame) -> pl.DataFrame:
    """Attach each row's clean facility name from the intact vintage.

    The two vintages of the furnace census are complementary and each is unusable
    alone: `gspt_2024` has intact facility names but no owners, `owner_filled_2025` has
    every owner but had its non-ASCII characters destroyed. They are combined by joining
    on `align_key`, never by editing strings -- see
    docs/adr/ADR-003-encoding-degradation.md for why repair is not possible here.

    Every row keeps its own `corrected_cl` verbatim. `corrected_cl_clean` is the intact
    name where one could be found, and `alignment_status` says which of the three cases
    a row fell into, so the join is auditable from the table itself.
    """
    clean = (
        frame.filter(
            (pl.col("source_version") == CLEAN_NAME_VERSION) & pl.col("align_key").is_not_null()
        )
        .select("align_key", pl.col("corrected_cl").alias("_clean_cl"))
        .unique(subset="align_key", keep="first")
    )
    joined = frame.join(clean, on="align_key", how="left")
    if joined.height != frame.height:
        raise ValueError(
            f"vintage alignment changed the row count ({frame.height} -> {joined.height}); "
            "the clean-name lookup is not unique on align_key"
        )
    return (
        joined.with_columns(
            pl.when(pl.col("_clean_cl").is_not_null())
            .then(pl.col("_clean_cl"))
            .otherwise(pl.col("corrected_cl"))
            .alias("corrected_cl_clean"),
            pl.when(pl.col("_clean_cl").is_not_null())
            .then(pl.lit(CLEAN_NAME_VERSION))
            .otherwise(pl.col("source_version"))
            .alias("corrected_cl_name_version"),
            pl.when(pl.col("_clean_cl").is_not_null())
            .then(pl.lit("aligned"))
            .when(pl.col("align_key").is_null())
            .then(pl.lit("unaligned_no_key"))
            .otherwise(pl.lit("unaligned_no_match"))
            .alias("alignment_status"),
        )
        .with_columns(enc.degradation_flags("_clean_cl", "corrected_cl"))
        .drop("_clean_cl")
    )


def read_furnace_years(
    files: tuple[tuple[str, str, Path], ...] | None = None,
) -> pl.DataFrame:
    """Union all furnace CSVs, then align the two vintages against each other.

    Both vintages are kept and distinguished by `source_version`, so bronze stays
    faithful to what each file says. On top of that, every row gains the intact facility
    name from `gspt_2024` and a record of how it got there. A consumer wanting the
    usable combination filters `source_version = 'owner_filled_2025'` and reads `owner`
    together with `corrected_cl_clean`.
    """
    specs = files if files is not None else FURNACE_FILES
    frames = [read_furnace_file(t, v, Path(p)) for t, v, p in specs]
    frame = align_vintages(pl.concat(frames, how="vertical"))
    validated = _validate(frame, FURNACE_YEARS_SCHEMA)
    enc.audit(validated)
    return validated


# --------------------------------------------------------------------------------------
# dlt resources
# --------------------------------------------------------------------------------------


# `plant_id` is the only genuine natural key in the project (197 values over 197 rows),
# so it is declared as the primary key. dlt logs one INFO line per load saying it kept
# the resulting `nullable=False` hint over Arrow's `nullable=True`; that is the intended
# outcome, not a problem. The other three tables have no natural key and declare none.
@dlt.resource(
    name="plantgen",
    write_disposition="replace",
    table_format="iceberg",
    columns=dlt_columns(PLANTGEN_SCHEMA),
    schema_contract=SCHEMA_CONTRACT,
    primary_key="plant_id",
)
def plantgen_resource(path: Path | None = None):
    yield _arrow(read_plantgen(path))


@dlt.resource(
    name="gspt_plants",
    write_disposition="replace",
    table_format="iceberg",
    columns=dlt_columns(GSPT_PLANTS_SCHEMA),
    schema_contract=SCHEMA_CONTRACT,
)
def gspt_plants_resource(path: Path | None = None):
    yield _arrow(read_gspt_plants(path))


@dlt.resource(
    name="gspt_production",
    write_disposition="replace",
    table_format="iceberg",
    columns=dlt_columns(GSPT_PRODUCTION_SCHEMA),
    schema_contract=SCHEMA_CONTRACT,
)
def gspt_production_resource(path: Path | None = None):
    yield _arrow(read_gspt_production(path))


@dlt.resource(
    name="furnace_years",
    write_disposition="replace",
    table_format="iceberg",
    columns=dlt_columns(FURNACE_YEARS_SCHEMA),
    schema_contract=SCHEMA_CONTRACT,
)
def furnace_years_resource(files: tuple[tuple[str, str, Path], ...] | None = None):
    yield _arrow(read_furnace_years(files))


@dlt.source(name="steel_bronze")
def steel_bronze_source():
    """All four bronze resources."""
    return [
        plantgen_resource(),
        gspt_plants_resource(),
        gspt_production_resource(),
        furnace_years_resource(),
    ]


READERS = {
    "plantgen": read_plantgen,
    "gspt_plants": read_gspt_plants,
    "gspt_production": read_gspt_production,
    "furnace_years": read_furnace_years,
}
