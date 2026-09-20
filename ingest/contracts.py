"""Pandera contracts for the four bronze tables, plus dlt column hints derived from them.

[Polars] These are `pandera.polars` schemas, so the contract is checked on the Polars
frame *before* anything is handed to dlt. That ordering is the point: a missing
required column or a type that drifted stops the build at the extract step, before a
single row reaches the Iceberg table.

Division of labour with dlt's own schema contract:

* Pandera, here, guards the *input*: required columns, dtypes, key uniqueness,
  value ranges.
* dlt's `schema_contract` (see `ingest.sources`) guards the *stored table*: it
  freezes the column set and the column types of the Iceberg table so that a new or
  retyped column cannot quietly land in the lake.

`dlt_columns` translates a schema into dlt column hints so that both gates are
defined once, from the same place.
"""

from __future__ import annotations

from typing import Any

import pandera.polars as pa
import polars as pl

# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------

_DLT_TYPES: dict[Any, str] = {
    pl.Utf8: "text",
    pl.String: "text",
    pl.Int64: "bigint",
    pl.Float64: "double",
    pl.Boolean: "bool",
}


def _dlt_type(dtype: Any) -> str:
    if isinstance(dtype, pl.List):
        return "json"
    return _DLT_TYPES.get(dtype, "text")


def dlt_columns(schema: pa.DataFrameSchema) -> dict[str, dict[str, Any]]:
    """Derive dlt column hints from a Pandera schema.

    Declaring the column set up front is what gives dlt's ``"freeze"`` contract
    something to compare against, and it keeps the stored table's column order stable.

    Nullability is deliberately *not* hinted. Pandera already enforces it on the input,
    and the Arrow table carries the real nullability into Iceberg; adding a dlt hint on
    top only makes dlt log "column hints were different" on every single load, which
    trains the reader to ignore a warning that should mean something.
    """
    hints: dict[str, dict[str, Any]] = {}
    for name, column in schema.columns.items():
        hints[name] = {
            "data_type": _dlt_type(
                column.dtype.type if hasattr(column.dtype, "type") else column.dtype
            )
        }
    return hints


def _text(nullable: bool = True, **kwargs: Any) -> pa.Column:
    return pa.Column(pl.Utf8, nullable=nullable, **kwargs)


# --------------------------------------------------------------------------------------
# plantgen
# --------------------------------------------------------------------------------------

PLANTGEN_SCHEMA = pa.DataFrameSchema(
    name="plantgen",
    strict=True,
    unique=["plant_id"],
    columns={
        # PlantID is a perfect natural key: 197 values over 197 rows.
        "plant_id": _text(nullable=False),
        # PlantName matches City verbatim on ~85% of rows, so it behaves as a place
        # name rather than a plant name. Kept as-is here; the consequence for entity
        # resolution is handled downstream.
        "plant_name": _text(nullable=False),
        "county": _text(nullable=False),
        # Two-letter abbreviation, e.g. "PA". GSPT spells the same fact out in full.
        "state": _text(nullable=False),
        # Five-character state+county FIPS. The source file keeps its leading zero
        # ("09001"); a numeric read would destroy it.
        "st_cnty_fips": pa.Column(pl.Utf8, nullable=False, checks=pa.Check.str_matches(r"^\d{5}$")),
        "state_fips": pa.Column(pl.Utf8, nullable=False, checks=pa.Check.str_matches(r"^\d{2}$")),
        "cnty_fips": pa.Column(pl.Utf8, nullable=False, checks=pa.Check.str_matches(r"^\d{3}$")),
        "msa83": _text(),
        "msa93": _text(),
        "pmsa83": _text(),
        "pmsa93": _text(),
        "plant_start_yr": pa.Column(pl.Int64, nullable=True),
        "phone_number": _text(),
        "street_address": _text(),
        "city": _text(),
        # Zero-padded to five characters; null where the source has no ZIP at all.
        "zip_code": pa.Column(pl.Utf8, nullable=True, checks=pa.Check.str_matches(r"^\d{5}$")),
        "aisi_code": _text(),
        "shutdown_yr": pa.Column(pl.Int64, nullable=True),
        "specialty_grade": pa.Column(pl.Int64, nullable=True),
        "source_file": _text(nullable=False),
    },
)

# --------------------------------------------------------------------------------------
# gspt_plants
# --------------------------------------------------------------------------------------

# Every GSPT column stays text in bronze. The sheet uses a sentinel vocabulary inside
# otherwise-numeric columns ("unknown", ">0") and inside date columns ("2022-12",
# ">2019"), so coercing at ingest would collapse "reported as unknown" and "not
# reported" into the same null. The sentinels are resolved in the dbt staging layer,
# where the mapping is a documented, tested transformation. The two exceptions are
# `latitude`/`longitude`, derived from `coordinates` because the coordinate -> county
# FIPS join is the primary cross-source path and needs real floats.
_GSPT_TEXT_COLUMNS = (
    "plant_id",
    "plant_name_english",
    "plant_name_other_language",
    "other_plant_names_english",
    "other_plant_names_other_language",
    "owner",
    "owner_other_language",
    "owner_gem_id",
    "owner_permid",
    "soe_status",
    "parent",
    "parent_gem_id",
    "parent_perm_id",
    "location_address",
    "municipality",
    "subnational_unit_province_state",
    "country_area",
    "region",
    "other_language_location_address",
    "coordinates",
    "coordinate_accuracy",
    "gem_wiki_page",
    "capacity_operating_status",
    "plant_age_years",
    "announced_date",
    "construction_date",
    "start_date",
    "pre_retirement_announcement_date",
    "idled_date",
    "retired_date",
    "nominal_crude_steel_capacity_ttpa",
    "nominal_bof_steel_capacity_ttpa",
    "nominal_eaf_steel_capacity_ttpa",
    "nominal_ohf_steel_capacity_ttpa",
    "other_unspecified_steel_capacity_ttpa",
    "nominal_iron_capacity_ttpa",
    "nominal_bf_capacity_ttpa",
    "nominal_dri_capacity_ttpa",
    "other_unspecified_iron_capacity_ttpa",
    "ferronickel_capacity_ttpa",
    "sinter_plant_capacity_ttpa",
    "coking_plant_capacity_ttpa",
    "pelletizing_plant_capacity_ttpa",
    "category_steel_product",
    "steel_products",
    "steel_sector_end_users",
    "workforce_size",
    "iso_14001",
    "iso_50001",
    "responsiblesteel_certification",
    "main_production_process",
    "main_production_equipment",
    "detailed_production_equipment",
    "power_source",
    "iron_ore_source",
    "met_coal_source",
)

_NON_NULL_GSPT = {"plant_id", "plant_name_english", "country_area", "coordinates"}

GSPT_PLANTS_SCHEMA = pa.DataFrameSchema(
    name="gspt_plants",
    strict=True,
    # `plant_id` is deliberately NOT unique: 111 North American rows carry 93 ids.
    # The real grain is plant x production unit, so uniqueness is asserted on the
    # surrogate `source_row` instead.
    unique=["source_row"],
    columns={
        **{c: _text(nullable=c not in _NON_NULL_GSPT) for c in _GSPT_TEXT_COLUMNS},
        "latitude": pa.Column(pl.Float64, nullable=True, checks=pa.Check.in_range(-90, 90)),
        "longitude": pa.Column(pl.Float64, nullable=True, checks=pa.Check.in_range(-180, 180)),
        # False where `coordinates` is a sentinel rather than a pair. One North American
        # plant is in this state (P100000121251, "Electra iron plant", coordinates =
        # "unknown"), and `coordinate_accuracy` still reads "approximate" on it -- so
        # accuracy cannot stand in for presence. The flag is carried explicitly so that
        # a plant with no geographic evidence stays visible as such all the way to the
        # manual labelling queue, instead of being papered over with a fallback rule.
        "has_coordinates": pa.Column(pl.Boolean, nullable=False),
        "source_row": pa.Column(pl.Int64, nullable=False),
        "source_file": _text(nullable=False),
    },
)

# --------------------------------------------------------------------------------------
# gspt_production
# --------------------------------------------------------------------------------------

PRODUCTION_METRICS = ("crude_steel", "bof_steel", "eaf_steel", "ohf_steel", "iron", "bf", "dri")

GSPT_PRODUCTION_SCHEMA = pa.DataFrameSchema(
    name="gspt_production",
    strict=True,
    unique=["plant_id", "year", "metric"],
    columns={
        "plant_id": _text(nullable=False),
        "plant_name_english": _text(),
        "year": pa.Column(pl.Int64, nullable=False, checks=pa.Check.in_range(2019, 2022)),
        "metric": pa.Column(
            pl.Utf8, nullable=False, checks=pa.Check.isin(list(PRODUCTION_METRICS))
        ),
        # Null when the cell holds a sentinel such as "unknown"; `value_raw` keeps it.
        "value_ttpa": pa.Column(pl.Float64, nullable=True),
        "value_raw": _text(nullable=False),
        "source_file": _text(nullable=False),
    },
)

# --------------------------------------------------------------------------------------
# furnace_years
# --------------------------------------------------------------------------------------

FURNACE_YEARS_SCHEMA = pa.DataFrameSchema(
    name="furnace_years",
    strict=True,
    # `facility_id` is only unique WITHIN a furnace type: 18 ids appear in both the
    # BOF and the EAF file and point at different facilities (id 70 is Severstal
    # Dearborn in BOF and American Cast Iron Pipe Birmingham in EAF). `furnace_key`
    # is the surrogate that makes the id safe to join on.
    unique=["source_version", "furnace_type", "source_row"],
    columns={
        # Nullable because exactly one row in eaf_owner_filled.csv (a South American facility,
        # Montevideo) carries the R sentinel "NA" in ID, FID and Data_ID alike. The row
        # is kept rather than dropped -- bronze stays faithful -- but it can never be
        # joined, so downstream filters on `furnace_key is not null`.
        "furnace_key": pa.Column(
            pl.Utf8, nullable=True, checks=pa.Check.str_matches(r"^(BOF|EAF):\d+$")
        ),
        "furnace_type": pa.Column(pl.Utf8, nullable=False, checks=pa.Check.isin(["BOF", "EAF"])),
        "facility_id": _text(nullable=True),
        # Furnace number within the facility. Present only in the EAF files, which is
        # what makes (facility, year) non-unique there.
        "fid": _text(),
        "year": pa.Column(pl.Int64, nullable=False, checks=pa.Check.in_range(2012, 2023)),
        # Null throughout the gspt_2024 vintage; populated in owner_filled_2025.
        # `owner` is whitespace-trimmed; `owner_verbatim` is the cell exactly as the file
        # holds it. The two differ on 22 rows, where the same facility-year appears twice
        # with the owner spelled "AKSteelCorp." and "AKSteelCorp. ". Trimming is the right
        # default for comparison, but bronze claims to be faithful, and a difference that
        # only exists before trimming is invisible to anyone reading the trimmed column.
        "owner": _text(),
        "owner_verbatim": _text(),
        # Aliases parsed out of the stringified Python list in `Corrected C/L`,
        # exactly as this row's own file spells them.
        "corrected_cl": pa.Column(pl.List(pl.Utf8), nullable=False),
        # The same aliases taken from the gspt_2024 vintage when this row aligns to it.
        # That vintage carries no `owner` at all but its facility names are intact;
        # owner_filled_2025 carries every owner but went through a lossy encoding
        # conversion. The two are combined by alignment, never by string repair.
        # See docs/adr/ADR-003-encoding-degradation.md.
        "corrected_cl_clean": pa.Column(pl.List(pl.Utf8), nullable=False),
        "corrected_cl_name_version": pa.Column(
            pl.Utf8, nullable=False, checks=pa.Check.isin(["gspt_2024", "owner_filled_2025"])
        ),
        # Alignment key: "<type>:<id>:<year>:<fid or ->". Null when `facility_id` is
        # null, which is the only way a row can fail to align for want of a key.
        "align_key": _text(nullable=True),
        "alignment_status": pa.Column(
            pl.Utf8,
            nullable=False,
            checks=pa.Check.isin(["aligned", "unaligned_no_key", "unaligned_no_match"]),
        ),
        # True when this row's own name is a `?`-damaged copy of the aligned clean name.
        "encoding_degraded": pa.Column(pl.Boolean, nullable=False),
        # True when the damage also changed the string's length, i.e. characters were
        # swallowed and the original cannot be recovered from the damaged copy at all.
        "encoding_lossy": pa.Column(pl.Boolean, nullable=False),
        # `owner` exists only in the damaged vintage, so there is nothing to compare it
        # against. A bare "?" count is reported rather than a degradation verdict.
        "owner_has_replacement_char": pa.Column(pl.Boolean, nullable=False),
        "main_product_line": _text(),
        "year_list": pa.Column(pl.List(pl.Utf8), nullable=False),
        # `No. of furnaces` is an annotated column, not a clean integer: it holds
        # "1 (#5)", "1*", "1 (not melting)" and bare "Not melting". The parsed count,
        # the verbatim cell and the one annotation that changes the meaning of the
        # record are all kept.
        "no_of_furnaces": pa.Column(pl.Int64, nullable=True),
        "no_of_furnaces_raw": _text(),
        "not_melting": pa.Column(pl.Boolean, nullable=False),
        "power_kwh_per_metric_ton": pa.Column(pl.Float64, nullable=True),
        # Not a primary key: 187 distinct values over 1971 EAF rows.
        "data_id": _text(),
        "source_version": pa.Column(
            pl.Utf8, nullable=False, checks=pa.Check.isin(["gspt_2024", "owner_filled_2025"])
        ),
        "source_row": pa.Column(pl.Int64, nullable=False),
        "source_file": _text(nullable=False),
    },
)

# Columns that only exist once the two vintages have been aligned against each other.
# A single furnace file cannot produce them, so `read_furnace_file` stops short of these
# and `read_furnace_years` fills them in.
FURNACE_ALIGNMENT_COLUMNS: tuple[str, ...] = (
    "corrected_cl_clean",
    "corrected_cl_name_version",
    "alignment_status",
    "encoding_degraded",
    "encoding_lossy",
)

FURNACE_FILE_COLUMNS: tuple[str, ...] = tuple(
    c for c in FURNACE_YEARS_SCHEMA.columns if c not in FURNACE_ALIGNMENT_COLUMNS
)

SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "plantgen": PLANTGEN_SCHEMA,
    "gspt_plants": GSPT_PLANTS_SCHEMA,
    "gspt_production": GSPT_PRODUCTION_SCHEMA,
    "furnace_years": FURNACE_YEARS_SCHEMA,
}
