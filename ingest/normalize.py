"""Column-name canonicalisation and value normalisation for the ingestion layer.

[Polars] Every helper here is either a pure Python string function or a Polars
expression. See docs/adr/ADR-002-dataframe-boundary.md.

Two behaviours in this module carry most of the correctness weight of P1:

1. `canonical_column_name` collapses the two column-name spellings that genuinely
   exist in the furnace CSVs (``Corrected C/L`` vs ``Corrected.C.L``) onto one
   canonical name, so a source that flips spelling is absorbed rather than
   producing a second column.
2. `zip5` / `pad_fips` repair identifiers that a numeric read would have already
   destroyed. PLANTGEN's own file has already lost the leading zero on four ZIP
   codes (it stores ``"6607"`` for Bridgeport CT), so padding is not defensive
   coding, it is a correction the data requires.
"""

from __future__ import annotations

import ast
import re

import polars as pl

_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")

# PLANTGEN uses tight CamelCase with embedded acronyms (StCntyFIPS, AISICode, MSA83).
# Generic camel-splitting guesses wrong on those, so the 19 columns are mapped by hand.
PLANTGEN_COLUMN_MAP: dict[str, str] = {
    "PlantID": "plant_id",
    "PlantName": "plant_name",
    "County": "county",
    "State": "state",
    "StCntyFIPS": "st_cnty_fips",
    "StateFIPS": "state_fips",
    "CntyFIPS": "cnty_fips",
    "MSA83": "msa83",
    "MSA93": "msa93",
    "PMSA83": "pmsa83",
    "PMSA93": "pmsa93",
    "PlantStartYr": "plant_start_yr",
    "PhoneNumber": "phone_number",
    "StreetAddress": "street_address",
    "City": "city",
    "ZipCode": "zip_code",
    "AISICode": "aisi_code",
    "ShutdownYr": "shutdown_yr",
    "SpecialtyGrade": "specialty_grade",
}


def canonical_column_name(name: str) -> str:
    """Fold a source column header onto its canonical snake_case name.

    Any run of non-alphanumeric characters becomes a single underscore, which is what
    makes the R-mangled and the original spellings land on the same name:

    >>> canonical_column_name("Corrected C/L") == canonical_column_name("Corrected.C.L")
    True
    >>> canonical_column_name("Power (kWh/ metric ton)")
    'power_kwh_metric_ton'
    >>> canonical_column_name("No..of.furnaces")
    'no_of_furnaces'
    """
    return _NON_ALNUM.sub("_", str(name)).strip("_").lower()


def canonicalize_columns(
    frame: pl.DataFrame, overrides: dict[str, str] | None = None
) -> pl.DataFrame:
    """Rename every column of `frame` to its canonical name.

    `overrides` wins over the generic rule. Raises if two source columns collapse onto
    the same canonical name, which would silently drop data.
    """
    overrides = overrides or {}
    mapping: dict[str, str] = {}
    seen: dict[str, str] = {}
    for col in frame.columns:
        target = overrides.get(col, canonical_column_name(col))
        if target in seen:
            raise ValueError(
                f"columns {seen[target]!r} and {col!r} both canonicalise to {target!r}"
            )
        seen[target] = col
        mapping[col] = target
    return frame.rename(mapping)


def clean_text(column: str) -> pl.Expr:
    """Trim whitespace and turn empty strings into true nulls."""
    stripped = pl.col(column).cast(pl.Utf8).str.strip_chars()
    return pl.when(stripped.str.len_chars() == 0).then(None).otherwise(stripped).alias(column)


def _drop_float_suffix(expr: pl.Expr) -> pl.Expr:
    """Strip a trailing ``.0`` left behind by an upstream float round-trip."""
    return expr.str.replace(r"\.0+$", "")


def zip5(column: str) -> pl.Expr:
    """Normalise a US ZIP code to five characters, zero-padded.

    Handles the three shapes this project actually sees: clean text (``"16003"``),
    a value that already lost its leading zero (``"6607"``), and a value that went
    through a float (``"601.0"``). ZIP+4 is truncated to the 5-digit prefix.
    Anything that is not digits after cleaning becomes null rather than a wrong code.
    """
    raw = _drop_float_suffix(pl.col(column).cast(pl.Utf8).str.strip_chars())
    digits = raw.str.extract(r"^(\d{1,9})", 1).str.slice(0, 5)
    return (
        pl.when(digits.is_null()).then(None).otherwise(digits.str.pad_start(5, "0")).alias(column)
    )


def pad_fips(column: str, width: int) -> pl.Expr:
    """Normalise a FIPS code to a fixed width, zero-padded, null when not numeric."""
    raw = _drop_float_suffix(pl.col(column).cast(pl.Utf8).str.strip_chars())
    digits = raw.str.extract(r"^(\d+)", 1)
    return (
        pl.when(digits.is_null())
        .then(None)
        .otherwise(digits.str.pad_start(width, "0"))
        .alias(column)
    )


def parse_alias_list(value: str | None) -> list[str]:
    """Parse a stringified Python list of plant aliases into a list of strings.

    The furnace CSVs store `Corrected C/L` as ``"['Owner City, State', ...]"``.
    `ast.literal_eval` is used rather than `eval` so the cell cannot execute code.
    A cell that is not a list literal is treated as a single alias, so no alias is
    ever lost to a parse failure.
    """
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return [text]
    if isinstance(parsed, (list, tuple)):
        return [str(item).strip() for item in parsed if str(item).strip()]
    return [str(parsed).strip()]


def alias_list_expr(column: str, alias: str) -> pl.Expr:
    """Apply `parse_alias_list` over a Polars string column."""
    return pl.col(column).map_elements(parse_alias_list, return_dtype=pl.List(pl.Utf8)).alias(alias)


def split_coordinates(column: str) -> list[pl.Expr]:
    """Split GSPT's ``"lat, lon"`` text column into two float columns.

    Returns nulls (not zeros) when the cell does not hold a usable pair.
    """
    raw = pl.col(column).cast(pl.Utf8).str.strip_chars()
    pattern = r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$"
    return [
        raw.str.extract(pattern, 1).cast(pl.Float64).alias("latitude"),
        raw.str.extract(pattern, 2).cast(pl.Float64).alias("longitude"),
    ]


def leading_int(column: str, alias: str) -> pl.Expr:
    """Extract the leading integer from an annotated count column.

    `No. of furnaces` is not a clean integer column. It carries annotations that the
    original compiler wrote into the cell: ``"1 (#5)"`` (which furnace), ``"1*"``
    (footnote), ``"1 (not melting)"`` and bare ``"Not melting"``. The count is
    recovered here; the verbatim cell is preserved alongside it so no annotation is
    lost, and `not_melting_flag` turns the most load-bearing annotation into a real
    boolean.
    """
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.extract(r"^\s*(\d+)", 1)
        .cast(pl.Int64, strict=False)
        .alias(alias)
    )


def not_melting_flag(column: str, alias: str) -> pl.Expr:
    """True when the count cell says the furnace was not melting that year."""
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.to_lowercase()
        .str.contains("not melting")
        .fill_null(False)
        .alias(alias)
    )
