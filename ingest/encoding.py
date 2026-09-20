"""Detection of character-encoding degradation between two vintages of the same records.

[Polars] See docs/adr/ADR-003-encoding-degradation.md.

The `owner_filled_2025` furnace files went through a lossy encoding conversion: every
non-ASCII character in them was replaced by ``?``. The `gspt_2024` vintage of the same
records is intact. The two can therefore be *compared*, which is what this module does.

They must not be used to *repair* each other. Two failure modes occur in this data:

* ``Nucor Steel–Berkeley`` -> ``Nucor Steel?Berkeley`` — a 1:1 substitution of one
  character (U+2013 EN DASH) by one ``?``. Reversible in principle, by guessing.
* ``North American Höganäs`` -> ``North American H?an?`` — 42 characters become 40.
  Two multi-byte sequences were each swallowed *along with a following character*.
  Nothing in the damaged string says which characters were lost.

Because the second class is not reversible and is indistinguishable from the first
without the original, this module only ever *reports*. It never rewrites a string.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

logger = logging.getLogger(__name__)

REPLACEMENT_CHAR = "?"


def has_non_ascii(text: str | None) -> bool:
    """True when the string contains a character outside ASCII."""
    return bool(text) and any(ord(char) > 127 for char in text)


def is_degraded(original: str | None, candidate: str | None) -> bool:
    """True when `candidate` looks like an encoding-damaged copy of `original`.

    The test is deliberately narrow: the candidate must have gained a ``?`` that the
    original does not have, and the original must contain a non-ASCII character that
    could have produced it. A ``?`` that is simply part of the text is not a match.
    """
    if original is None or candidate is None:
        return False
    if REPLACEMENT_CHAR not in candidate or REPLACEMENT_CHAR in original:
        return False
    return has_non_ascii(original)


def is_lossy(original: str | None, candidate: str | None) -> bool:
    """True when the damage also changed the string's length.

    A 1:1 substitution keeps the length. A shorter candidate means characters were
    swallowed, and the original cannot be reconstructed from it at all.
    """
    if not is_degraded(original, candidate):
        return False
    return len(original or "") != len(candidate or "")


def _compare_lists(original: list[str] | None, candidate: list[str] | None) -> tuple[bool, bool]:
    """Compare two alias lists element-wise; returns ``(degraded, lossy)``."""
    if not original or not candidate or len(original) != len(candidate):
        return (False, False)
    degraded = any(is_degraded(o, c) for o, c in zip(original, candidate, strict=True))
    lossy = any(is_lossy(o, c) for o, c in zip(original, candidate, strict=True))
    return (degraded, lossy)


def degradation_flags(original_column: str, candidate_column: str) -> list[pl.Expr]:
    """Polars expressions flagging encoding degradation between two alias-list columns."""
    pair = pl.struct([pl.col(original_column), pl.col(candidate_column)])
    return [
        pair.map_elements(
            lambda row: _compare_lists(row[original_column], row[candidate_column])[0],
            return_dtype=pl.Boolean,
        )
        .fill_null(False)
        .alias("encoding_degraded"),
        pair.map_elements(
            lambda row: _compare_lists(row[original_column], row[candidate_column])[1],
            return_dtype=pl.Boolean,
        )
        .fill_null(False)
        .alias("encoding_lossy"),
    ]


def replacement_char_flag(column: str, alias: str) -> pl.Expr:
    """Flag a plain text column that contains a ``?``.

    Weaker evidence than `degradation_flags`, because there is no intact counterpart to
    compare against — the `Owner` column exists only in the damaged vintage. It is
    reported, never acted on.
    """
    return (
        pl.col(column).cast(pl.Utf8).str.contains(REPLACEMENT_CHAR, literal=True).fill_null(False)
    ).alias(alias)


@dataclass(frozen=True)
class EncodingReport:
    """What the contract check found. Counts, not corrections."""

    rows_checked: int
    degraded_rows: int
    lossy_rows: int
    degraded_facilities: int
    owner_replacement_rows: int

    @property
    def clean(self) -> bool:
        return self.degraded_rows == 0 and self.owner_replacement_rows == 0

    def as_dict(self) -> dict[str, int]:
        return {
            "rows_checked": self.rows_checked,
            "degraded_rows": self.degraded_rows,
            "lossy_rows": self.lossy_rows,
            "degraded_facilities": self.degraded_facilities,
            "owner_replacement_rows": self.owner_replacement_rows,
        }


def audit(frame: pl.DataFrame) -> EncodingReport:
    """Count encoding damage on an aligned furnace frame and log a warning if any.

    A warning, not an error. The damaged vintage is the only one that carries `owner`,
    so refusing to load it would cost more than it saves. The build continues; the
    counts are surfaced so a regression is visible.
    """
    degraded = frame.filter(pl.col("encoding_degraded"))
    report = EncodingReport(
        rows_checked=frame.height,
        degraded_rows=degraded.height,
        lossy_rows=frame.filter(pl.col("encoding_lossy")).height,
        degraded_facilities=degraded.select("furnace_type", "facility_id").unique().height,
        owner_replacement_rows=frame.filter(pl.col("owner_has_replacement_char")).height,
    )
    if not report.clean:
        logger.warning(
            "encoding degradation detected in furnace_years: "
            "%d/%d rows across %d facilities carry a name damaged relative to the "
            "gspt_2024 vintage (%d of them irreversibly, the string changed length); "
            "%d rows carry a '?' in `owner`, which has no intact counterpart. "
            "Damaged values are kept verbatim and are NOT repaired.",
            report.degraded_rows,
            report.rows_checked,
            report.degraded_facilities,
            report.lossy_rows,
            report.owner_replacement_rows,
        )
    return report
