"""The one documented pandas exception inside `ingest/`.

WHY PANDAS HERE, AND NOWHERE ELSE IN `ingest/`
----------------------------------------------
The GSPT workbook's `Yearly Production` sheet is a pivot table with a *two-row*
header. Row 0 is a merged-cell year banner (``2019`` appears once and spans the seven
metric columns beneath it); row 1 carries the full per-column label
(``"Crude steel production 2019 (ttpa)"``). Polars' Excel reader exposes a single
header row, so reconstructing the merged banner means reading the sheet header-less
and forward-filling row 0 — a positional-index operation that pandas expresses
directly and Polars does not.

Using the banner is not cosmetic. The year appears twice, once in row 0 and once
inside the row-1 label, so the two can be cross-checked against each other; a shifted
or mis-merged banner is caught here instead of silently mis-dating production.

The output of this module is a *long* pandas DataFrame that is immediately converted
to a list of plain dicts by `ingest.sources`. No pandas object crosses the module
boundary. See docs/adr/ADR-002-dataframe-boundary.md.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from ingest.config import GSPT_XLSX, SHEET_YEARLY_PRODUCTION

# "Crude steel production 2019 (ttpa)" -> metric="Crude steel", year=2019
_LABEL = re.compile(r"^(?P<metric>.+?)\s+production\s+(?P<year>\d{4})\s*\(ttpa\)$", re.IGNORECASE)

_METRIC_CODES = {
    "crude steel": "crude_steel",
    "bof steel": "bof_steel",
    "eaf steel": "eaf_steel",
    "ohf steel": "ohf_steel",
    "iron": "iron",
    "bf": "bf",
    "dri": "dri",
}

ID_COLUMNS = ("Plant ID", "Plant name (English)")


class YearlyProductionHeaderError(ValueError):
    """Raised when the two header rows of `Yearly Production` disagree."""


def _parse_header(banner: pd.Series[Any], labels: pd.Series[Any]) -> list[tuple[int, int, str]]:
    """Return ``(column_index, year, metric_code)`` for every metric column.

    `banner` is header row 0 (merged year cells, forward-filled here); `labels` is
    header row 1. The year is read from both and the two must agree.
    """
    filled = banner.ffill()
    parsed: list[tuple[int, int, str]] = []
    for position, label in enumerate(labels):
        if not isinstance(label, str):
            continue
        if label in ID_COLUMNS:
            continue
        match = _LABEL.match(label.strip())
        if match is None:
            raise YearlyProductionHeaderError(f"unrecognised production column label: {label!r}")
        year = int(match.group("year"))
        metric_name = match.group("metric").strip().lower()
        if metric_name not in _METRIC_CODES:
            raise YearlyProductionHeaderError(f"unknown production metric: {metric_name!r}")
        banner_year = filled.iloc[position]
        if pd.isna(banner_year):
            raise YearlyProductionHeaderError(f"column {position} has no year banner above it")
        if int(banner_year) != year:
            raise YearlyProductionHeaderError(
                f"column {position} ({label!r}) sits under year banner {int(banner_year)}"
            )
        parsed.append((position, year, _METRIC_CODES[metric_name]))
    return parsed


def _to_float(value: Any) -> float | None:
    """Coerce a cell to a float, mapping GSPT's sentinel vocabulary to null.

    The sheet uses ``unknown`` and ``>0`` where a number is not available. Those are
    kept verbatim in `value_raw`; only a genuine number reaches `value_ttpa`.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(str(value).strip().replace(",", ""))
    except ValueError:
        return None


def read_yearly_production_long(xlsx_path: Path | None = None) -> list[dict[str, Any]]:
    """Read and unpivot `Yearly Production` into ``(plant, year, metric, value)`` records.

    Cells that are entirely empty are dropped; cells holding a sentinel such as
    ``unknown`` are kept, with `value_raw` set and `value_ttpa` null, because
    "reported as unknown" and "not reported at all" are different facts.
    """
    path = Path(xlsx_path) if xlsx_path is not None else GSPT_XLSX
    sheet = pd.read_excel(path, sheet_name=SHEET_YEARLY_PRODUCTION, header=None)
    if len(sheet) < 3:
        raise YearlyProductionHeaderError("sheet has no data rows below its two header rows")

    banner, labels = sheet.iloc[0], sheet.iloc[1]
    label_list = [str(x) if isinstance(x, str) else x for x in labels]
    for required in ID_COLUMNS:
        if required not in label_list:
            raise YearlyProductionHeaderError(f"missing required column {required!r}")
    plant_id_at = label_list.index("Plant ID")
    plant_name_at = label_list.index("Plant name (English)")

    metric_columns = _parse_header(banner, labels)
    body = sheet.iloc[2:].reset_index(drop=True)

    records: list[dict[str, Any]] = []
    for row in body.itertuples(index=False, name=None):
        plant_id = row[plant_id_at]
        if plant_id is None or (isinstance(plant_id, float) and pd.isna(plant_id)):
            continue
        plant_name = row[plant_name_at]
        for position, year, metric in metric_columns:
            cell = row[position]
            if cell is None or (isinstance(cell, float) and pd.isna(cell)):
                continue
            records.append(
                {
                    "plant_id": str(plant_id).strip(),
                    "plant_name_english": None
                    if plant_name is None or (isinstance(plant_name, float) and pd.isna(plant_name))
                    else str(plant_name).strip(),
                    "year": year,
                    "metric": metric,
                    "value_ttpa": _to_float(cell),
                    "value_raw": str(cell).strip(),
                }
            )
    return records
