"""Normalisation needed before the two sources can be compared at all.

[pandas] Reads nothing; these are pure functions over strings. Nothing here imports
Polars or `ingest`. See docs/adr/ADR-002-dataframe-boundary.md.

The three problems this module exists for, each measured on the real sources:

1. **States do not intersect at all.** GSPT writes ``Alabama``, PLANTGEN writes ``AL``.
   The raw intersection of the two columns is *empty*, so without folding them to one
   form, state blocking would produce zero candidate pairs.
2. **GSPT has no ZIP column.** The ZIP is inside `Location address` free text, and it is
   the single most discriminating field the two sources share.
3. **Street addresses are written differently** (``Rd`` vs ``Road``, ``S`` vs
   ``South``), so an exact comparison on the raw string is close to useless.
"""

from __future__ import annotations

import re

US_STATES: dict[str, str] = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "puerto rico": "PR",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}

CA_PROVINCES: dict[str, str] = {
    "alberta": "AB",
    "british columbia": "BC",
    "manitoba": "MB",
    "new brunswick": "NB",
    "newfoundland and labrador": "NL",
    "nova scotia": "NS",
    "ontario": "ON",
    "prince edward island": "PE",
    "quebec": "QC",
    "québec": "QC",
    "saskatchewan": "SK",
    "northwest territories": "NT",
    "nunavut": "NU",
    "yukon": "YT",
}

SUBDIVISIONS = {**US_STATES, **CA_PROVINCES}
ABBREVIATIONS = set(SUBDIVISIONS.values())

# Values that mean "not recorded" across these sources. GSPT writes them into the field
# itself rather than leaving it empty.
# "nan" is included because a null arriving from a pandas float column stringifies to
# it, and a literal "nan" is never a real value in these sources.
SENTINELS = {"", "unknown", "n/a", "na", "none", "null", "nan", "-"}

# Years outside this range are not years. PLANTGEN writes 9999 for "unknown" in
# `PlantStartYr` (8 of its 89 populated values) and in the MSA/PMSA codes; its real
# start years run 1879-2002.
MIN_YEAR, MAX_YEAR = 1700, 2030

_ZIP = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
_WHITESPACE = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\s]")

# Street-type and directional abbreviations, folded so that "6500 S Boundary Rd" and
# "6500 South Boundary Road" compare as the same string.
_STREET_TOKENS: dict[str, str] = {
    "street": "st",
    "str": "st",
    "road": "rd",
    "avenue": "ave",
    "av": "ave",
    "boulevard": "blvd",
    "drive": "dr",
    "highway": "hwy",
    "parkway": "pkwy",
    "lane": "ln",
    "route": "rt",
    "rte": "rt",
    "north": "n",
    "south": "s",
    "east": "e",
    "west": "w",
    "northeast": "ne",
    "northwest": "nw",
    "southeast": "se",
    "southwest": "sw",
    "post office": "po",
}


def is_sentinel(value: str | None) -> bool:
    """True when the value is absent, or is a source's way of writing "absent".

    A float NaN counts as absent. It reaches here whenever a nullable column comes back
    from DuckDB as a float column, and `str(nan)` is `"nan"`, which would otherwise be
    treated as a real value and silently join rows that share nothing.
    """
    if value is None:
        return True
    if isinstance(value, float) and value != value:
        return True
    return str(value).strip().lower() in SENTINELS


def clean(value: str | None) -> str | None:
    """Trim, collapse whitespace, and map a sentinel to None."""
    if value is None:
        return None
    text = _WHITESPACE.sub(" ", str(value)).strip()
    return None if is_sentinel(text) else text


def normalize_subdivision(value: str | None) -> str | None:
    """Fold a state or province to its two-letter abbreviation.

    ``"Alabama"`` and ``"AL"`` both become ``"AL"``. This is what makes the two sources
    share a state at all: compared raw, their state columns have an empty intersection.
    """
    text = clean(value)
    if text is None:
        return None
    upper = text.upper()
    if upper in ABBREVIATIONS:
        return upper
    return SUBDIVISIONS.get(text.lower())


def extract_zip(address: str | None) -> str | None:
    """Pull a 5-digit ZIP out of a free-text address.

    GSPT writes ``"1 Steel Dr., Calvert, Alabama, 36513-1300, United States"``. The last
    5-digit run is taken, because a street number can also be five digits
    (``"16770 Rebar Road, Jacksonville, FL 32234"``) and the ZIP is always later in the
    string than the street number.
    """
    text = clean(address)
    if text is None:
        return None
    matches = _ZIP.findall(text)
    return matches[-1] if matches else None


def normalize_text(value: str | None) -> str | None:
    """Lowercase, strip punctuation, collapse whitespace.

    Used for names and cities. PLANTGEN's `PlantName` differs from GSPT's `Municipality`
    by case alone on at least one row (`Laplace` / `LaPlace`), so nothing downstream may
    be case-sensitive.
    """
    text = clean(value)
    if text is None:
        return None
    text = _PUNCT.sub(" ", text.lower())
    text = _WHITESPACE.sub(" ", text).strip()
    return text or None


def normalize_street(address: str | None) -> str | None:
    """Reduce a street address to a comparable token string.

    Only the part before the first comma is kept: GSPT appends city, state, ZIP and
    country to the same field, and those are compared separately as their own evidence.
    Keeping them here would let one agreement count several times.
    """
    text = clean(address)
    if text is None:
        return None
    head = text.split(",")[0]
    tokens = (normalize_text(head) or "").split()
    folded = [_STREET_TOKENS.get(token, token) for token in tokens]
    result = " ".join(folded).strip()
    return result or None


def split_aliases(value: str | None) -> list[str]:
    """Split GSPT's `Other plant names (English)` into individual normalised names."""
    text = clean(value)
    if text is None:
        return []
    parts = re.split(r"[;|]", text)
    out = []
    for part in parts:
        normalized = normalize_text(part)
        if normalized:
            out.append(normalized)
    return out


def parse_year(value: str | None) -> int | None:
    """Pull a four-digit year out of a messy date field.

    GSPT's `Start date` mixes ``"1974"``, ``"2022-06"``, ``">2019"``, ``"unknown"`` and
    ``"N/A"``. Anything that does not yield a plausible year becomes None -- and a None
    year must never be read as disagreement, only as absence, because PLANTGEN's
    `PlantStartYr` is missing on 108 of 197 rows.
    """
    text = clean(value)
    if text is None:
        return None
    match = re.search(r"\b(\d{4})\b", text)
    if match is None:
        return None
    return plausible_year(int(match.group(1)))


def plausible_year(value: int | float | None) -> int | None:
    """Keep a year only if it could be one.

    PLANTGEN stores 9999 where the start year is unknown -- on 8 of the 89 rows that
    look populated. Left alone it would reach the model as a real year and turn a pair
    with a genuine year on the other side into a *disagreement*, penalising it for what
    is really an absence.
    """
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    year = int(value)
    return year if MIN_YEAR <= year <= MAX_YEAR else None
