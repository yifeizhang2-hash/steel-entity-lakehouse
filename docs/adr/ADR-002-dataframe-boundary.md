# ADR-002: Two DataFrame libraries, separated by storage

- **Status**: accepted
- **Date**: 2026-09-18
- **Deciders**: project author

## Context

The repository uses two DataFrame libraries:

- **Polars** in `ingest/`
- **pandas** everywhere downstream (`geo/`, `resolution/`, `labeling/`, `transform/`)

Using two is a cost. They disagree about missing values, about string types, about
whether an operation is eager or lazy, and about what an index is. A reader switching
between them has to switch mental models, and a value passed between them can change
meaning silently.

The cost is paid deliberately, and it is paid **once, at a boundary**, not continuously.

## Decision

### 1. Polars is confined to `ingest/`

The ingest layer's whole job is to get types right on the way in. Polars is chosen for
exactly two reasons.

**Identifier type safety.** This project is full of numbers that are not numbers.
`PlantID`, `ZipCode`, `StCntyFIPS`, GSPT's `Plant ID`, the furnace `ID` and `FID` are
all identifiers. Read with type inference, pandas turns `ZipCode` into `float64` and
`StCntyFIPS` into `int64`; `"09001"` becomes `9001` and `"00601"` becomes `601.0`.
Polars' `schema_overrides` makes the declaration explicit, per column, at the read call
— it is visible in the diff and a reviewer can check it. pandas' `dtype=` argument can
do the same thing, so this is not a capability Polars uniquely has; it is that Polars
makes the declaration the normal way to read a file rather than the careful way.

**Null and NaN are different things.** pandas has historically represented a missing
integer by promoting the column to float and writing `NaN`. `NaN` is a float that is
not equal to itself: it breaks joins, it breaks `GROUP BY`, and it survives into
Parquet as a value rather than as an absence. That matters directly here — `ShutdownYr`
is present on 71 of 197 rows, `PlantStartYr` on 89 of 197, and those absences must
arrive in Iceberg as SQL `NULL`. Polars has one missing value, `null`, and an `Int64`
column keeps its type when values are missing. (pandas 2+ offers nullable extension
dtypes that behave correctly, but they are opt-in; in Polars the correct behaviour is
the only behaviour.)

**Performance is not a reason.** On 18,800 rows both libraries finish in milliseconds.
Any claim that Polars was chosen here for speed would be a claim about a difference too
small to measure. It was chosen for semantics.

### 2. pandas everywhere downstream

The downstream libraries are pandas-native: GeoPandas is a pandas subclass, and Splink
expects pandas or a SQL backend. Converting to Polars just to convert back would add a
translation layer that buys nothing.

### 3. The two never meet in memory

**`ingest/` produces Iceberg tables. It does not produce DataFrames.**

- No function in `ingest/` returns a DataFrame to a downstream caller. The public
  entry point, `ingest.pipeline.run`, returns `list[LoadResult]` — table name, row
  count, Iceberg metadata location.
- No module in `geo/`, `resolution/`, `labeling/`, `transform/` or `orchestration/`
  imports from `ingest/`, or imports Polars.
- Downstream layers read through `lakehouse.duck.connect()`, which exposes each Iceberg
  table as a DuckDB view. They materialise pandas frames from DuckDB when they need to.
- `lakehouse/` itself imports neither library. It is the neutral storage boundary.

The hand-off is a **table**, not an object. Two mental models, joined by a file format
that has one.

## The one documented exception

`ingest/excel.py` imports pandas.

The GSPT `Yearly Production` sheet has a two-row header: row 0 is a merged-cell year
banner spanning seven metric columns, row 1 carries the full label. Reconstructing the
banner means reading the sheet header-less and forward-filling a row by position, which
pandas expresses directly. The parse is also used to cross-check the two header rows
against each other — the year appears in both, so a shifted banner is caught rather than
silently mis-dating production values.

The exception is bounded: the module returns a list of plain dicts. No pandas object
leaves it. The reason is written at the top of the file, and
`tests/test_dataframe_boundary.py` fails if any other module in `ingest/` imports
pandas, or if that file stops explaining itself.

## Consequences

- One rule to remember: *Polars reads files, pandas reads tables.*
- The boundary is executable, not aspirational —
  `tests/test_dataframe_boundary.py` parses every module's imports and fails on a
  violation.
- A type error can only enter at one place, and that place has Pandera contracts on it.
- Cost: contributors must know both libraries. Accepted, because the alternative is
  either fragile identifier handling on the way in or an unnecessary translation layer
  on the way out.
