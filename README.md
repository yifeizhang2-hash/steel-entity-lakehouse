# steel-entity-lakehouse

A local lakehouse that integrates four datasets describing North American steel plants
into a queryable dimensional model, and resolves which rows across those datasets refer
to the same physical plant.

**Complete**: ingestion, geographic enrichment, entity resolution, labelling loop, dbt
dimensional model, Dagster orchestration. `make all` builds the whole thing from four
files in about 90 seconds.

**192 pairs have been labelled by a human**, with the model's score hidden throughout, and
that produced the first real measurement:

> **The model's scoring works. Its one-to-one assignment is a coin flip.**
> Precision 0.926 where one plant has a single confident candidate; **0.500** where it has
> several. Seven of the nine false positives in the whole sample are in that second group.

[What the labels measured](#what-the-labels-measured) has the per-stratum breakdown and
why it must not be pooled into one number.
[What this project does not know](#what-this-project-does-not-know) is the section to
read first if you are evaluating this.

---

## Architecture

```
data/raw/source_version=*/            4 files, ~19k rows, committed to the repo
  │
  │  ingest/          [Polars + dlt]  schema_overrides, Pandera contracts,
  ▼                                   cross-vintage alignment
lake/warehouse/  ── Apache Iceberg tables, SQLite catalog ──┐
  raw.plantgen 197 · raw.gspt_plants 1,848                  │
  raw.gspt_production 12,409 · raw.furnace_years 4,352      │
  │                                                         │
  ├── geo/          [GeoPandas]  coordinates → county FIPS  │
  │     geo.gspt_plant_county 93                            │
  │                                                         │
  ├── resolution/   [Splink]  blocking → EM → calibrated    │
  │     resolution.gspt_plantgen_candidates 1,484           │
  │                                                         │
  ├── labeling/     [Streamlit]  stratified queue → JSONL   │
  │     192-pair queue · decisions.jsonl: 192 human labels   │
  │                                                         │
  ▼                                                         │
transform/  [dbt on DuckDB] ◄── duckdb_lake_views ──────────┘
  5 staging views · 9 marts · 48 assertions
  │
  ▼
orchestration/  [Dagster]  assets, 5 cross-asset checks, file-arrival sensor
```

Everything is one machine, one `make` target, no cloud credentials, no accounts.

## The problem

Four sources. **No shared key between any of them.**

| # | Source | Rows | Real grain | Natural key |
|---|---|---|---|---|
| 1 | `PLANTGEN.csv` | 197 | plant | `PlantID`, 197 unique — perfect |
| 2 | GSPT `Steel Plants` | 1,848 global (111 US/Canada) | plant x production unit | `Plant ID` is **not** unique: 111 NA rows, 93 ids |
| 3 | GSPT `Yearly Production` | 12,409 after unpivot | plant x year x metric | `Plant ID` + year + metric |
| 4 | BOF / EAF furnace records | 4,352 over two vintages | **furnace x year** | `ID` is only unique *within* furnace type |

Sources 2 and 3 share `Plant ID`. Nothing else joins to anything. Source 1 has no owner
column at all; source 4 identifies plants only through a stringified Python list of
aliases like `['Kyoei Steel AltaSteel Ltd. Edmonton, Alta.']`.

So there are two jobs:

1. **Entity resolution** — decide whether a PLANTGEN row and a GSPT row are the same
   plant, and record the evidence for that decision.
2. **Modelling** — turn the four sources into dimensions and facts with yearly
   production and ownership history.

This is a **correctness-under-ambiguity** problem, not a throughput problem. About
18,800 rows in total.

## Why the stack looks like this

| Layer | Choice |
|---|---|
| Packaging | uv |
| Ingestion DataFrames | **Polars** (`ingest/` only) |
| All other DataFrames | **pandas** |
| Loader | dlt |
| Table format | Apache Iceberg (pyiceberg + local SQLite catalog) |
| Query engine | DuckDB |
| Contracts | Pandera |
| Entity resolution | Splink |
| Geo | GeoPandas + Census county boundaries |
| Transformation | dbt-core on DuckDB |
| Orchestration | Dagster assets + asset checks |
| CI | GitHub Actions, ruff, pytest, pre-commit |

Each choice answers a measured fact about *this* data, not a general preference:

| Choice | The fact that forced it |
|---|---|
| **Polars in `ingest/`** | `StCntyFIPS` is `"09001"` and `ZipCode` is `"6607"`. Inferred, they become `9001` and `6607.0`. `ShutdownYr` is present on 71/197 rows and those absences must reach Iceberg as SQL `NULL`, not float `NaN`. **Not performance** — both libraries are sub-millisecond here. |
| **pandas everywhere else** | GeoPandas is a pandas subclass; Splink expects pandas. Converting back and forth would buy nothing. |
| **Iceberg, at 19k rows** | Entity resolution is iterative. "This mart was built from this snapshot" is what makes a match run reproducible. And schema evolution is not hypothetical: the same records ship as `Corrected C/L` *and* `Corrected.C.L`. |
| **DuckDB** | Reads Iceberg, runs dbt, runs Splink's backend. One engine for the whole warehouse at this size. |
| **Pandera** | A missing required column must stop the build *before* dlt writes anything. |
| **Splink** | Four weak, partly-missing signals have to be weighed against each other. The alternative is a hand-made scorecard where every number is invented. |
| **Census 1:500k boundaries** | 11.6 MB against 80 MB for full TIGER. The cost is measured and stated: it puts one lakefront plant 255 m into Lake Michigan. |
| **dbt** | Nine marts with 48 assertions, where the assertions are the point. |
| **Dagster** | Software-defined assets and asset checks express "this table exists *and* here is what must be true about it". For a project whose argument is correctness under ambiguity, that is the feature that earns its place. |

**Deliberately not used: Spark, Kafka, Airflow, cloud warehouses.** Spark's value is
distributing work across machines; at 18,800 rows its planner costs more than the
computation. Kafka needs a stream, and every source here is a periodic file drop.
Reaching for them would show tool familiarity while showing poor judgement about when
to use them. Full reasoning, including why a lakehouse is still worth it at this size
(snapshots for reproducible match runs, explicit schema evolution), in
[ADR-001](docs/adr/ADR-001-local-lakehouse-not-spark.md).

### Two DataFrame libraries, and why that is not a mess

Polars lives only in `ingest/`. Its output is **Iceberg tables, never DataFrames** —
downstream layers re-read from DuckDB. The two mental models hand off through storage
and never meet in memory.

The reasons for Polars are exactly two, and **performance is not one of them** (both
libraries are sub-millisecond on this data):

1. **Identifier type safety.** `ZipCode`, `StCntyFIPS`, `PlantID`, `Plant ID`, `ID`,
   `FID` are identifiers that look like numbers. Inferred, `"09001"` becomes `9001`.
   `schema_overrides` makes the declaration explicit at the read call.
2. **null vs NaN.** `ShutdownYr` is present on 71 of 197 rows. Those absences must
   reach Iceberg as SQL `NULL`, not as a float `NaN` that breaks joins.

The rule is enforced by a test that parses every module's imports
(`tests/test_dataframe_boundary.py`). Full argument in
[ADR-002](docs/adr/ADR-002-dataframe-boundary.md).

## Data

**The source files are not in this repository.** The Global Steel Plant Tracker is
published by [Global Energy Monitor](https://globalenergymonitor.org/projects/global-steel-plant-tracker/)
under its own terms, and the PLANTGEN census and furnace extracts are not mine to pass
on either. `data/raw/` is gitignored.

To run the full pipeline, place the sources in this layout:

```
data/raw/
  source_version=gspt_2024/
    Global-Steel-Plant-Tracker-April-2024-Standard-Copy-V1.xlsx
    PLANTGEN.csv
    bof_mini.csv
    eaf_mini.csv
  source_version=owner_filled_2025/
    bof_owner_filled.csv
    eaf_owner_filled.csv
```

What this means in practice:

- **With the data**: everything below runs. 362 tests pass.
- **Without it**: linting, the dbt parse and the schema/contract/boundary tests still
  run; every test that reads a source skips itself rather than failing, and the
  data-dependent CI job is skipped rather than failed. A fresh clone is green, and
  green means "the parts that can be checked here were checked", not "everything works".

The one file the repository does fetch is the Census county boundary shapefile
(11.6 MB from `www2.census.gov`, cached in `geo/cache/`). `make geo-offline` refuses to
download and fails instead, if that matters to you.

## Quick start

```bash
make install     # uv sync + pre-commit hooks
make lint test   # ruff + the full pytest suite
make ingest      # build the four bronze Iceberg tables under lake/
make geo         # attach county FIPS (downloads 11.6 MB from census.gov once, then caches)
make geo-offline # same, but fail instead of downloading
make resolve     # train the linker; print blocking, calibration and weights
make queue       # show the stratified labelling queue's composition
make label       # open the Streamlit review UI (writes labeling/decisions.jsonl)
make evaluate    # precision/recall per stratum against the 192 human labels
make dbt         # build and test the dimensional model (14 models, 48 assertions)
make materialize # run the whole graph through Dagster, with asset checks
make dagster-ui  # browse the asset graph and check results
make query       # row counts, read back through DuckDB
make all         # all of the above
```

`lake/` is a build artifact and is gitignored; `make clean` deletes it. `geo/cache/` holds
the Census boundary file and is also gitignored; `make geo` re-uses it and only contacts
`www2.census.gov` when it is empty. `make clean-cache` forces a re-download.

### What `make all` actually prints

From a completely clean checkout, roughly 90 seconds:

```
$ make clean && make all
All checks passed!                              # ruff check
64 files already formatted                      # ruff format --check
plantgen         rows=    197  .../raw/plantgen/metadata/00001-....metadata.json
gspt_plants      rows=   1848  ...
gspt_production  rows=  12409  ...
furnace_years    rows=   4352  ...
GSPT -> county FIPS coverage
  North American GSPT plants                       93
    with usable coordinates                        92
  assigned a county FIPS                           83
  county present in PLANTGEN                       68
1. Blocking
  full cross join                                18321
  candidate pairs after blocking                  1484
  reduction ratio (fallback included)           0.9190
Done. PASS=69 WARN=0 ERROR=0 SKIP=0 TOTAL=69    # dbt build: 14 models, 48 assertions
343 passed                                      # pytest
geo.gspt_plant_county                    93
raw.furnace_years                      4352
raw.gspt_plants                        1848
raw.gspt_production                   12409
raw.plantgen                            197
resolution.gspt_plantgen_candidates    1484
```

### `make materialize` — the same pipeline through Dagster

```
$ make materialize
raw__plantgen                    ASSET_MATERIALIZATION   Materialized value raw plantgen.
raw__gspt_plants                 ASSET_MATERIALIZATION   ...
geo__gspt_plant_county           ASSET_MATERIALIZATION   ...
geo__..._geographic_coverage_has_not_regressed
                                 ASSET_CHECK_EVALUATION  passed.
resolution__gspt_plantgen_candidates
                                 ASSET_MATERIALIZATION   ...
resolution__..._match_precision_against_human_labels
                                 ASSET_CHECK_EVALUATION  passed.         <- now evaluates
duckdb_lake_views                ASSET_MATERIALIZATION   views raw plantgen ... (6 views)
dbt_models                       ASSET_MATERIALIZATION   marts dim_plant, fct_furnace_year, ...
                                 ASSET_CHECK_EVALUATION  48 dbt assertions, all passed.
raw__..._furnace_rows_reconcile_from_bronze_to_mart
                                 ASSET_CHECK_EVALUATION  passed.
raw__..._ownership_noise_filtering_is_bounded_on_both_sides
                                 ASSET_CHECK_EVALUATION  passed.
resolution__..._bridge_row_count_reconciles_with_the_linker
                                 ASSET_CHECK_EVALUATION  passed.
RUN_SUCCESS
```

All five Dagster checks pass. The precision check is worth showing in full, because what
it gates on is a decision:

```
match_precision_against_human_labels   passed=True
  gated_on:   high_confidence stratum at p >= 0.99
  precision:  0.926   (tp=25 fp=2 fn=0)
  floor:      0.85
  note:       NOT a pooled precision. Strata are sampled at different rates, and
              scoring and assignment fail separately.
  contested_precision_reported_not_gated: 0.5
  contested_note: one-to-one assignment is an open problem, not a threshold problem;
                  see ADR-009
  false_negatives_across_all_strata: 4
  labels_total: 192      annotators: ['fei']
```

It gates on **one stratum**, not a pooled figure. `contested` at 0.500 is measured and
reported next to it but deliberately does not gate: that number is the arithmetic of an
unsolved assignment problem, and failing the build on it would fail forever for a reason
no threshold can fix.

Before a human labelled anything, the same check reported
`SKIPPED - not evaluable / passed=False / severity=WARN` rather than a pass — a green
tick for a measurement nobody took is worse than a missing one. That branch is still
there and still tested.

### `make label` — the review UI

Streamlit, one GSPT record at a time with **all** of its queued candidates side by side:

```
GSPT record P100000120919
2 candidate(s) in the queue for this record · stratum: ambiguous, low_confidence

  field        GSPT record
  name         Cleveland-Cliffs Butler steel plant
  other names  a predecessor name
  city         Lyndora
  state        Pennsylvania
  address      1 Armco Dr, Lyndora, PA 16045-1065, United States
  owner        Cleveland-Cliffs Inc

Candidates in PLANTGEN
  reference record A               reference record B
  name    <redacted>              name    <redacted>
  city    <redacted>              city    <redacted>
  county  <redacted>              county  <redacted>
  ZIP     <redacted>              ZIP     <redacted>
  address <redacted>      address —

  > What the model thinks          > What the model thinks      <- collapsed, below the
  [Same plant] [Not a match]       [Same plant] [Not a match]      records, hidden by
  [Unsure]                         [Unsure]                        default

              [ None of these is a match ]
```

Choosing one candidate writes `match` for it and `no_match` for its rivals in the same
breath. Every decision records the annotator, a UTC timestamp, the stratum, the queue
seed, and **whether the model's probability was on screen** — so anchoring can be
measured afterwards rather than assumed absent.

## Ingestion: getting the types right

Four dlt pipelines writing Iceberg tables into `lake/warehouse/raw/`, registered in a
SQLite catalog at `lake/catalog.db`, queryable through DuckDB:

| Table | Rows | Notes |
|---|---|---|
| `raw.plantgen` | 197 | one row per plant |
| `raw.gspt_plants` | 1,848 | full sheet; 111 rows / 93 ids are North American |
| `raw.gspt_production` | 12,409 | long `(plant, year, metric, value)` |
| `raw.furnace_years` | 4,352 | both vintages of BOF + EAF, unioned and cross-aligned |
| `geo.gspt_plant_county` | 93 | one row per North American GSPT plant, with its county |
| `resolution.gspt_plantgen_candidates` | 1,484 | scored candidate pairs with their evidence vector |

```python
from lakehouse.duck import connect

con = connect()
con.sql("select * from raw.plantgen where state = 'PA'").show()
```

### Correctness properties the ingest layer guarantees

- **Identifiers stay text.** `ZipCode` is zero-padded to five characters; four PLANTGEN
  ZIPs (`6607`, `8077`, `8862`, `8872`) had already lost their leading zero *in the
  source file* and are repaired. `StCntyFIPS` keeps `09001` intact.
- **Missing is null, never NaN.** `shutdown_yr` arrives in Iceberg as a nullable
  `BIGINT` with 126 nulls and zero NaNs.
- **Two column-name spellings collapse to one.** The furnace CSVs ship as both
  `Corrected C/L` and `Corrected.C.L` (likewise `No. of furnaces` / `No..of.furnaces`,
  `Year List` / `Year.List`, `Power (kWh/ metric ton)` / `Power..kWh..metric.ton.`).
  Canonicalisation folds any spelling onto one name, so a rename is absorbed.
- **A missing required column halts the build.** Pandera contracts run on the Polars
  frame before dlt sees anything.
- **Aliases are parsed, not stringly-typed.** `Corrected C/L` is parsed with
  `ast.literal_eval` into a real `list<string>` column, queryable in DuckDB.
- **`ID` is namespaced by furnace type.** 18 ids appear in both the BOF and EAF files
  and mean different facilities (id 70 is Severstal Dearborn in BOF, American Cast Iron
  Pipe Birmingham in EAF), so the surrogate key is `furnace_key = "<type>:<id>"`.
- **Two damaged vintages are combined by alignment, not by string repair.** See below
  and [ADR-003](docs/adr/ADR-003-encoding-degradation.md).
- **R's `NA` is a null.** The furnace CSVs were written by R; left unlisted, `"NA"`
  would have become a literal string in `ID`, `FID`, `Data_ID` and `Power`.
- **Annotated counts are split, not dropped.** `No. of furnaces` holds `1 (#5)`, `1*`,
  `Not melting`. The parsed integer, the verbatim cell and a `not_melting` boolean are
  all kept.

### Bronze is faithful

The bronze tables keep what the sources say, including what they say badly. GSPT
capacity columns stay text because the sheet mixes numbers with a sentinel vocabulary
(`unknown`, `>0`) and collapsing those to null would erase the difference between
"reported as unknown" and "not reported". Sentinel resolution belongs in the dbt
staging layer, where it is a documented and tested transformation. Likewise the whole
global GSPT sheet is loaded, not just North America; the 111-row subset is a downstream
filter on `country_area`.

## What the sources actually contain

Thirteen measured defects are catalogued in
**[docs/data-quality-findings.md](docs/data-quality-findings.md)**, including three that
were found *because an assertion failed* and one methodological error in my own
verification. The four that shape everything downstream:

**The furnace census ships in two vintages and neither is usable alone.** `gspt_2024` has
intact facility names and **no owner column**; `owner_filled_2025` has every owner and
**not one surviving non-ASCII character** — `Nucor Steel–Berkeley` became
`Nucor Steel?Berkeley`, `North American Höganäs` became `North American H?an?`, losing
two characters outright. 143 rows across 12 facilities. They are combined by *aligning*
on `(furnace_type, id, year, fid)`; no `?` is ever substituted back, because a `?` does
not say how many characters it replaced. [ADR-003](docs/adr/ADR-003-encoding-degradation.md)

**`Coordinate accuracy = 'approximate'` means invented, not imprecise.** "Nucor Steel
Pacific Northwest", whose address field reads "Pacific Northwest, United States", is
recorded at 37.0902, −95.7129 — the geographic centre of the contiguous US — and resolves
to **Montgomery County, Kansas**. Excluded outright from geographic blocking; a weight
cannot repair a fabricated value.

**PLANTGEN's "plant name" is a place name**, equal to its own `City` on 168 of 197 rows
(85.3%). Entity resolution must compare it against GSPT's *city*, not GSPT's plant name.

**A North American dataset contains South American plants that can never join.** 22 rows
— a South American facility (Montevideo), another South American facility (Caracas) — have no `ID`, `FID` or `Data_ID` at all.
Bronze keeps them flagged; staging filters them.

## Geography: the strongest cross-source link, and its ceiling

`make geo` resolves GSPT coordinates to county FIPS and puts them in the same key space
as PLANTGEN's `StCntyFIPS`. Absolute counts, at plant grain (93 North American plants,
not 111 plant-unit rows):

| | plants |
|---|---|
| North American GSPT plants | 93 |
| with usable coordinates | 92 |
| assigned a county FIPS | **83** |
| not assigned (8 Canada, 1 US) | 9 |
| assigned county present among PLANTGEN's 136 | **68** |
| assigned county absent from PLANTGEN | 15 |

Split by accuracy: `exact` 90 plants → 81 assigned → **68** matched; `approximate`
3 plants → 2 assigned → **0** matched.

The 15 unmatched are mostly mills newer than PLANTGEN's vintage (Nucor Brandenburg,
SDI Sinton, Nucor West Virginia, Mesabi Nashwauk).

**The ceiling of county-level blocking.** Of PLANTGEN's 197 plants in 136 counties,
105 counties hold exactly one plant and 31 hold more than one. So **105 plants are alone
in their county and 92 share it** — for those 92, a county match narrows the candidate
set but cannot identify a plant. That residual is what the matching model must cover with other evidence
(city, owner, capacity, start year).

**The 1:500k boundary file has a measured cost.** Cleveland-Cliffs Indiana Harbor sits
on the Lake Michigan shore; the generalised county boundary cuts inland of it, so the
point falls in the lake — **255 m** outside Lake County, IN. Heavy industry is
disproportionately waterfront, so this is a systematic risk, not a fluke. Rather than
snapping it to the nearest county, `geo.gspt_plant_county` records
`nearest_county_fips` and `nearest_county_distance_m` for every unassigned point. The
distance separates the cases cleanly: 255 m for the clipped shoreline plant, 1.1 km for
Algoma across the St. Marys River, and 59–166 km for the seven Canadian plants that are
genuinely not in any US county. Whether a near miss counts as a match is a matching
decision, left to the matching layer.

## Entity resolution: what the evidence is actually worth

`make resolve` trains a Splink `link_only` model. **No weight is set by hand** — a test
fails the build if any parameter is fixed. See
[ADR-004](docs/adr/ADR-004-probabilistic-matching.md).

**Blocking.** County FIPS, ZIP and normalised state, plus a fallback for records no rule
can reach:

```
full cross join                       18,321
candidate pairs after blocking         1,484
  of which from the no-key fallback      591   (3 records with no blocking key)
reduction ratio (fallback included)     0.9190
```

The three unreachable records are the placeholder-coordinate plants: no county, no ZIP,
and state recorded as `unknown`. Without the fallback they would not score badly, they
would simply be absent — and the reduction ratio would look better for having dropped
them.

**Calibration.** The distribution is bimodal, which is what a usable model looks like:
1,377 pairs below 0.01, 64 above 0.99, and only 12 in the ambiguous middle.

**What the evidence is actually worth**, EM-estimated, with the pair count each rests on:

| comparator | level | pairs | match weight |
|---|---|---|---|
| place | exact | 79 | **+7.67** |
| street | exact | 15 | +7.56 |
| county | near 500 m | 3 | +7.41 *(low support)* |
| zip5 | exact | 55 | +7.06 |
| county | exact | 140 | +6.83 |
| alias_place | contains token | 34 | +6.74 |
| start_year | same year | 17 | +5.00 |
| place | disagree | 1,395 | **−4.03** |
| county | disagree | 750 | −3.72 |
| start_year | within 3 years | 24 | +1.42 |

Three levels could not be estimated and are **left unset rather than guessed**:
`place.place_jw_92`, `alias_place.alias_exact_element` and `county.county_near_5km` —
each because zero candidate pairs fell into it. Three more are flagged **low support**
(fewer than 10 pairs), including the 255 m geography level, whose weight rests on three
pairs from a single plant.

**Does county blocking failing actually hurt?** In aggregate, barely:

| PLANTGEN plants | count | best match > 0.9 |
|---|---|---|
| alone in their county | 105 | 36 (34%) |
| sharing a county | 92 | 33 (36%) |

ZIP, street and place carry the pairs county cannot separate. **But the average hides a
county where they do not.** The two most crowded counties behave completely differently:

| county | plants | with ZIP | distinct ZIPs | ZIP separates all? |
|---|---|---|---|---|
| Allegheny PA (Pittsburgh) | 9 | 9 | 9 | **yes** |
| Cook IL (Chicago) | 10 | 9 | 7 | **no** |

In Chicago, two pairs of plants share a ZIP (`60411`, `60617`) and one plant has no ZIP
at all, so county and ZIP fail *together*. Geographic blocking is weakest exactly where
steel is densest, and Cook County is therefore sampled exhaustively for labelling.

**Sometimes geography carries no information at all, not merely weak information.**
Cleveland-Cliffs Indiana Harbor reaches two PLANTGEN plants that share its county
(18089), its ZIP (46312) *and* its city (East Chicago). Only the plant name differs —
`Indiana Harbor` against `East Chicago` — and one of the two has no street address. This
is not a modelling defect; it is real ambiguity in the data, and it goes to a human.

**The Canadian plants score nothing, correctly.** All 8 generate **zero candidate
pairs** — they are never compared, rather than compared and rejected. Those are
different outcomes and the report prints them differently. They stay in the denominator:
dropping them in staging would shrink it and make the match rate look better than it is.

## What the labels measured

192 pairs, one annotator (`fei`), **192 of 192 with the model's probability hidden**.
`make evaluate` reproduces everything below.

### Per stratum, at p ≥ 0.99 — and these must not be pooled

| stratum | labelled | TP | FP | FN | TN | precision |
|---|---:|---:|---:|---:|---:|---:|
| `cook_county` | 94 | 1 | 0 | 0 | 93 | **1.000** |
| `low_confidence` | 30 | 0 | 0 | 0 | 30 | — |
| `high_confidence` | 27 | 25 | 2 | 0 | 0 | **0.926** |
| `contested` | 20 | 7 | 7 | 0 | 6 | **0.500** |
| `ambiguous` | 10 | 0 | 0 | 4 | 6 | — |
| | | 33 | 9 | 4 | 135 | |

**There is deliberately no headline precision figure.** Three strata were sampled
exhaustively and two as 30-pair random draws, so a pooled number averages over a sample
the *sampler* composed rather than over the world. It would also hide the finding:

```
not contested    28 predicted positive,  26 correct    precision 0.929
contested        14 predicted positive,   7 correct    precision 0.500
```

**Scoring and assignment fail separately, and only one of them is a threshold problem.**
`contested` is *defined* as the model putting several candidates for one plant above the
bar; at most one can be right, so precision converges on 1/k and 7/14 is the arithmetic
of a coin flip, not a scoring defect. See
[ADR-009](docs/adr/ADR-009-one-to-one-assignment-is-unresolved.md).

**The same 50% appears again at facility grain, by a different route.** Taking each
contested plant's highest-scoring candidate — what any greedy or Hungarian assignment
reduces to here — against the human verdict, across the 10 contested plants:

```
5 right · 2 wrong · 3 where the correct answer was "none of these"   =  5/10
```

Those three are the important ones. **An assignment algorithm cannot express "none of
the above"** — its only operation is selecting an element, so given a row of confident
candidates it must return one. On 3 of 10 plants it is not choosing badly among plausible
options; it is forced to emit a confident error where the correct output is silence.

And two of the ten have a probability gap of **literally `0.000000`**: three candidates
for Cleveland-Cliffs Cleveland carry bit-identical scores and identical evidence vectors,
so their ordering is decided by row order. Nucor Steel Seattle has two PLANTGEN
candidates both named `Seattle`, in city `Seattle`, ZIP `98106`, county `53033` — every
compared field identical. The model is not uncertain there; it has no information at all,
and scoring them equally is the correct report of that.

This settles a decision made before there were labels: **greedy one-to-one assignment
would have been 50% correct on exactly the pairs where it would have been applied.**

### Three results only a stratified queue could produce

**Zero false negatives in the low-confidence stratum.** 30 pairs drawn from below p=0.01
(max probability 2.15 × 10⁻⁴) contained **no true matches at all**. The model missed
nothing down there. Label only the top of the ranking and this number does not exist —
recall becomes uncomputable and reads as 1.0 for the wrong reason.

**Cook County: 94 labelled pairs, 1 true match, found — precision 1.000, zero misses.**
The hardest subgroup in the dataset, where county *and* ZIP fail together, and the model
was perfect on it. The cost is worth stating plainly: **94 labels bought 1 positive.**
Exhaustive sampling was right once, to get a recall number for a hard region that is not
an extrapolation; a repeat should draw ~30.

**The anchoring check is clean: `shown=0, hidden=192`.** The annotator never saw the
model's probability. This matters more than it looks — a label set produced while looking
at the model's answer cannot be used to evaluate that model, and the only way to know is
to record what was on screen at decision time. It was recorded per decision, not assumed.

### The labels barely moved the weights

Re-estimating m from the labels against the EM-only fit, the largest change across all
comparison levels is **0.31** (`zip5`, disagreement level, −1.02 → −1.34); `place_exact`
moves by 0.003. EM, trained with no labels at all, had essentially found the same
parameters a human's 37 positive examples imply. That is independent corroboration of the
unsupervised fit — and it is also why the correlated-comparator problem survives: labels
this few shift the weights, they do not restructure them.

## The labelling loop: producing ground truth without corrupting it

A labelling loop. **It produces no labels** — that is the point; a machine-written
decision log could not be told apart from human judgement afterwards.

**Stratified queue** (`make queue`), reproducible from a fixed seed:

| stratum | pairs | exhaustive | why |
|---|---:|---|---|
| ambiguous `[0.1, 0.9)` | 12 | yes | the model is genuinely unsure |
| contested | 26 | yes | competing confident claims; the machine cannot separate them |
| Cook County | 94 | yes | county *and* ZIP fail together here |
| high confidence `≥0.99` | 30 of 45 | no | check for false positives |
| low confidence `<0.01` | 30 of 1,298 | no | **check for false negatives — without these, recall is not computable** |
| **total** | **192** | | |

The last row is the one that is easy to skip and must not be. Label only the top of the
ranking and you get a precision number with no recall number, and no way to correct the
feature correlation the model is known to have.

**Review UI** (`make label`), built around two rules:

- *The model's opinion is not visually dominant.* Probability and evidence vector sit
  collapsed, below the records, away from the verdict buttons — and whether they were
  revealed is recorded with every decision, so anchoring can be measured rather than
  assumed away.
- *Every candidate for a record is shown at once.* Asking "is this the one?" one at a
  time is an easier and different question from "which of these, if any?". Choosing one
  candidate writes `no_match` for its rivals in the same breath.

**Append-only decision log** (`labeling/decisions.jsonl`). Nothing is ever edited;
changing your mind appends a line and the old one stays. Each record carries the pair,
the verdict, the annotator, a UTC timestamp, the stratum, the queue seed and the
probability that was on screen.

**Evaluation** (`make evaluate`): precision/recall at seven thresholds, per-stratum
breakdown, an anchoring check, and m re-estimated from the labels with a side-by-side
weight comparison against the EM-only fit.

## The dimensional model

`make dbt` builds 14 models and 48 assertions on DuckDB over the Iceberg lake.
**`dbt build`: 62 nodes, PASS=62, ERROR=0.**

| mart | rows | grain |
|---|---:|---|
| `dim_plant` | 290 | plant, per source (93 GSPT + 197 PLANTGEN) |
| `dim_production_unit` | 111 | plant x production unit |
| `dim_owner` | 45 | GSPT `Owner PermID` |
| `dim_furnace` | 212 | furnace, keyed `<type>-<id>-<fid>` |
| `fct_furnace_year` | 2,153 | furnace x year, 2012–2023 |
| `fct_plant_production_year` | 764 | plant x year x metric, 2019–2022 |
| `dim_facility_ownership_scd2` | 179 | ownership span (161 facilities) |
| `fct_ownership_change` | 18 | classified transition |
| `bridge_plant_xref` | 1,492 | candidate link (1,484 scored + 8 never compared) |

**Every filter reconciles against bronze**, and filters live in staging so a scope
decision is visible in one file:

```
raw.furnace_years      4,352
  − superseded vintage  2,153
  − South American         22   (no ID; can never join)
  − exact duplicates       24   (one facility, see below)
  = fct_furnace_year    2,153
```

**`bridge_plant_xref` contains no verdict, and a test fails the build if one appears.**
Zero labels, no threshold, and correlated comparators — see
[ADR-006](docs/adr/ADR-006-marts-carry-no-unearned-conclusions.md). The 8 Canadian
plants are `never_compared`, not "low probability", and unresolved rows carry no
candidate key so a downstream join cannot pick up a link the model never endorsed.

**A changed string is not a changed owner.** Raw diffs find 10 changes over 18 span
boundaries; `fct_ownership_change` classifies them and **2** survive:

| class | n | what it is |
|---|---:|---|
| `observation_gap` | 8 | owner unchanged; the facility left the data for a year |
| `round_trip` | 4 | both legs of an A → B → A excursion inside two years |
| `likely_typo` | 2 | `JSW Steel USA` → `JSW Stee USA` → back: one dropped letter, two fake transfers |
| `name_refinement` | 2 | `ArcelorMittal` ↔ `ArcelorMittal Dofasco Inc.` |
| **`credible_transfer`** | **2** | BOF-70 → ArcelorMittal (2022); Big River Steel → U.S. Steel (2022) |

**This number was wrong and has been corrected.** The classifier used to judge each
transition alone, so in `A -> B -> A` only the *return* leg looked like a
round trip and the outbound leg survived as "credible". If a record bounces back within
two years, the outbound step is exactly as unreliable as the one undoing it — credibility
is a property of the **sequence**, which no per-transition rule can express. The fix took
credible transfers from 4 to 2, while leaving `BOF-70 2022`, which happens *after* the
excursion, intact. A regression test pins that survival, because an implementation
collapsing each facility to its modal owner would delete it and conclude that 100% of BOF
ownership changes are noise.

**"Reported as unknown" stays distinct from "never reported."** Every sentinel-bearing
column carries a `_status` in `{reported, unknown, bounded, unparsed, absent}`.

**The assertions have been watched failing.** Four injection tests break the data on a
copy and require `dbt build` to go red: a duplicated source row, a duplicated fact row,
an overlapping SCD2 interval, and an `is_match` column added to the bridge.

### A defect the assertions found

The grain assertion failed the first time it ran. `eaf_owner_filled.csv` contains
facility EAF:79 (the facility) **48 times where it should appear 24** —
every row duplicated on the next source line, byte-identical including `Data_ID`, and no
other facility affected. The other vintage settles it: `eaf_mini.csv` holds exactly 24.
Staging deduplicates, and a guard pins how many rows the dedup may remove so a future
vintage duplicating something else fails the build rather than vanishing into a
`qualify`.

## Orchestration and quality gates

`make materialize` runs the whole pipeline as a Dagster asset graph:

```
raw/plantgen  raw/gspt_plants  raw/gspt_production  raw/furnace_years
        |            |                 |                   |
        +------------+--------+--------+-------------------+
                              v
                    geo/gspt_plant_county
                              v
              resolution/gspt_plantgen_candidates
                              v
                 views/* (6)  <- duckdb_lake_views
                              v
              14 dbt models + 48 dbt assertions
```

`RUN_SUCCESS`, end to end from `make clean`.

**The 48 dbt assertions are not duplicated as Dagster checks.** Dagster runs dbt and
surfaces dbt's own results, so each rule has exactly one definition. The 5 Dagster-native
checks cover only what dbt cannot see — see
[ADR-007](docs/adr/ADR-007-orchestration-checks-do-not-mirror-dbt.md):

| check | what it catches |
|---|---|
| `bridge_row_count_reconciles_with_the_linker` | 1,484 candidates + 8 never-compared = 1,492 |
| `furnace_rows_reconcile_from_bronze_to_mart` | every dropped row explained by a named filter |
| `geographic_coverage_has_not_regressed` | coverage falling versus the previous run |
| `ownership_noise_filtering_is_bounded_on_both_sides` | filters that stopped working **and** filters eating real transfers |
| `match_precision_against_human_labels` | precision on the `high_confidence` stratum; skips if no labels exist |

That last one now evaluates: **0.926 on the `high_confidence` stratum**, gated there and
not on a pooled figure. Before a human labelled anything it returned `passed=False`,
severity `WARN`, `status: "SKIPPED - not evaluable"` — never a pass. A green tick on an
unmeasurable property sits in the same column as a measured one, and that difference is
the whole point of this project. The skip branch is still tested.

`duckdb_lake_views` is an asset rather than a resource, because rebuilding the views is
real work that can really fail and its failure should have a place in the lineage. dbt's
sources are mapped onto those view assets, so the graph says `bronze → view → dbt` —
which is what actually happens, since dbt reads DuckDB views and not Iceberg.

## What this project does not know

The most useful thing a portfolio project can do is be precise about its own edges.
These are in rough order of how much they would matter to someone relying on the output.

### 1. The accuracy numbers are within-sample, and rest on 37 positives

There are now real numbers, and four things limit what they can be asked to support.

**They are within-sample, not population precision and recall.** The five strata were
sampled at deliberately different rates — `cook_county`, `contested` and `ambiguous`
exhaustively, `high_confidence` and `low_confidence` as 30-pair random draws from 45 and
1,298. Every figure is therefore conditional on a stratum whose sampling rate the
*sampler* chose. Pooling them produces a number that describes the queue, not the data,
which is why no pooled precision appears anywhere as a headline.

**37 positive labels is a thin basis.** `high_confidence` precision of 0.926 is 25
correct out of 27; two more errors would take it to 0.852. The label-informed
re-estimation of m rests on those same 37 positives, so the re-estimated weights are
themselves noisy — which is part of why they moved so little.

**11 `unsure` verdicts are excluded from every metric, not classified.** Six of them are
in `contested`, which is exactly where the difficulty is: the annotator declined on
almost a third of that stratum. Counting them as either class would invent a judgement
that was explicitly withheld, but excluding them means `contested` precision is computed
on the 14 pairs where a call was possible, and the six hardest are simply absent from it.

**One annotator, so there is no inter-annotator agreement to measure.** Every label is
`fei`'s. Nothing here distinguishes "this pair is genuinely a match" from "this is how
one person read an ambiguous pair", and on a task whose whole premise is ambiguity that
is a real gap. A second annotator on an overlapping subset would be the first thing to
add.

What the labels do support is the structural finding — scoring at 0.93, assignment at
0.50 — because that gap is far larger than the noise in a 37-positive sample, and it has
a mechanism ([ADR-009](docs/adr/ADR-009-one-to-one-assignment-is-unresolved.md)) rather
than just a measurement.

### 2. The match weights are inflated, and by an unknown amount

Splink assumes comparisons are conditionally independent given match status. Three of
this model's comparators — `zip5`, `street` and `county` — are **geographically
correlated by construction**: a pair agreeing on ZIP will usually agree on county. The
same evidence is therefore counted more than once, and confident weights are higher than
they should be.

`place` and `alias_place` were deliberately built to avoid this (which is why place-name
agreement is one comparator and not two, and why the alias comparator uses only GSPT's
*secondary* names). The geographic three could not be. Correcting it needs enough labelled pairs to
re-estimate m against the correlation; 37 positives were not enough. Running
`estimate_m_from_pairwise_labels` against them moved the largest weight by **0.31** and
most by under 0.05 — the labels confirmed the EM fit rather than restructuring it.

**The direction of the error is known; the size is not.** Four of the five contested
plants where taking the top score happened to be *correct* were decided by a gap under
0.001 — differences this inflation makes untrustworthy, which is why no assignment is
made on them. A coin landing the right way up is not evidence that flipping it works.

### 3. Three comparison levels have no estimate at all

`place.place_jw_92`, `alias_place.alias_exact_element` and `county.county_near_5km` were
each observed in **zero** candidate pairs, so neither m nor u could be estimated. They
are left unset rather than filled with a default, and the report names them.

Three more are estimated from fewer than ten pairs and flagged **low support** —
including `county.county_near_500m` at weight +7.41, which rests on **three pairs from a
single plant**. It is also the level that rescues Indiana Harbor, so it is kept rather
than deleted, and labelled rather than trusted.

### 4. The ownership heuristics are hand-set and unvalidated

Edit distance ≤ 2, substring containment, a 3-year round-trip window, and a noise band of
0.30–0.95. Every one was chosen by looking at these 18 transitions. None has been checked
against a label.

Every row of `fct_ownership_change` carries
`classification_confidence = 'heuristic_unvalidated'` and a `classification_rule` naming
the exact threshold that fired. **The 2 credible transfers are candidates, not facts.**

This figure was also wrong once and has been corrected: an earlier classifier judged each
transition in isolation, which made only the *return* leg of an A→B→A excursion look like
a round trip and let the outbound leg through as credible. Credibility is a property of
the sequence. Fixing it took the count from 4 to 2.

### 5. One-to-one assignment is unsolved

Measured at **0.500** twice over: on the 14 contested pairs where a call was possible,
and on the 10 contested plants at facility grain (5 right, 2 wrong, 3 where the answer
was "none of these").

No matcher is implemented, and the reason is stronger than a tuning argument. **An
assignment algorithm cannot express "none of the above"**, which is the correct answer
for 3 of those 10 plants; its only operation is selecting an element. Where it does
choose, two of the ten have bit-identical candidate scores with identical evidence
vectors, so it would be resolving them by row order dressed as optimisation. Contested
records stay visible as multiple candidates and go to a person.
[ADR-009](docs/adr/ADR-009-one-to-one-assignment-is-unresolved.md) records the two paths
and the conditions under which assignment becomes reasonable.

### 6. CI cannot run the pipeline, only the parts that need no data

Because the sources are not redistributed (see [Data](#data)), CI is split in two. The
`check` job always runs: lint, `dbt parse`, and the full test suite, in which every
data-reading test skips itself. The `pipeline` job -- ingest, geo, resolution, dbt build,
Dagster materialize -- is `workflow_dispatch` only, because the sources are gitignored
and a GitHub-hosted runner therefore never has them. **On this repository that job does
not run**, and saying it is "skipped when data is absent" would have dressed up a
permanent condition as a conditional one.

So a green CI badge here means less than it looks like. It means the code lints, the dbt
project parses, and the data-independent tests pass. **The end-to-end run is verified on
my machine and nowhere else**, and that is a weaker claim than a green pipeline job would
have been.

### 7. Smaller things, each measured

- **Single machine.** Every source is read fully into memory. Correct at 19k rows;
  it does not survive an input that does not fit in RAM.
- **The 1:500k boundary file is too coarse for waterfront plants.** Indiana Harbor lands
  255 m into Lake Michigan. Reported as a measured near-miss rather than snapped; full
  TIGER would fix it and only `geo/download.py` would change.
- **dlt's `"columns": "freeze"` contract does not fire on a table's first load** — it is
  evaluated against the persisted schema. Pandera `strict=True` plus an explicit
  projection is the first gate; the contract is the second. Both pinned by tests.
- **8 Canadian plants can never match.** PLANTGEN is a US census. They are
  `never_compared`, not "low probability", and they stay in the denominator — filtering
  them would shrink it and flatter the match rate.
- **The encoding damage is permanent.** 11 of 12 damaged names could be reconstructed by
  guessing; the twelfth cannot, and nothing distinguishes the two classes from the
  damaged string alone. Nothing is guessed.
- **Bronze trims whitespace.** `clean_text` strips it before the Iceberg write, so
  `owner_verbatim` exists to preserve the untrimmed cell. A difference that only exists
  before trimming would otherwise be invisible downstream — which is exactly how the
  EAF:79 duplicates were once mis-diagnosed.

## Documentation

- **[Architecture decision records](docs/adr/README.md)** — eight decisions, each
  answering a question the data forced. The index explains the thread running through
  them.
- **[Data quality findings](docs/data-quality-findings.md)** — thirteen measured defects,
  including three found *because an assertion failed*, and one methodological error in
  my own verification.

## Repository layout

```
ingest/        [Polars]  four dlt pipelines -> Iceberg
lakehouse/     [neutral] SQLite Iceberg catalog + DuckDB access
geo/           [pandas]  Census county boundaries, point-in-polygon
resolution/    [pandas]  Splink blocking, comparisons, scoring
labeling/                Streamlit review UI, stratified sampler, evaluation
transform/               dbt: 5 staging views -> 9 marts, 48 assertions
orchestration/           Dagster assets, asset checks, file-arrival sensor
baselines/               tracked reference values for the drift checks
tests/                   343 pytest tests
docs/adr/                architecture decision records
data/raw/                the four sources, committed
```
