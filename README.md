# steel-entity-lakehouse

This project ingests four datasets describing North American steel plants, works out
which records across them refer to the same physical plant, and publishes a dimensional
model of plants, owners, furnaces, production and ownership history, with the match
quality measured against human labels rather than asserted.

## Why this exists

Four sources describe the same steel plants and none of them share an identifier. One
registry names a plant `Cleveland-Cliffs Butler steel plant`; a US facility census calls
the same site `Butler`, and for 85% of its rows that "plant name" column holds a city
name. Joining them is the whole problem, and the naive join is a string match that quietly
gets it wrong.

Getting it right matters because everything downstream depends on plant identity: annual
production, ownership transfers, capacity by process. A wrong link silently merges two
plants or splits one, and no aggregate built on top will look wrong.

Written for anyone evaluating data engineering work: how the sources are typed on the way
in, what the marts contain, what is checked, and how good the matching actually is. The
[Limitations](#limitations) section is the honest part.

## Data sources

| Source | Format | Rows | Grain | Key | In repo? |
|---|---|---:|---|---|---|
| Global Steel Plant Tracker (GSPT), Apr 2024 | Excel, `Steel Plants` sheet | 1,848 (111 North American) | plant x production unit | `Plant ID`, not unique | No |
| GSPT yearly production | Excel, `Yearly Production` sheet | 12,409 | plant x year x metric | two-row header, unpivoted on read | No |
| PLANTGEN | CSV | 197 | one US facility | `PlantID`, unique | No |
| BOF / EAF furnace records | CSV, two vintages | 4,352 | facility x year x furnace | `(furnace_type, ID, Year, FID)` | No |

The source files are not in this repository. GSPT is published by
[Global Energy Monitor](https://globalenergymonitor.org/projects/global-steel-plant-tracker/)
under its own terms; the other three are licensed and not mine to pass on. `data/raw/` is
gitignored. To run the pipeline, place them as:

```
data/raw/source_version=gspt_2024/          GSPT xlsx, PLANTGEN.csv, bof_mini.csv, eaf_mini.csv
data/raw/source_version=owner_filled_2025/  bof_owner_filled.csv, eaf_owner_filled.csv
```

The furnace files ship in two vintages with different column names, `Corrected C/L`
against `Corrected.C.L`, because one branch passed through R's `make.names()`. Both
resolve to one contract on read. No source refreshes on a schedule; each is a file drop.

## Pipeline overview

```text
four source files
  -> ingestion          Polars + dlt, identifiers locked to text, contracts on column names
  -> bronze             four Iceberg tables, partitioned by source_version
  -> geo enrichment     coordinates -> county FIPS via Census boundaries
  -> entity resolution  Splink: block on county/ZIP/state, EM-estimated weights
  -> labelling          stratified queue -> human verdicts -> precision and recall
  -> staging            5 dbt views, sentinels separated from real values
  -> marts              10 dbt tables: dims, facts, SCD2 ownership, candidate bridge
  -> checks             54 dbt assertions, 5 Dagster asset checks, 362 pytest tests
```

## Architecture

| Stage | Tool | Why here |
|---|---|---|
| Ingestion | dlt + Polars | Schema contracts on column names; strict types so identifiers survive |
| Storage | Apache Iceberg + SQLite catalog | Snapshots per source version, no server |
| Query | DuckDB | Reads Iceberg directly, zero setup |
| Geo | GeoPandas + Census county boundaries | Offline point-in-polygon, cached |
| Resolution | Splink on DuckDB | Weights estimated by EM rather than chosen by me |
| Transformation | dbt-core | 5 staging views, 10 marts, 54 assertions |
| Orchestration | Dagster | Assets partitioned by source version, checks across assets |
| Review UI | Streamlit | Stratified labelling queue, append-only decision log |
| Testing | pytest + dbt tests | 362 + 54 |

No Spark, Kafka or cloud warehouse. Four files, 19k rows, one machine.

## Quick start

Prerequisites: Python 3.11, [uv](https://docs.astral.sh/uv/), and `make`. No environment
variables, no cloud credentials, no database server.

```
make install     # uv sync + pre-commit hooks
make test        # 362 tests
make ingest      # four bronze Iceberg tables under lake/
make geo         # county FIPS (downloads 11.6 MB from census.gov once, then caches)
make resolve     # train the linker, print blocking and weights
make dbt         # build and test the dimensional model
make materialize # the whole graph through Dagster, with asset checks
make all         # everything above
make query       # row counts, read back through DuckDB
```

Without the source data, `make install`, `make lint` and `make test` still work; every
test that reads a source skips itself, and `make ingest` onwards will fail for want of
input. `make geo-offline` fails instead of downloading if the boundary cache is empty.

`make clean && make all` on a machine that has the data, about 90 seconds:

```
All checks passed!                              # ruff check
65 files already formatted                      # ruff format --check
plantgen         rows=    197
gspt_plants      rows=   1848
gspt_production  rows=  12409
furnace_years    rows=   4352
  North American GSPT plants                       93
    with usable coordinates                        92
  assigned a county FIPS                           83
    county present in PLANTGEN                     68
  full cross join                                18321
  candidate pairs after blocking                  1484
Done. PASS=69 WARN=0 ERROR=0 TOTAL=69           # dbt: 15 models, 54 assertions
362 passed                                      # pytest
```

## Outputs

Ten marts, built by dbt into DuckDB over the Iceberg lake.

| Table | Rows | Grain | Downstream use |
|---|---:|---|---|
| `dim_plant` | 290 | one plant per source system | plant lookup; the two populations sit side by side rather than merged |
| `dim_production_unit` | 111 | plant x production unit | capacity and process by unit, GSPT's real grain |
| `dim_owner` | 45 | one owner | the only stable external identifier in the project, `Owner PermID` |
| `dim_furnace` | 212 | one furnace | keyed on furnace type plus id, because id alone collides across types |
| `fct_furnace_year` | 2,153 | facility x year x furnace | annual furnace observations, 2012-2023 |
| `fct_plant_production_year` | 764 | plant x year x metric | annual production, 2019-2022 |
| `dim_facility_ownership_scd2` | 179 | facility x validity interval | who owned what, when |
| `fct_ownership_change` | 18 | one observed transition | each classified by how far it should be believed |
| `fct_ownership_noise` | 5 | one filter stage | how much noise each stage removed |
| `bridge_plant_xref` | 1,492 | one candidate pair | cross-source links, carrying probability and evidence |

`bridge_plant_xref` is a candidate table, not a match table. It has no `is_match` column
and an assertion fails the build if one appears. Rows below the bar carry no candidate key
at all, so a downstream join cannot pick up a link the model never endorsed.

## Data quality and validation

**54 dbt assertions**, run on every build:

| Kind | Count | Examples |
|---|---:|---|
| `not_null` | 28 | every key and validity column |
| `accepted_values` | 6 | classification and status enums |
| `unique` | 5 | plant and owner keys |
| `relationships` | 3 | facts resolve to their dimensions |
| `unique_combination_of_columns` | 2 | fact grain |
| `accepted_range` | 2 | probabilities in [0, 1] |
| custom | 8 | SCD2 intervals do not overlap; one current row per facility; the bridge carries no verdict; unresolved rows carry no candidate key; sentinels stay distinguishable from real values |

**5 Dagster asset checks** cover what dbt cannot see across assets: bridge row count
reconciles with the linker output, furnace rows reconcile from bronze to mart,
geographic coverage has not regressed against a tracked baseline, ownership noise
filtering is bounded on both sides, and match precision against the human labels.

**362 pytest tests**, including regression tests that pin specific defects found in the
data so a future source version cannot reintroduce them silently.

Known data quality problems, all measured:

- `ZipCode` arrives as float64 and `StCntyFIPS` as int; `6607.0` and `09001` both lose
  their leading zero unless identifiers are read as text.
- `ShutdownYr` is 36% populated, and a float column cannot tell missing from NaN.
- One vintage of the furnace files lost every non-ASCII character to `?`. One name lost
  two characters rather than one, so the damage is not reliably invertible.
- One EAF facility appears 48 times where the clean vintage has 24. Only 2 of the 24
  duplicate pairs are byte-identical; the rest differ by a trailing space in `Owner`.
- `Coordinate accuracy = approximate` does not mean imprecise. One plant whose address
  reads "Pacific Northwest, United States" sits at the geographic centre of the
  contiguous US, which places it in Kansas.
- `PlantStartYr = 9999` is a sentinel in 8 of 89 values, so real coverage is 81 of 197.
- Cook County holds 10 plants across 7 distinct ZIPs, so county and ZIP fail together.
- One pair of candidates shares a county, a ZIP and a city, so no geographic evidence
  separates them.

Thirteen findings in full, including three found because an assertion failed, are in
[docs/data-quality-findings.md](docs/data-quality-findings.md).

## Results

I labelled 192 candidate pairs by hand with the model's score hidden. Precision at
p >= 0.99, by sampling stratum:

| stratum | labelled | TP | FP | FN | TN | precision |
|---|---:|---:|---:|---:|---:|---:|
| `cook_county` | 94 | 1 | 0 | 0 | 93 | 1.000 |
| `low_confidence` | 30 | 0 | 0 | 0 | 30 | n/a |
| `high_confidence` | 27 | 25 | 2 | 0 | 0 | 0.926 |
| `contested` | 20 | 7 | 7 | 0 | 6 | 0.500 |
| `ambiguous` | 10 | 0 | 0 | 4 | 6 | n/a |
| | | 33 | 9 | 4 | 135 | |

The strata are sampled at different rates, three exhaustively and two randomly, so
pooling them into one number describes the queue rather than the data. I do not report a
single headline precision for that reason.

Grouped a different way: outside contested cases the model gets 26 of 28 right (0.929).
On contested cases it gets 7 of 14 (0.500), and seven of the nine false positives in the
whole sample sit there. `contested` means several candidates for one plant scored above
the bar and only one can be right. Taking the top scorer gets 5 of 10 plants right, 2
wrong, and on 3 more the correct answer was to reject every candidate. Two of those
facilities carry identical probabilities across their candidates, so the ordering comes
from row order
([ADR-009](docs/adr/ADR-009-one-to-one-assignment-is-unresolved.md)).

Nothing below p = 0.01 was a match, across 30 labelled pairs.

## Operations

**Rerunning.** `make all` is idempotent. `make clean` deletes `lake/`, which is entirely
rebuildable; `make clean-cache` also drops the cached county boundaries and forces a
re-download.

**A new source release.** Bronze is partitioned by `source_version`, so a new vintage
lands beside the old one instead of overwriting it. Add the directory under `data/raw/`
and materialize that partition. Column renames are absorbed by the ingestion contract; a
missing required column stops the load.

**Scheduling.** There is none, deliberately. The sources are annual file drops, so a
Dagster sensor watches `data/raw/` for a new `source_version` directory and a test
asserts that no schedule exists. A nightly build against unchanged inputs would turn a
stale pipeline green every night
([ADR-008](docs/adr/ADR-008-events-not-schedules.md)).

**Failure handling.** Each stage fails independently and says which. Contract violations
stop ingestion; assertion failures stop the dbt build; asset checks report against a
tracked baseline in `baselines/`, which lives outside `lake/` so `make clean` cannot
erase the reference. The label-dependent precision check reports
`SKIPPED - not evaluable` rather than passing when no labels exist.

**Local versus CI.** CI runs lint, the dbt parse and the full test suite, in which the
data-reading tests skip. The pipeline job is `workflow_dispatch` only, because the sources
are gitignored and a hosted runner never has them. The end-to-end run is verified locally.

## Limitations

1. **The accuracy numbers are within-sample.** They rest on 37 positives; two more errors
   would move 0.926 to 0.852. 11 `unsure` verdicts are excluded rather than classified,
   and 6 of those 11 are in `contested`. One annotator, so no inter-annotator agreement
   is measurable.
2. **The weights are inflated by an unknown amount.** ZIP, street and county are
   geographically correlated by construction, which violates Splink's conditional
   independence assumption. Re-estimating from the labels moved the largest weight by
   0.31, so 37 positives shift the weights without restructuring them.
3. **Three comparison levels have no estimate.** No candidate pair fell into them.
4. **The ownership heuristics are hand-set.** Edit distance and containment thresholds
   with no labels behind them. `fct_ownership_change` marks them `heuristic_unvalidated`
   and names the rule per row.
5. **One-to-one assignment is unsolved.** See Results above and ADR-009.
6. **CI runs only the parts that need no data.** The end-to-end run is verified on my
   machine and nowhere else.
7. **Smaller things:** single machine, every source read fully into memory. The 1:500k
   boundary file puts one waterfront plant 255 m offshore. dlt's column freeze does not
   fire on a table's first load, so Pandera is the primary gate. 8 Canadian plants can
   never match a US census and stay in the denominator.

## Repository layout

```
ingest/          source ingestion: dlt pipelines, contracts, typing
lakehouse/       Iceberg catalog and DuckDB access
geo/             county boundary download and point-in-polygon
resolution/      Splink blocking, comparisons, scoring
labeling/        Streamlit review UI, stratified sampler, evaluation
transform/       dbt: 5 staging views, 10 marts, 54 assertions
orchestration/   Dagster assets, asset checks, file-arrival sensor
baselines/       tracked reference values for the drift checks
tests/           362 pytest tests
docs/adr/        nine architecture decision records
data/raw/        the four sources, gitignored
```

## Further documentation

- [Architecture decision records](docs/adr/README.md), nine decisions with an index.
- [Data quality findings](docs/data-quality-findings.md), thirteen measured defects.
