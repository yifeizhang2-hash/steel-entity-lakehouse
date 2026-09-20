# steel-entity-lakehouse

Four datasets describe North American steel plants. None of them share an ID. This
project reads all four into a local lakehouse, works out which rows refer to the same
physical plant, and builds a dimensional model on top.

I labelled 192 candidate pairs by hand with the model's score hidden, which is where the
numbers below come from.

## Results

Precision at p >= 0.99, by sampling stratum:

| stratum | labelled | TP | FP | FN | TN | precision |
|---|---:|---:|---:|---:|---:|---:|
| `cook_county` | 94 | 1 | 0 | 0 | 93 | 1.000 |
| `low_confidence` | 30 | 0 | 0 | 0 | 30 | — |
| `high_confidence` | 27 | 25 | 2 | 0 | 0 | 0.926 |
| `contested` | 20 | 7 | 7 | 0 | 6 | 0.500 |
| `ambiguous` | 10 | 0 | 0 | 4 | 6 | — |
| | | 33 | 9 | 4 | 135 | |

The strata are sampled at different rates, three exhaustively and two randomly, so
pooling them into one number describes the queue rather than the data. I do not report a
single headline precision for that reason.

Grouped a different way: outside contested cases the model gets 26 of 28 right (0.929).
On contested cases it gets 7 of 14 (0.500). Seven of the nine false positives in the
whole sample sit in that second group.

`contested` means the model put several candidates for one plant above the bar, and only
one can be right. Taking the top scorer gets 5 of 10 plants right, 2 wrong, and on 3 more
the correct answer was to reject every candidate. Two of those facilities carry identical
probabilities across their candidates, so the ordering comes from row order
([ADR-009](docs/adr/ADR-009-one-to-one-assignment-is-unresolved.md)).

Nothing below p = 0.01 was a match, across 30 labelled pairs.

## Data

The source files are not in this repository. The Global Steel Plant Tracker is published
by [Global Energy Monitor](https://globalenergymonitor.org/projects/global-steel-plant-tracker/)
under its own terms, and the other three are not mine to pass on either. `data/raw/` is
gitignored. To run the pipeline, place the sources like this:

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

Without them, linting and the schema tests still run; every test that reads a source
skips itself. The project downloads one file, the Census county boundary shapefile
(11.6 MB, cached in `geo/cache/`). `make geo-offline` fails instead of downloading.

## Quick start

```
make install     # uv sync + pre-commit hooks
make lint test   # ruff + the full pytest suite
make ingest      # build the four bronze Iceberg tables under lake/
make geo         # attach county FIPS (downloads boundaries once, then caches)
make resolve     # train the linker; print blocking, calibration and weights
make label       # open the Streamlit review UI
make evaluate    # precision/recall per stratum against the 192 labels
make dbt         # build and test the dimensional model
make materialize # run the whole graph through Dagster, with asset checks
make all         # all of the above
```

`make clean && make all` from a clean checkout, about 90 seconds:

```
All checks passed!                              # ruff check
65 files already formatted                      # ruff format --check
plantgen         rows=    197
gspt_plants      rows=   1848
gspt_production  rows=  12409
furnace_years    rows=   4352
GSPT -> county FIPS coverage
  North American GSPT plants                       93
    with usable coordinates                        92
  assigned a county FIPS                           83
    county present in PLANTGEN                     68
  full cross join                                18321
  candidate pairs after blocking                  1484
  reduction ratio (fallback included)           0.9190
Done. PASS=69 WARN=0 ERROR=0 TOTAL=69           # dbt: 14 models, 48 assertions
362 passed                                      # pytest
```

## How it works

**Ingestion.** Four dlt pipelines land the sources as Iceberg tables, read with Polars
under explicit schema overrides. Identifier columns are locked to `Utf8` at read, because
pandas and Polars both infer `ZipCode` as a float and cleaning punctuation off `6607.0`
produces a valid-looking wrong ZIP. Low-coverage columns keep a real null, so a missing
`ShutdownYr` stays distinguishable from a NaN.

**Geography.** GSPT carries coordinates for 92 of 93 North American plants; PLANTGEN
carries `StCntyFIPS` for all 197. Reverse-geocoding the coordinates against Census county
boundaries gives the only link covered on both sides. ZIP is shared on 43 values and the
state vocabularies do not overlap at all, since one spells `Alabama` and the other `AL`.

**Resolution.** Splink over DuckDB, blocked on county FIPS, ZIP and normalised state,
which cuts 18,321 possible pairs to 1,484. The weights come from EM rather than from me.
Three comparison levels had no pairs to learn from and are left unset rather than filled
in with a plausible number. I compare the query name against the candidate's city as
well as its name, because PLANTGEN's plant name is its city for 85% of rows.

**Labelling.** The queue is stratified, not ranked. 30 pairs below p = 0.01 are the only
way to measure recall, and a queue drawn from the top would give a precision figure with
nothing to divide it by. The Cook County cohort is exhaustive because county and ZIP both
fail there. The UI records whether the model's score was visible; across all 192
decisions it was hidden.

**Model.** Five staging views and nine marts in dbt, with 48 assertions. Grain came out
of the data: GSPT's `Plant ID` covers 93 plants across 111 rows, so plants and production
units are separate dimensions, and `facility_id` is unique only within a furnace type.
`bridge_plant_xref` carries a probability and an evidence vector, and no `is_match`
column. A test greps for one.

**Orchestration.** Dagster assets partitioned by `source_version`, surfacing dbt's 48
assertions rather than restating them, plus five checks covering what dbt cannot see
across assets. A file sensor watches `data/raw/` for a new vintage. There is no schedule,
and a test asserts that.

## What the data turned out to contain

- `ZipCode` arrives as float64. `6607.0` loses its leading zero unless identifiers are
  read as text.
- `StCntyFIPS` has the same problem: `09001` becomes `9001`.
- `ShutdownYr` is 36% populated, and pandas cannot tell its missing values from NaN.
- The furnace CSVs ship in two vintages with different column names, `Corrected C/L`
  against `Corrected.C.L`, because one branch passed through R's `make.names()`.
- `owner_filled_2025` lost every non-ASCII character to `?`. Hoganas lost two characters,
  not one, so the damage is not reliably invertible.
- EAF facility 79 appears 48 times where the clean vintage has 24. Only 2 of the 24
  duplicate pairs are byte-identical; the other 22 differ by a trailing space in `Owner`.
- `Coordinate accuracy = approximate` does not mean imprecise. Nucor Steel Pacific
  Northwest, whose address reads "Pacific Northwest, United States", sits at
  37.0902, -95.7129, the geographic centre of the contiguous US.
- Cook County holds 10 plants across 7 distinct ZIPs, so county and ZIP fail together.
- Cleveland-Cliffs Indiana Harbor and PLANTGEN's East Chicago share a county, a ZIP and a
  city. No geographic evidence separates them.
- `PlantStartYr = 9999` is a sentinel in 8 of 89 values, so real coverage is 81 of 197.

Thirteen findings in full, including three found because an assertion failed, are in
[docs/data-quality-findings.md](docs/data-quality-findings.md).

## Limitations

1. **The accuracy numbers are within-sample.** They rest on 37 positives; two more errors
   would move 0.926 to 0.852. 11 `unsure` verdicts are excluded rather than classified,
   and 6 of those 11 are in `contested`. One annotator, so no inter-annotator agreement
   is measurable.
2. **The weights are inflated by an unknown amount.** `zip5`, `street` and `county` are
   geographically correlated by construction, which violates Splink's conditional
   independence assumption. Re-estimating from the labels moved the largest weight by
   0.31, so 37 positives shift the weights without restructuring them.
3. **Three comparison levels have no estimate.** No candidate pair fell into them.
4. **The ownership heuristics are hand-set.** Edit distance and containment thresholds,
   with no labels behind them. `fct_ownership_change` marks them
   `heuristic_unvalidated` and names the rule per row.
5. **One-to-one assignment is unsolved.** See Results above and ADR-009.
6. **CI runs the parts that need no data.** The `check` job lints, parses the dbt project
   and runs the suite, in which the data-reading tests skip. The `pipeline` job is
   `workflow_dispatch` only, because the sources are gitignored and a hosted runner never
   has them. The end-to-end run is verified on my machine and nowhere else.
7. **Smaller things:** single machine, every source read fully into memory. The 1:500k
   boundary file puts Indiana Harbor 255 m into Lake Michigan. dlt's column freeze does
   not fire on a table's first load, so Pandera is the primary gate. 8 Canadian plants
   can never match a US census and stay in the denominator.

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
tests/                   362 pytest tests
docs/adr/                nine architecture decision records
data/raw/                the four sources, gitignored (see Data above)
```

## Documentation

- [Architecture decision records](docs/adr/README.md), nine decisions with an index.
- [Data quality findings](docs/data-quality-findings.md), thirteen measured defects.
