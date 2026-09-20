# ADR-007: Orchestration reports dbt's results; it does not restate them

- **Status**: accepted
- **Date**: 2026-09-19
- **Deciders**: project author

## Context

By P6 there were already 48 dbt assertions. Adding Dagster raised an obvious question:
should those become Dagster asset checks too, so the platform shows them natively?

## Decision

**No. There is exactly one definition of each rule.** `dbt_assets` runs `dbt build` and
streams dbt's own results into Dagster, so every one of the 48 appears as an asset check
in the UI without being written twice. Two copies of an invariant drift, and once they
disagree nobody can tell which is authoritative.

Dagster-native checks are reserved for the three things dbt genuinely cannot see.

### 1. Cross-asset arithmetic

dbt can test `bridge_plant_xref` on its own. It cannot check that the bridge holds one
row per linker candidate plus one per never-compared plant, because the linker runs
outside dbt's world. Measured: 1,484 + 8 = 1,492.

Likewise the furnace reconciliation — bronze 4,352, minus 2,153 superseded vintage,
minus 22 unkeyable South American rows, minus 24 EAF:79 repeats, equals 2,153 in the
fact. An unexplained change means a filter widened silently, and no single-model test
would notice.

### 2. Comparisons against the previous run

A suite that passes on today's data says nothing about whether coverage just collapsed.
`geographic_coverage_has_not_regressed` compares county assignment against the previous
best and fails on any drop.

`ownership_noise_filtering_is_bounded_on_both_sides` bounds the rejected share from
**both** directions. A lower bound alone catches filters that stopped working; it calls
filters that have started eating real transfers perfectly healthy. Currently 14 of 18
transitions are rejected (0.78) against a band of 0.30–0.95. Both bounds are hand-set
tripwires and the check says so in its own metadata.

### 3. Checks that cannot run yet

`match_precision_against_human_labels` **does not pass**. The label set is empty, so
precision is undefined, and it reports `passed=False`, severity `WARN`, with
`status: "SKIPPED - not evaluable"` and the reason spelled out.

Returning `passed=True` would have been easy and would have been a lie: it would put a
green tick in the same column as a measured result, and the distinction between "we
checked and it was fine" and "we could not check" is exactly what this project refuses
to blur. The check is `blocking=False`, so an unmeasurable property does not stop the
pipeline either.

## Two structural choices

**`duckdb_lake_views` is an asset, not a resource.** Iceberg metadata locations move on
every load, so the views must be rebuilt every run; that is real work that can really
fail. As a resource its failure would be a stack trace during initialisation with no
position in the lineage.

**dbt's sources are mapped onto those view assets, one for one.** By default dagster-dbt
maps the dbt source `raw.plantgen` to the asset key `["raw", "plantgen"]` — which is
also the bronze asset's key, so the graph would read `bronze → dbt` and dbt would be
free to run alongside the bootstrap it depends on. That lineage is also false: dbt reads
DuckDB views, not Iceberg. Mapping each source to `["views", ...]` gives
`bronze → view → dbt model`, which both orders the run correctly and describes what
actually happens.

## Consequences

- `dagster asset materialize --select '*'` runs the whole pipeline: 4 bronze assets, geo,
  resolution, 6 view assets, 14 dbt models, 48 dbt assertions and 5 Dagster checks.
- Only the furnace census is partitioned, by `source_version`, because it is the only
  asset that genuinely has two vintages. Partitioning the others by a dimension they do
  not have would be decoration.
- The drift baseline lives in `lake/check_baseline.json`, which `make clean` deletes. The
  next run then re-establishes it and reports WARN rather than comparing — so the check
  protects between runs, not across a full wipe. Stated rather than hidden.
- Four degradation tests break the warehouse on a copy and require each check to go red,
  including both sides of the ownership band.
