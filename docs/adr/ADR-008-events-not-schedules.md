# ADR-008: An event, a tracked baseline, and no cron

- **Status**: accepted
- **Date**: 2026-09-19
- **Deciders**: project author

## No schedule

The reflex at this point is a daily or weekly cron. Here it would be theatre.

Every source is an annual file drop: GSPT publishes its tracker once a year, and the
furnace census arrives as a recompiled vintage. A timer would re-read the same four
files on 364 days out of 365, produce byte-identical output, and fill the run history
with green ticks that carry no information.

The second-order effect is worse. A schedule makes a stale pipeline look healthy: "it
ran this morning" reads as "the data is current", and those are different claims. The
run history would show freshness the data does not have.

**So there is no schedule. There is a sensor** watching `data/raw/source_version=*` for a
vintage nobody has processed, with a cursor so a restart does not replay history.
Between vintages nothing runs, which is the right amount of work when nothing changed.

It is defined with `DefaultSensorStatus.STOPPED`. Starting a daemon that watches a
directory should be a deliberate act by whoever operates the repository, not a side
effect of importing a module.

Knowing when *not* to add scheduling is part of the judgement this project is meant to
show.

## The drift baseline is tracked, not a build artifact

`orchestration/checks.py` compares geographic coverage against a recorded best. That
reference used to live in `lake/check_baseline.json`.

`lake/` is gitignored build output that `make clean` deletes. A baseline stored there
disappears with one clean, and the next run re-establishes it **from whatever the
pipeline just produced** — including a degraded run — and reports success. A reference
that the thing it measures can erase is not a reference.

It now lives in `baselines/check_baseline.json`, which is tracked. A change to it shows
up in a diff and gets looked at.

## `furnace_years` stays one partitioned asset

The two vintages are not independent. `gspt_2024` has intact facility names and no
owners; `owner_filled_2025` has every owner and lost every non-ASCII character. ADR-003
joins them on `(furnace_type, id, year, fid)` so each row can carry the intact name and
the owner together — which means **they have to be read together**. Splitting them into
two assets would break the alignment that makes either usable.

So the partition records **which vintage a run is *about*, not which vintage it reads**.
The loader writes both in one pass; the partition key selects what the run's metadata
reports on. This is a deliberate departure from the usual meaning of a partition and is
stated here so nobody infers isolation that does not exist.

## `fct_ownership_change` is not hidden from consumers

The tempting move is to keep a table full of unvalidated heuristics out of the default
consumer view until labels exist.

That would repeat the failure this project keeps arguing against: silent absence. A
consumer who cannot see the table does not conclude "this is uncertain", they conclude
the data does not exist, and the uncertainty becomes invisible rather than visible.

The table stays. `classification_confidence` marks every row as `derived` or
`heuristic_unvalidated`, `classification_rule` names the exact threshold that fired, and
the README says the transfers are candidates rather than facts. Uncertainty travels with
the row, where a consumer will actually meet it.
