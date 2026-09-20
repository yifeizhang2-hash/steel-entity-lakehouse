# Architecture decision records

Each record answers one question the data forced, and records what was measured rather
than what seemed reasonable. They are worth reading in order: the later ones lean on
findings established in the earlier ones.

| # | Decision | The question it answers |
|---|---|---|
| [001](ADR-001-local-lakehouse-not-spark.md) | A local lakehouse, not a distributed stack | Why 18,800 rows do not justify Spark, Kafka or Airflow — and why a *table format* still earns its place at that size |
| [002](ADR-002-dataframe-boundary.md) | Two DataFrame libraries, separated by storage | Why Polars is confined to `ingest/`, why the reason is **not** performance, and how the boundary is enforced by a test that parses imports |
| [003](ADR-003-encoding-degradation.md) | Encoding damage is detected and aligned around, never repaired | Why `North American H?an?` cannot be turned back into `Höganäs`, and why the two damaged vintages are combined by joining rather than by editing strings |
| [004](ADR-004-probabilistic-matching.md) | Weights are estimated, not chosen | Why no match weight is hand-set, what happens to a comparator that fires on 0 of 18,321 pairs, and why a weights table without pair counts cannot be read |
| [005](ADR-005-labels-are-not-generated.md) | Labels are human, sampled deliberately, audited for anchoring | Why no threshold has been set, why the low-probability stratum is the one that must not be skipped, and why the UI hides the model's opinion by default |
| [006](ADR-006-marts-carry-no-unearned-conclusions.md) | A mart may not state a conclusion the pipeline has not earned | Why `bridge_plant_xref` has no `is_match` column, why a changed string is not a changed owner, and why "reported as unknown" stays distinct from "never reported" |
| [007](ADR-007-orchestration-checks-do-not-mirror-dbt.md) | Orchestration reports dbt's results; it does not restate them | Why the 48 dbt assertions are not duplicated as Dagster checks, and why a check that cannot run reports a skip rather than a pass |
| [008](ADR-008-events-not-schedules.md) | An event, a tracked baseline, and no cron | Why a daily schedule would be theatre on annual file drops, and why a baseline stored in gitignored build output is not a baseline |
| [009](ADR-009-one-to-one-assignment-is-unresolved.md) | One-to-one assignment is unresolved, and left to a human | What 192 human labels revealed: the model scores at 0.93 and assigns at 0.50, and why a Hungarian matcher would turn an unknown into a confident wrong answer |

## The thread running through them

Every one of these is the same decision in a different costume: **do not present a
conclusion as more certain than the evidence supports.**

- ADR-003 refuses to guess a character that was destroyed.
- ADR-004 refuses to invent a weight EM could not estimate.
- ADR-005 refuses to accept a match without a label.
- ADR-006 refuses to binarise a probability into a verdict.
- ADR-007 refuses to show a green tick for a measurement nobody took.
- ADR-008 refuses to let a timer imply freshness the data does not have.
- ADR-009 refuses to break a 0.0002 tie that the model has no basis to break.
