# ADR-001: A local lakehouse, not a distributed stack

- **Status**: accepted
- **Date**: 2026-09-18
- **Deciders**: project author

## Context

The project integrates four sources describing North American steel plants:

| Source | Rows | Grain |
|---|---|---|
| `PLANTGEN.csv` | 197 | plant |
| GSPT `Steel Plants` | 1,848 global / 111 North American | plant x production unit |
| GSPT `Yearly Production` | 12,409 after unpivot | plant x year x metric |
| BOF + EAF furnace records | 4,352 across two vintages | furnace x year |

Around 18,800 rows in total; the largest single input file is a 1 MB workbook. The
whole set fits in a laptop's memory several hundred times over.

The hard part of this project is not moving the data. It is that **the four sources
share no key**. Deciding that PLANTGEN row 156 and GSPT plant `P100000121251` are the
same physical plant requires probabilistic matching over coordinates, county FIPS,
city names and owner names, and that decision has to be auditable: for each pair, what
evidence supported it, what probability was assigned, and who confirmed it.

## Decision

Build the whole thing locally: Iceberg tables on the filesystem with a SQLite catalog,
DuckDB as the query engine, dbt for transformation, Dagster for orchestration, Splink
for entity resolution.

Do **not** use Spark, Kafka, Airflow, or a cloud warehouse.

## Rationale

**Spark.** Spark's value is partitioning work across machines. At 18,800 rows its
scheduler, serialization and JVM startup cost more than the computation. A DuckDB
aggregation over this data finishes in milliseconds; the same query through Spark
spends longer planning than DuckDB spends answering. Introducing it here would
demonstrate familiarity with a tool while demonstrating poor judgement about when to
reach for it, which is the opposite of what a portfolio should show.

**Kafka.** Every source is a periodic file drop: an annual tracker release, a
recompiled furnace census. There is no stream. A queue between a file and a table adds
a component that can lose messages, in exchange for nothing.

**Airflow.** The pipeline is a small DAG of assets whose correctness properties matter
more than their scheduling. Dagster's software-defined assets and asset checks express
"this table exists, and here is what must be true about it" directly; Airflow expresses
"this task ran". For a project whose whole argument is about correctness under
ambiguity, asset checks are the feature that earns its place.

**Cloud warehouse.** A hard requirement of this project is zero cost and zero cloud
credentials: a reviewer must be able to clone the repo and run `make all`. A managed
warehouse would break that, and it would buy scale that is not needed.

## Why a lakehouse at all, then?

If the data is small, why not a single DuckDB file?

Because two properties of the table format are load-bearing here, and neither is about
size:

1. **Snapshots.** Entity resolution is iterative. A model is trained, a threshold is
   chosen, labels are added, the model is retrained. Being able to say "this mart was
   built from this snapshot of the bronze tables" is what makes a match result
   reproducible and a regression explicable. Iceberg gives that for free.
2. **Schema evolution as a first-class operation.** This is not hypothetical here: the
   same furnace records ship under two different column-name spellings (`Corrected C/L`
   and `Corrected.C.L`), and both must land in one table. A format with an explicit
   schema and an explicit evolution story is the right home for that.

Iceberg on a local filesystem with a SQLite catalog gives both at zero operational cost.

## Consequences

- A reviewer can clone, run `make all`, and get the full pipeline with no accounts.
- The engineering effort goes where the difficulty is: matching, evidence, labelling.
- If the data grew by four orders of magnitude, the Iceberg tables would already be in
  a format Spark, Trino or Snowflake can read. The decision is reversible; that is part
  of why the table format was worth having.
- The pipeline is single-machine. It cannot survive an input that does not fit in RAM.
  That limit is accepted knowingly and is documented in the README.
