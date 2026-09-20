# ADR-006: A mart may not state a conclusion the pipeline has not earned

- **Status**: accepted
- **Date**: 2026-09-19
- **Deciders**: project author

## Context

A mart is where people stop reading the code. Whatever a column says at this layer is
what downstream dashboards, joins and decisions will treat as fact, and the reasoning
behind it is three hops away. That makes the mart layer the place where an unearned
conclusion does the most damage and is hardest to notice.

Three conclusions were available to state here, and none of them has been earned.

## Decision 1: `bridge_plant_xref` carries no verdict

**There is no `is_match` column, and a test fails the build if one appears.**

Zero human labels exist. No threshold has been chosen. And ADR-004 recorded that the
linker's `zip5`, `street` and `county` comparators are geographically correlated by
construction, which violates conditional independence and inflates exactly the confident
weights a threshold would be drawn against. Binarising in that state would be asserting
correctness on no evidence — with the appearance of rigour, because the number came out
of a probabilistic model.

The table carries `match_probability`, the six-part evidence vector, `blocking_reason`,
and `resolution_status ∈ {candidate, never_compared}`.

`never_compared` is a separate state on purpose. The 8 Canadian GSPT plants have no
counterpart in PLANTGEN — it is a US census — so blocking never reached them. Recording
that as a low probability would put them in an accuracy denominator as failures of the
model rather than as absences from the reference data.

**An unresolved row carries no candidate key.** A `never_compared` row has
`plantgen_plant_key` null, enforced by an assertion. If it carried one, a downstream
join would pick up a link the model never endorsed, and one hop later nothing would
show where that link came from.

## Decision 2: a changed string is not a changed owner

`fct_ownership_change` classifies every ownership transition instead of counting them.

Diffing raw owner strings finds 10 changes across 18 span boundaries. Every one of those
numbers is wrong as a count of ownership transfers:

| class | n | what it actually is |
|---|---:|---|
| `observation_gap` | 8 | the owner did not change; the facility vanished from the data for a year |
| `likely_typo` | 2 | `JSW Steel USA` → `JSW Stee USA` → `JSW Steel USA`: one dropped letter, two fake transfers |
| `name_refinement` | 2 | `ArcelorMittal` ↔ `ArcelorMittal Dofasco Inc.`: same owner, different precision |
| `oscillation` | 2 | A → B → A within two years: far likelier to be inconsistent entry than two deals |
| `credible_transfer` | **4** | what survives |

Normalisation happens before the diff — 87 distinct raw owner strings collapse to 83 —
because comparing raw text measures formatting churn. The 48 encoding-damaged rows
(`Standard Steel ? Burnham`, `North American H?an?`) have no intact counterpart anywhere,
since the vintage carrying owners is the vintage that was damaged; the `?` is stripped as
punctuation so they normalise stably rather than flickering, and spans built from them
are flagged.

Even `credible_transfer` is a claim about a free-text column, not a verified corporate
action. The column is named for what it is.

## Decision 3: "reported as unknown" and "never reported" stay different

GSPT writes its non-answers into the value column: `unknown`, `>0`, `N/A`. Every
sentinel-bearing column becomes a value plus a `_status` in
`{reported, unknown, bounded, unparsed, absent}`.

`unknown` means the tracker looked and could not find out. `absent` means there is no
row. Collapsing both to null erases the difference between a gap in the world's
knowledge and a gap in the dataset, and nothing downstream could recover it.

PLANTGEN's own sentinel is `9999` in `PlantStartYr` — 8 of its 89 populated values. Left
alone it reads as a year and turns an absence into a disagreement.

## What the assertions are for

A grain assertion nobody has watched fail is a comment.
`tests/test_transform.py::TestAssertionsActuallyFail` breaks the data three ways on a
copy of the database and requires `dbt build` to go red each time: a duplicated source
row, a duplicated fact row, and an overlapping SCD2 interval. A fourth rewrites
`bridge_plant_xref` to add `is_match` and requires the build to reject it.

The first of those found a real defect while being written. `eaf_owner_filled.csv`
contains facility EAF:79 (the facility) **48 times where it should
appear 24** — every row duplicated on the following source line, byte-identical including
`Data_ID`, and no other facility affected. The other vintage settles it: `eaf_mini.csv`
holds exactly 24. So staging deduplicates, and a guard pins the number of rows the dedup
is allowed to remove, so that a future vintage duplicating something *else* fails the
build instead of disappearing into a `qualify`.

## Consequences

- Scoping is reconcilable: 4,352 bronze furnace-years − 2,153 (superseded vintage)
  − 22 (South American, unkeyable) − 24 (exact duplicates) = 2,153 in the fact.
- Filters live in staging, never in bronze, so every scope decision is visible in one
  file and reversible.
- Nobody can read a match out of this warehouse, because there is not one in it yet.
