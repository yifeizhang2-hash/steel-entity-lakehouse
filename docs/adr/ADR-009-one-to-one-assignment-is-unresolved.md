# ADR-009: One-to-one assignment is an open problem, and is left to a human

- **Status**: accepted
- **Date**: 2026-09-20
- **Deciders**: project author

## What the labels showed

192 pairs were labelled by one annotator with the model's probability hidden throughout.
Split by stratum, at a threshold of 0.99:

| stratum | labelled | TP | FP | FN | TN | precision |
|---|---:|---:|---:|---:|---:|---:|
| `cook_county` | 94 | 1 | 0 | 0 | 93 | **1.000** |
| `low_confidence` | 30 | 0 | 0 | 0 | 30 | — |
| `high_confidence` | 27 | 25 | 2 | 0 | 0 | **0.926** |
| `contested` | 20 | 7 | 7 | 0 | 6 | **0.500** |
| `ambiguous` | 10 | 0 | 0 | 4 | 6 | — |

Collapsed to the split that explains it:

```
not contested    28 predicted positive,  26 correct   precision 0.929
contested        14 predicted positive,   7 correct   precision 0.500
```

**Seven of the nine false positives in the entire labelled sample are in `contested`.**

## The finding

**The model's scoring is good. Its one-to-one assignment is a coin flip.**

This is structural, not a defect. `contested` is *defined* as the model putting several
candidates for one plant above 0.99. At most one of them can be the same physical plant,
so if the model cannot choose, precision converges on 1/k for k candidates. With two
candidates that is 0.5, and 7/14 is exactly what was measured.

The model is not failing to recognise a match. It is recognising several and having no
basis to prefer one.

## What an assignment algorithm would actually have done

Pair-level precision is suggestive; the decisive measurement is at **facility** grain.
Taking each contested GSPT plant's highest-scoring candidate — what any greedy or
Hungarian assignment reduces to when the alternatives are this close — and comparing it
against the human verdict:

| GSPT plant | cands | gap | taking the top score |
|---|---:|---:|---|
| Cleveland-Cliffs Cleveland | 3 | **0.000000** | ✗ wrong |
| Nucor Steel Seattle | 3 | **0.000000** | ✓ right |
| CMC Alabama | 3 | 0.000030 | **human: none of these** |
| Cleveland-Cliffs Indiana Harbor | 2 | 0.000034 | ✓ right |
| Timken Faircrest | 2 | 0.000438 | ✗ wrong |
| Timken Harrison | 2 | 0.000580 | ✓ right |
| Nucor Steel Birmingham | 3 | 0.000780 | ✓ right |
| Finkl Steel Chicago | 4 | 0.003309 | **human: none of these** |
| Vallourec Star Youngstown | 2 | 0.024449 | ✓ right |
| Republic Steel Canton | 2 | 0.026600 | **human: none of these** |

**5 right, 2 wrong, 3 structurally unanswerable — 50%**, arriving at the same figure as
the pair-level count by a different route.

Three facts from this table carry the decision.

### 1. Two of the ties are exact, not close

`Cleveland-Cliffs Cleveland` and `Nucor Steel Seattle` have a gap of **literally
`0.000000`** — and not through rounding. The candidates carry **bit-identical
probabilities and identical evidence vectors**:

```
Cleveland-Cliffs Cleveland steel plant
  candidate A
            p=0.9997464585746214   zip=0 street=0 place=3 county=3   no_match
  candidate B
            p=0.9997464585746214   zip=0 street=0 place=3 county=3   MATCH
  candidate C
            p=0.9997464585746214   zip=0 street=0 place=3 county=3   no_match
```

Every compared field agrees identically across all three. **The ordering is decided by
row order, not by evidence.** The model is not uncertain here in the way a 0.6 score is
uncertain — it has *no information whatsoever* to separate these records, and it says so
correctly by scoring them the same. An assignment algorithm would take whichever row the
warehouse returned first and report the result with full confidence. One of these two
plants guesses right and the other guesses wrong, which is what guessing looks like.

### 2. The answer is "none of these" three times out of ten — and assignment cannot say that

This is the root objection, and it is more fundamental than any weighting problem.

On `CMC Alabama`, `Finkl Steel Chicago` and `Republic Steel Canton` the human's verdict
was that **none** of the candidates is the same plant. Finkl had four candidates above
the bar and the right answer was to reject all four.

**An assignment algorithm cannot express "none of the above."** Greedy, Hungarian,
optimal-transport — they all consume a score matrix and emit a pairing. Given a row of
confident candidates, every one of them returns one. Not because it is badly tuned, but
because *selecting an element* is the only operation it has. Its output type has no
value meaning "reject this row".

So on 3 of 10 facilities it is not choosing wrongly among plausible options; it is
**forced to produce a confident wrong answer where the correct output is silence.** No
amount of recalibration changes that, because it is a property of the operation, not of
the numbers fed into it.

Human review has the null option natively: the UI's "None of these is a match" button
writes `no_match` for every candidate at once, and it was used three times here.

### 3. Indiana Harbor chose correctly — and that is not reassurance

An earlier version of this record used Indiana Harbor as the example of assignment
picking wrongly. **That was wrong, and the correction matters.** Its two candidates are
separated by 0.000034 and the human chose `East Chicago` — which *is* the higher-scoring
one. Taking the top score would have been right.

It is kept in the table as a right answer, because presenting it as a failure would be
using a false example to argue a true conclusion. What it actually demonstrates is the
weaker and more useful point: **a coin landing the right way up is not evidence that
flipping it is a method.** Four of the five correct answers above have gaps under
0.001 on weights known to be inflated by correlated comparators (ADR-004). They are not
right for a reason the pipeline could rely on again.

## Decision

**Leave assignment unsolved and route contested records to a human. Do not implement a
matcher.**

This was already the rule before there were labels, and the labels turned it from a
precaution into a measurement: **greedy one-to-one assignment would have been 50% correct
on exactly the pairs where it was applied.** A rule that is right half the time, applied
silently, is worse than no rule, because the output looks the same either way.

## The two paths, and why neither is taken now

**Global optimal assignment (Hungarian / maximum-weight bipartite matching).** Treat the
1,484 scored pairs as a bipartite graph and take the assignment maximising total weight
under a one-to-one constraint. This is the textbook answer and it is cheap at 93 × 197.

It is not taken for two reasons, in order of severity.

**It cannot represent the right answer on 30% of these facilities.** Three of the ten
need "none of these", and a matcher's output type has no such value. A global optimum
would improve on greedy where the choice is between plausible candidates, and would make
no difference at all where the correct output is a rejection.

**Where it does choose, it would break ties that carry no information.** Two of the ten
have bit-identical scores across candidates with identical evidence vectors; four more
are separated by under 0.001 on weights known to be inflated by correlated geographic
comparators. A global optimum resolves those by row order dressed as optimisation, and
presents the pick with the same face as a well-separated match. It converts an *unknown*
into a *wrong answer that looks decided*.

Worse, it would make the error invisible. Today a contested record is visibly contested:
`resolution_status = 'candidate'` on several rows, all carried into `bridge_plant_xref`.
After assignment there would be one row and nothing marking that a tie was broken.

**Keep contested records in human review.** This is the current behaviour. The labelling
UI shows every candidate for a record side by side, choosing one writes `no_match` for
its rivals, and `bridge_plant_xref` carries no verdict column at all (ADR-006).

## When assignment would become the right move

Not on more data — more rows will not separate two plants in the same ZIP. It becomes
reasonable when the model can *justify* a preference:

1. The conditional-independence violation is corrected, so a 0.0002 gap means something.
2. Evidence exists that actually distinguishes same-ZIP plants — street address on both
   sides (PLANTGEN has it on 126/197), or a signal neither source carries today. Two
   reference records sharing a name, a city and a ZIP cannot be separated by any field
   currently compared.
3. The output can express rejection, so that "none of these" stops being unrepresentable.
4. There are enough labelled contested pairs to measure whether assignment beats 0.5.
   There are 14 pairs across 10 facilities. That is not enough to demonstrate anything.

Until then, the honest output of a contested record is two candidates and a person.

## Consequences

- `bridge_plant_xref` continues to carry candidates, never an assignment.
- The `match_precision_against_human_labels` asset check gates on the `high_confidence`
  stratum only. `contested` is measured and reported alongside it but **does not gate**:
  a near-0.5 precision there is the arithmetic of an unsolved assignment problem, and
  gating on it would fail the check forever for a reason no threshold can fix.
- The 192-pair labelling budget bought this finding. A queue drawn only from the top of
  the ranking would have reported ~0.93 and missed it entirely, because contested pairs
  *are* top-of-ranking pairs.
- `tests/test_labeling.py::TestContestedAssignmentIsUnresolved` pins the existence of
  zero-gap contested groups. If a future change makes every gap non-zero, the most
  likely cause is an artificial tiebreak having been introduced somewhere, not the model
  having got better at separating identical records.
