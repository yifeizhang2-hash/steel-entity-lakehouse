# ADR-005: The label set is built by humans, sampled deliberately, and audited for anchoring

- **Status**: accepted
- **Date**: 2026-09-19
- **Deciders**: project author

## Context

P3 produced 1,484 scored candidate pairs and no way to know whether any of them is
right. The model's own probability cannot answer that: it is the thing under test.

Three tempting shortcuts, each of which produces a number that looks like accuracy and
is not:

1. **Accept everything above 0.99 and call it a match.** With zero labels this asserts
   correctness on no evidence — the exact failure this project exists to avoid. It is
   worse here than usual, because P3 documented that `zip5`, `street` and `county` are
   geographically correlated by construction, which violates the model's conditional
   independence assumption and *inflates* confident weights. A threshold drawn on top of
   a known-inflated score is unreliable twice over.
2. **Label the top of the ranking.** Precision comes out computable and recall does not.
   With no labels below the threshold there are no false negatives to find, so recall
   reads 1.0 and means nothing.
3. **Have the machine label its own candidates.** Every metric then comes out at 1.0 by
   construction, and the resulting file is indistinguishable from human judgement to
   everything downstream.

## Decision

**No threshold is set and no pair is accepted anywhere in this repository.** A threshold
is chosen from a PR curve built on real labels, in a later phase.

**The labels are written by a person.** This repository ships the decision log empty, and
`tests/test_labeling.py` fails if it is not.

**Sampling is stratified, and two of the five strata exist purely to make the metrics
honest.** The queue is 192 pairs:

| stratum | pairs | exhaustive | why it is in the queue |
|---|---:|---|---|
| ambiguous `[0.1, 0.9)` | 12 | yes | most information per label |
| contested | 26 | yes | competing confident claims on one record |
| Cook County | 94 | yes | county and ZIP both fail here |
| `≥ 0.99` | 30 of 45 | no | false positives |
| `< 0.01` | 30 of 1,298 | no | false negatives, and therefore recall at all |

Cook County is exhaustive rather than sampled because it is the one place where the two
geographic signals fail together: 10 plants, 7 distinct ZIPs, two ZIP collisions and one
plant with no ZIP. Labelling it completely is the only way to get a recall number for a
hard region that is not an extrapolation.

**One-to-many conflicts go to a human intact.** No greedy 1:1 assignment. Cleveland-
Cliffs Indiana Harbor reaches two PLANTGEN plants sharing its county, its ZIP *and* its
city; a tie-break rule would hide precisely the case a person is needed for.

**Low-support weights are kept and flagged, not deleted.** `county_near_500m` rests on
three pairs — and it is the level that puts Indiana Harbor in the candidate set at all.
Removing it would quietly undo a decision that demonstrably works. The report prints it
as `+7.41 (low support, n=3)` so nobody reads it as established.

## Anchoring

A labelling UI that shows the model's verdict prominently collects agreement, not
judgement, and launders the model's errors into ground truth.

So: the probability and the evidence vector are collapsed, placed below the records and
away from the buttons, and hidden by default. **Whether they were revealed is stored with
every decision**, because the alternative to measuring anchoring is assuming it did not
happen. `labeling.evaluate` reports agreement-with-model separately for decisions made
with the score shown and hidden.

## Consequences

- `make evaluate` prints "NO LABELS ... there is no precision or recall number of any
  kind" until a human has labelled. That is the correct output, not a gap.
- The synthetic labels exist only to exercise the pipeline. They are marked
  `synthetic: true`, written to a separate gitignored file, deliberately built to
  *disagree* with the model so the metrics have something to measure, and every number
  derived from them is printed under a banner saying it is not accuracy.
- The queue is 192 pairs against a 120–150 budget, because Cook County is exhaustive.
  Restricting Cook to its 19 non-trivial pairs would give ~117 and lose the exhaustive
  recall estimate. That trade is the reviewer's to make; it is one constructor argument.
- `estimate_m_from_pairwise_labels` **ignores `clerical_match_score` and treats every row
  it is given as a confirmed match.** Passing the rejections as well — the obvious reading
  of "train from the labels" — would assert that every rejected pair is a match and
  silently corrupt every m value. Only positives are passed, and the reason is in the
  function's docstring where the next person will look.
