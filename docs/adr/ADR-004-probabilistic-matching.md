# ADR-004: Weights are estimated, not chosen

- **Status**: accepted
- **Date**: 2026-09-19
- **Deciders**: project author

## Context

GSPT and PLANTGEN share no key. Deciding whether a GSPT plant and a PLANTGEN plant are
the same facility means weighing several weak, partly-missing signals against each
other. The obvious approach is a scorecard: ZIP match = 10 points, same county = 5, name
similarity > 0.9 = 3, accept above 12.

Every number in that sentence would be invented.

## Decision

Use Splink's Fellegi–Sunter model and estimate every m and u probability from the data
with EM. **No weight in this repository is set by hand.** A test asserts it: every
comparison level is checked for `fix_m_probability` / `fix_u_probability`, and a fixed
parameter fails the build.

Where a parameter cannot be estimated, it is **reported as unestimated**, not filled in.

### What evidence exists, and what does not

Only fields both sources carry can be evidence. Measured on the real data:

| evidence | GSPT | PLANTGEN |
|---|---|---|
| ZIP | 80/93, extracted from `Location address` free text | 180/197 |
| street address | 92/93 | 126/197 |
| place name (city / plant name) | 90/93 city | 197/197 |
| secondary plant names | 66/93 | — (none) |
| start year | 90/93 | **89/197** |
| county FIPS | 81/93 after exclusions | 197/197 |
| **owner** | 111/111 | **column does not exist** |
| **capacity** | present | **column does not exist** |
| **phone** | — | 147/197 |

Owner and capacity are not low-priority signals. PLANTGEN has no such columns, so they
cannot be compared at all. Phone is one-sided in the other direction. Any scorecard that
included them would be scoring a field that is never populated on one side.

## Three consequences that shaped the model

### 1. Place names get one comparator, not two

PLANTGEN's `PlantName` equals its own `City` on 168 of 197 rows. So "do the plant names
agree" is the wrong question; "do these records name the same place" is the right one,
and the answer can come from name/name, name/city, city/name or city/city.

The obvious implementation — a `city` comparator *and* a name-against-city comparator —
violates Splink's conditional-independence assumption, because PLANTGEN's name usually
*is* its city. The same agreement would be counted twice and every match weight would be
inflated. They are folded into one `place` comparator instead.

For the same reason, the alias comparator uses GSPT's `other_names` with the primary
plant name removed, so it is independent evidence rather than a restatement.

### 2. The alias comparator was changed because the specified one was measurably empty

An exact array intersection between the two alias lists fires on **0 of 18,321 pairs**.
The two vocabularies share zero strings: GSPT writes `"gerdau midlothian steel mill"`,
PLANTGEN writes `"dearborn"`. Neither m nor u is estimable, because the level is never
observed.

Token containment — does one of GSPT's secondary names contain the PLANTGEN place as a
whole word — fires and separates correctly (`"ak steel butler works"` against
`"butler"`). The comparator was changed on that measurement. Its weight is still EM's.

### 3. A near-miss on geography is an ordered level, not a threshold

P2 found one plant whose coordinates fall 255 m outside the generalised boundary of the
county it is in. Rather than deciding here whether 255 m "counts", the county comparator
has ordered levels — exact, within 500 m, within 5 km, else — and EM estimates each.

It worked: Cleveland-Cliffs Indiana Harbor reaches a reference record, which is literally
named `indiana harbor`, at p = 0.9997 through the `county_near_500m` level. It also
reaches a second reference record in the same county at p = 0.9995, which is the shared-county ambiguity
in miniature.

**But the weight for that level rests on 3 candidate pairs, all from that one plant.**
The report flags it as low support. An estimated number is not automatically a
trustworthy number, and a table of weights without the pair counts behind them cannot be
used to tell the difference.

### The one stated prior

`probability_two_random_records_match` is set to 1/197 rather than estimated. That is
not a tuned value: for a one-to-one link between 93 and 197 records, at most
min(93, 197) = 93 of the 18,321 possible pairs can be true matches, so 93/18,321 = 1/197
is the structural upper bound. A larger value would assert that a GSPT plant matches
more than one PLANTGEN plant.

## Consequences

- The weights table says which evidence actually discriminates, with the pair count each
  estimate rests on. Three levels are unestimated and three are flagged low-support.
- The model is only as good as its independence assumption. The `place` and
  `alias_place` comparators are built to be independent, but `zip5`, `street` and
  `county` are geographically correlated by construction — a pair agreeing on ZIP will
  usually agree on county. This inflates confident weights, and the calibration should
  be read with that in mind. Fixing it properly needs labelled pairs, which is P4.
- Nothing here is a match yet. The output is a candidate table with calibrated
  probabilities and the evidence vector behind each one.
