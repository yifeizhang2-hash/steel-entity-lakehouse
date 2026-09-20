# Data quality findings

Every item here was measured, not inferred, and every one is pinned by a test so that a
future vintage which fixes or worsens it fails the build rather than passing quietly.

Several were found *because* an assertion failed. Those are marked — they are the ones
that argue for writing the assertion before trusting the data.

---

## 1. `9999` is a year in PLANTGEN

`PlantStartYr` looks 89/197 populated. **Eight of those values are `9999`**, the file's
sentinel for unknown; the real range is 1879–2002. Genuine coverage is **81/197 (41%)**.

Left alone it reaches the linker as a real year and turns an *absence* into a
*disagreement*, penalising correct pairs for a gap in the reference data.

Staging keeps `start_year` and `start_year_status ∈ {reported, unknown, absent}` apart.

> Found late, while investigating something else, after the 89/45% figure had already
> been used to describe evidence coverage in P3. The P3 weights were recomputed.

## 2. One facility is written twice — and the copies are not identical

`eaf_owner_filled.csv` holds facility **EAF:79** (the facility) **48
times where it should hold 24**: every `(year, fid)` appears on two consecutive lines.
No other facility is affected.

The pairs are **not** byte-identical. Only 2 of the 24 match exactly; the other 22
differ in `Owner`, spelled `AKSteelCorp.` on one line and `AKSteelCorp. ` — with a
trailing space — on the other. So full-row dedup removes 2 rows and grain dedup removes
24.

The correct count is known rather than guessed: the other vintage, `eaf_mini.csv`, holds
exactly 24 rows for this facility.

> **Found by the grain assertion failing the first time it ran.**
>
> Also the site of a methodological error worth recording: the duplicates were first
> reported as "byte-identical" on the strength of printed output, where a trailing space
> is invisible, and checked at the *bronze* level — **after** `clean_text` had already
> stripped the whitespace. The verification ran downstream of the transformation that
> hides the difference it was claiming did not exist. `owner_verbatim` now preserves the
> untrimmed cell in bronze, because a difference that only exists before trimming was
> invisible to everyone downstream, and bronze claims to be faithful.

## 3. An entire vintage lost its non-ASCII characters

**Not one non-ASCII character survives anywhere in `owner_filled_2025`** — and it is the
only vintage carrying owners.

```
gspt_2024                                      owner_filled_2025
Nucor Steel–South Carolina Darlington, S.C. →  Nucor Steel?South Carolina Darlington, S.C.
North American Höganäs Co. Hollsopple, Pa.  →  North American H?an? Co. Hollsopple, Pa.
```

**143 rows across 12 facilities.** 11 of the 12 are a clean 1:1 substitution of one
EN DASH by one `?`. The twelfth is not: `Höganäs` → `H?an?` turns 42 characters into 40,
because each multi-byte sequence was swallowed *together with the character after it*.

That asymmetry is the whole argument against repair. A `?` means "one or more characters
were here" and the count is not recoverable from the string, so a substitution rule would
be right on 11 facilities and silently wrong on the twelfth — with no way to tell which
class a given `?` belongs to. The vintages are combined by **alignment** on
`(furnace_type, id, year, fid)` instead. See [ADR-003](adr/ADR-003-encoding-degradation.md).

48 rows carry the damage in `Owner` itself, where no intact counterpart exists at all.

## 4. A North American dataset containing South American plants that can never join

22 rows in `eaf_owner_filled.csv` — two South American facilities — have
**no `ID`, no `FID` and no `Data_ID`**. They are real records with real owners and years,
and there is no key on which to attach them to anything, ever.

Bronze keeps them flagged `alignment_status = 'unaligned_no_key'`; staging filters them.

## 5. `Coordinate accuracy = 'approximate'` does not mean imprecise. It means invented.

Of the three North American plants marked `approximate`, one has no coordinates at all
and the other two carry **country-level placeholders**:

| plant | address field says | coordinates | resolves to |
|---|---|---|---|
| Nucor Steel Pacific Northwest | "Pacific Northwest, United States" | 37.0902, −95.7129 | **Montgomery County, Kansas** |
| BlueScope Steel EAF | "Eastern United States" | 39.8157, −101.2752 | Rawlins County, Kansas |

`37.0902, −95.7129` is the conventional geographic centre of the contiguous United
States, to four decimal places.

Zero of the three match a PLANTGEN county. They are **excluded outright** from
geographic blocking rather than down-weighted: a weight cannot repair a fabricated value.

## 6. A plant that reports its accuracy but not its location

`P100000121251` ("Electra iron plant") has `Coordinates = "unknown"` while still
reporting `coordinate_accuracy = 'approximate'`. Accuracy therefore cannot be used as a
proxy for presence. It carries `has_coordinates = false` and goes to manual review with
no bespoke fallback rule.

## 7. Geographic blocking is weakest exactly where steel is densest

The two most crowded counties in PLANTGEN behave in opposite ways:

| county | plants | with ZIP | distinct ZIPs | ZIP separates them? |
|---|---:|---:|---:|---|
| Allegheny PA (Pittsburgh) | 9 | 9 | 9 | **yes** |
| Cook IL (Chicago) | 10 | 9 | 7 | **no** |

In Chicago two pairs share a ZIP (`60411`, `60617`) and one plant has none, so county
and ZIP fail *together*. The aggregate figure — 36% of shared-county plants still score
above 0.9, against 34% of plants alone in their county — hides this completely, which is
why Cook County is sampled exhaustively for labelling.

## 8. Sometimes geography carries zero information, not merely weak information

Cleveland-Cliffs Indiana Harbor reaches two PLANTGEN plants that share its **county
(18089), its ZIP (46312) and its city (East Chicago)**. Only the plant name differs —
`Indiana Harbor` against `East Chicago` — and one of the two has no street address at all.

This is not a modelling defect. It is real ambiguity in the data, and it is the first
thing in the labelling queue.

## 9. A generalised shoreline puts a plant in a lake

The Census 1:500,000 cartographic boundary for Lake County, IN cuts inland of the
lakefront, so Indiana Harbor's coordinates fall **255 m outside** every county polygon.
Heavy industry is disproportionately waterfront, so this is systematic rather than a
fluke.

It is reported as a near miss with a measured distance, not snapped to a county:
`nearest_county_fips` and `nearest_county_distance_m` separate the cases cleanly — 255 m
for the clipped shoreline, 1.1 km for Algoma across the St. Marys River, and 59–166 km
for the eight Canadian plants genuinely not in any US county.

## 10. A changed owner string is almost never a changed owner

Raw diffs find 10 ownership changes. **Two survive scrutiny.**

| stage | count |
|---|---:|
| raw source, before dedup: furnace-years supplied more than once | 24 |
| ...of those, repeats whose owner spelling differs before trimming | 22 |
| after dedup: consecutive-year raw string changes | 10 |
| after normalisation (87 raw owner strings → 83) | 10 |
| **after classification: credible transfers** | **2** |

Level 0 exists because the EAF:79 whitespace oscillation is removed by the
**deduplication**, not by normalisation — so it never reached the level-1 count, and a
ladder starting at level 1 could not see it at all.

The drop from 10 to 2 is four kinds of non-event:

- **8 observation gaps** — the owner did not change; the facility left the data for a year
- **2 likely typos** — `JSW Steel USA` → `JSW Stee USA` → back: one dropped letter, two fake transfers
- **2 name refinements** — `ArcelorMittal` ↔ `ArcelorMittal Dofasco Inc.`
- **4 round-trip legs** — A → B → A inside two years

> The round-trip figure was **wrong in an earlier version and has been corrected.** The
> classifier judged each transition on its own, so in `A -> B -> A` only the
> *return* leg looked like a round trip and the outbound leg survived as credible. That
> cannot be right: if the record bounces back within two years, the outbound step is
> exactly as unreliable as the one undoing it. Credibility is a property of the
> **sequence**, which no per-transition rule can express. Fixing it took credible
> transfers from 4 to 2.
>
> The repair had to avoid over-correcting: `BOF-70 2022 Republic → ArcelorMittal` happens
> *after* the excursion and must survive it. An implementation collapsing each facility
> to its modal owner would delete it and conclude that 100% of BOF ownership changes are
> noise. A dedicated regression test pins it.

## 11. The two alias vocabularies share zero strings

GSPT writes company-plus-plant strings (`gerdau midlothian steel mill`); PLANTGEN writes
bare place names (`dearborn`). Across 167 and 190 distinct values, the intersection is
**empty**, so an exact array-intersection comparator fires on **0 of 18,321 pairs** and
neither its m nor its u is estimable.

This is structural, not dirt. The comparator was replaced with token containment, which
fires on 34 pairs and separates correctly (`ak steel butler works` against `butler`).

## 12. PLANTGEN's "plant name" is a place name

`PlantName` equals its own `City` on **168 of 197 rows (85.3%)** ignoring case — 167
case-sensitively, the difference being PlantID 64, `Laplace` against `LaPlace`.

Entity resolution must therefore compare PLANTGEN's *plant name* against GSPT's *city*,
not against GSPT's plant name. All four pairings are folded into one comparator, because
two separate ones would double-count the same agreement.

## 13. Fields that look like evidence and are not

| field | why it cannot be used |
|---|---|
| **owner** | PLANTGEN has no owner column at all |
| **capacity** | PLANTGEN has no capacity column at all |
| **phone** | PLANTGEN only; GSPT has none |
| **state, raw** | `Alabama` vs `AL` — raw intersection is **empty** before normalisation |

Owner and capacity are not low-priority signals. They do not exist on one side.
