# ADR-003: Encoding damage is detected and aligned around, never repaired

- **Status**: accepted
- **Date**: 2026-09-19
- **Deciders**: project author

## Context

The furnace census exists in two vintages of the same records:

| | rows | `Owner` populated | non-ASCII in facility names |
|---|---|---|---|
| `gspt_2024` (`bof_mini.csv`, `eaf_mini.csv`) | 2,153 | **0 of 2,153** | 143 rows |
| `owner_filled_2025` (`*_owner_filled.csv`) | 2,199 | **2,199 of 2,199** | **0 rows** |

Neither vintage is usable on its own, and the reason is specific: the 2025 vintage is
the only one that carries owners, and it is also the only one that went through a lossy
character-encoding conversion. **Not one non-ASCII character survives anywhere in it.**

The damage is visible by comparison:

```
gspt_2024                                      owner_filled_2025
Nucor Steel–South Carolina Darlington, S.C. →  Nucor Steel?South Carolina Darlington, S.C.
North American Höganäs Co. Hollsopple, Pa.  →  North American H?an? Co. Hollsopple, Pa.
```

143 rows across 12 facilities are affected. The `Owner` column itself is damaged on 48
rows, across two owners (`Standard Steel ? Burnham`, `North American H?an?`).

## Decision

**The two vintages are combined by alignment on a key. No string is ever repaired.**

- `corrected_cl` keeps each row's own value, verbatim, whatever vintage it came from.
- `corrected_cl_clean` carries the intact name from `gspt_2024`, attached by joining on
  `align_key` = `<furnace_type>:<facility_id>:<year>:<fid or ->`.
- `corrected_cl_name_version` and `alignment_status` record where the clean name came
  from and whether alignment succeeded, so the join is auditable from the table itself.
- `encoding_degraded` and `encoding_lossy` flag the damage per row.
- `owner_has_replacement_char` flags a `?` in `owner`, which has no intact counterpart.

**Substituting characters back — replacing `?` with `–` — is forbidden.**

## Why repair is not available

It is tempting, because 11 of the 12 damaged names are a clean 1:1 substitution: one
EN DASH (U+2013) became one `?`, and the string length is unchanged. A regex would
"fix" them.

The twelfth is not:

```
'North American Höganäs Co. Hollsopple, Pa.'   42 characters
'North American H?an? Co. Hollsopple, Pa.'     40 characters
```

`ög` became `?` and `äs` became `?`. Each multi-byte sequence was swallowed **together
with the character following it**. Two characters are simply gone, and nothing in the
damaged string says which.

That is the decisive point. A `?` in a damaged string means "one or more characters
were here"; the count is not recoverable from the string. So a repair rule would be
right on 11 facilities and silently wrong on the twelfth — and there is no way to tell
which class a given `?` belongs to without already having the original. If the original
is available, alignment is strictly better than guessing; if it is not, guessing is
unsafe. There is no case where repair is the right tool.

The stakes are not cosmetic: these strings are the *only* identifier the furnace data
offers for a plant. They are the input to entity resolution in P4. A name silently
"corrected" into something that was never in either source would corrupt a match and
carry a confident-looking provenance trail behind it.

## The contract check

`ingest/encoding.py` compares aligned pairs and counts, for each row, whether the
candidate gained a `?` that the original lacks *and* the original contains a non-ASCII
character at all. A `?` that is genuinely part of the text does not match.

**It warns; it does not halt.** Halting would be wrong here: the damaged vintage is the
only source of owner data, so refusing to load it costs more than it saves. The
degradation is a permanent property of the input, not a regression to be fixed. What the
check earns is that the counts are visible, so if the damage *spreads* — a new vintage,
a broader conversion — it shows up as a number that moved rather than as a name that
quietly changed.

Measured on the current data: 143 degraded rows, 12 of them lossy, 12 facilities, plus
48 rows with a `?` in `owner`.

## Consequences

- Downstream consumers read `corrected_cl_clean` for matching and `owner` for
  ownership, and get the best available version of each without either being
  reconstructed.
- 22 rows cannot align, because they have no `ID` at all (`alignment_status =
  'unaligned_no_key'`). They keep their own damaged-vintage name and stay flagged.
- Anyone who later wants the 11 reversible names "fixed" has the evidence to do it
  deliberately, in a transformation layer, with the lossy twelfth excluded by
  `encoding_lossy`. That decision is not made for them at ingest.
