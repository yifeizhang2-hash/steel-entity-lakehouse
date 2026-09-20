-- Every observed ownership transition, classified by how much it should be believed.
--
-- A raw string diff is not an ownership change, and neither is a normalised one. This
-- model exists because the difference between "the name in the cell changed" and "the
-- plant changed hands" is the whole question, and reporting the first as the second
-- would put fabricated M&A events into a mart.
--
-- CREDIBILITY IS A PROPERTY OF THE SEQUENCE, NOT OF ONE TRANSITION
-- ----------------------------------------------------------------
-- An earlier version of this model classified each transition on its own and got a
-- structurally wrong answer. Consider BOF-70:
--
--     2015  A -> B          <- was called a credible transfer
--     2017  B -> A    <- was called an oscillation
--     2022  A -> C
--
-- Judged one at a time, only the RETURN leg looks like a round trip, so the outbound
-- leg survived as credible. That cannot be right: if the record is unreliable enough to
-- bounce back within two years, the outbound leg is exactly as unreliable. What is not
-- credible is the whole excursion, not the step that ends it. No per-transition rule
-- can see this, because whether 2015 is believable depends on what happens in 2017.
--
-- So the excursion is identified first -- a span whose predecessor and successor share
-- an owner that is not its own -- and BOTH of its legs are suppressed.
--
-- What must NOT happen is over-suppression. `BOF-70 2022 A -> C`
-- occurs AFTER the excursion and is untouched by it, so it has to survive. An
-- implementation that replaced a facility's whole sequence with its modal owner would
-- delete it and conclude that every BOF ownership change is noise.
-- `tests/assert_transfers_after_an_excursion_survive.sql` pins exactly that.
--
-- Classes, in order of how specifically they explain the transition:
--
--   observation_gap    the owner did not change; the SCD2 model opened a new span
--                      because the facility left the data for a year or more
--   likely_typo        normalised names within an edit distance of 2
--   name_refinement    one name contains the other (ArcelorMittal / ArcelorMittal
--                      Dofasco Inc.): the same owner at different precision
--   round_trip         either leg of a short A -> B -> A excursion
--   credible_transfer  none of the above; still a claim about a free-text column

{% set excursion_max_years = 3 %}

with spans as (
    select
        facility_key,
        furnace_type,
        facility_id,
        version_number,
        owner_normalized,
        owner_raw,
        valid_from_year,
        has_encoding_damage,
        lag(owner_normalized) over (
            partition by facility_key order by version_number
        ) as prev_owner,
        lead(owner_normalized) over (
            partition by facility_key order by version_number
        ) as next_owner,
        lag(owner_raw) over (
            partition by facility_key order by version_number
        ) as prev_owner_raw,
        lead(valid_from_year) over (
            partition by facility_key order by version_number
        ) as next_span_start
    from {{ ref('dim_facility_ownership_scd2') }}
),

-- A span the record departs to and then returns from. The duration bound is HAND-SET:
-- a facility that genuinely changes hands and changes back a decade later has had two
-- real transactions, while one that does so inside a couple of years has almost
-- certainly been recorded inconsistently. Every excursion in the current data lasts one
-- or two years, so the bound is not load-bearing today -- but it is a threshold nobody
-- has validated, and `classification_confidence` says so on every row.
excursions as (
    select
        facility_key,
        version_number,
        next_span_start - valid_from_year as excursion_years
    from spans
    where prev_owner is not null
      and next_owner is not null
      and prev_owner = next_owner
      and prev_owner is distinct from owner_normalized
      and next_span_start - valid_from_year <= {{ excursion_max_years }}
),

-- A transition runs from version n-1 into version n. It belongs to a round trip when
-- EITHER endpoint is an excursion span: the leg into the excursion and the leg out of
-- it are the same unreliable event seen from two sides.
transitions as (
    select
        s.facility_key,
        s.furnace_type,
        s.facility_id,
        s.version_number,
        s.prev_owner as from_owner_normalized,
        s.prev_owner_raw as from_owner_raw,
        s.owner_normalized as to_owner_normalized,
        s.owner_raw as to_owner_raw,
        s.valid_from_year as change_year,
        s.has_encoding_damage,
        exists (
            select 1 from excursions e
            where e.facility_key = s.facility_key
              and e.version_number in (s.version_number, s.version_number - 1)
        ) as in_round_trip,
        (
            select min(e.excursion_years) from excursions e
            where e.facility_key = s.facility_key
              and e.version_number in (s.version_number, s.version_number - 1)
        ) as round_trip_years
    from spans s
    where s.prev_owner is not null
)

select
    facility_key,
    furnace_type,
    facility_id,
    change_year,
    from_owner_raw,
    to_owner_raw,
    from_owner_normalized,
    to_owner_normalized,
    has_encoding_damage,
    in_round_trip,
    round_trip_years,
    levenshtein(from_owner_normalized, to_owner_normalized) as normalized_edit_distance,
    case
        when from_owner_normalized = to_owner_normalized then 'observation_gap'
        when levenshtein(from_owner_normalized, to_owner_normalized) <= 2 then 'likely_typo'
        when contains(to_owner_normalized, from_owner_normalized)
          or contains(from_owner_normalized, to_owner_normalized) then 'name_refinement'
        when in_round_trip then 'round_trip'
        else 'credible_transfer'
    end as change_class,
    -- `derived` where the answer follows from equality; everything else rests on a
    -- hand-chosen threshold that has never been checked against a label.
    case
        when from_owner_normalized = to_owner_normalized then 'derived'
        else 'heuristic_unvalidated'
    end as classification_confidence,
    case
        when from_owner_normalized = to_owner_normalized
            then 'equality (not a heuristic)'
        when levenshtein(from_owner_normalized, to_owner_normalized) <= 2
            then 'edit_distance<=2 (hand-set, no labels)'
        when contains(to_owner_normalized, from_owner_normalized)
          or contains(from_owner_normalized, to_owner_normalized)
            then 'substring containment (hand-set, no labels)'
        when in_round_trip
            then 'leg of an A->B->A excursion of {{ excursion_max_years }} years or less (hand-set, no labels)'
        else 'none of the above filters matched'
    end as classification_rule
from transitions
