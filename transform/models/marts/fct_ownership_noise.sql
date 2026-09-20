-- Where owner noise is removed, one stage at a time.
--
-- The point of reporting this as a ladder is to show which step kills which kind of
-- noise. A single "4 real transfers" figure hides the fact that three separate
-- mechanisms had to run to get there -- and an earlier version of this report started
-- at level 1, which made the deduplication step invisible: the trailing-space
-- oscillation on EAF:79 is removed by DEDUPLICATION, not by normalisation, so it never
-- reached the level-1 count and the report could not see it at all.
--
--   level 0  raw source, before deduplication
--   level 1  after deduplication
--   level 2  after owner-name normalisation
--   level 3  after classification
with pre_dedup as (
    select furnace_type, facility_id, year, fid, owner, owner_verbatim
    from {{ source('raw', 'furnace_years') }}
    where source_version = 'owner_filled_2025'
      and alignment_status <> 'unaligned_no_key'
),

-- Level 0a: furnace-years the source supplies more than once at all.
ambiguous_grain as (
    select count(*) as n from (
        select furnace_type, facility_id, year, fid
        from pre_dedup
        group by 1, 2, 3, 4
        having count(*) > 1
    )
),

-- Level 0b: of those, the ones where the repeated rows disagree about the owner once
-- whitespace is preserved. `owner_verbatim` is the untrimmed cell, which is the only
-- place this difference survives.
ambiguous_owner as (
    select count(*) as n from (
        select furnace_type, facility_id, year, fid
        from pre_dedup
        group by 1, 2, 3, 4
        having count(distinct owner_verbatim) > 1
    )
),

-- Level 1: consecutive-year changes in the raw (trimmed) owner string, after dedup.
observed as (
    select
        facility_key,
        year,
        min(owner_raw) as owner_raw,
        min(owner_normalized) as owner_normalized
    from {{ ref('fct_furnace_year') }}
    where owner_normalized is not null
    group by facility_key, year
),

lagged as (
    select
        *,
        lag(owner_raw) over (partition by facility_key order by year) as prev_raw,
        lag(owner_normalized) over (partition by facility_key order by year) as prev_normalized
    from observed
),

levels as (
    select 0 as level,
           'raw source, before dedup' as stage,
           (select n from ambiguous_grain) as noisy_units,
           'furnace-years the source supplies more than once' as measures
    union all
    select 0,
           'raw source: repeats disagreeing on owner',
           (select n from ambiguous_owner),
           'of those, the ones whose owner spelling differs before trimming'
    union all
    select 1,
           'after dedup',
           count(*) filter (where prev_raw is not null and owner_raw is distinct from prev_raw),
           'consecutive-year changes in the raw owner string'
    from lagged
    union all
    select 2,
           'after normalisation',
           count(*) filter (
               where prev_normalized is not null
                 and owner_normalized is distinct from prev_normalized
           ),
           'consecutive-year changes in the normalised owner name'
    from lagged
    union all
    select 3,
           'after classification',
           (select count(*) from {{ ref('fct_ownership_change') }}
            where change_class = 'credible_transfer'),
           'transitions surviving typo, refinement, oscillation and gap filters'
)

select * from levels order by level, stage
