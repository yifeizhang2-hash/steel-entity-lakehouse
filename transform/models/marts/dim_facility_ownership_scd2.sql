-- Ownership history per facility, as a Type 2 slowly-changing dimension.
--
-- WHY NORMALISED NAMES DRIVE THE DIFF
-- -----------------------------------
-- The only owner data in the project is a free-text name repeated once per furnace-year.
-- Diffing the raw strings measures formatting churn, not ownership: "Nucor Corp." and
-- "Nucor Corp" are the same company and a raw diff reports a transfer between them.
-- A change is therefore declared only when the NORMALISED name changes; the raw string
-- is carried alongside so the reader can see what actually moved.
--
-- The 48 encoding-damaged rows (`Standard Steel ? Burnham`, `North American H?an?`) have
-- no intact counterpart -- the vintage carrying owners is the vintage that was damaged
-- -- so `normalize_owner` strips the `?` as punctuation and they normalise to a stable
-- key rather than flickering. `has_encoding_damage` marks the spans built from them.
--
-- Grain is the facility, not the furnace: a plant's furnaces do not change hands
-- separately. `facility_key` is `<furnace_type>-<facility_id>`, type-qualified because
-- 18 ids collide across the two furnace types.
with facility_year as (
    select
        facility_key,
        furnace_type,
        facility_id,
        year,
        -- One owner per facility-year. The furnaces of a facility agree on it; taking a
        -- deterministic single value keeps the diff from oscillating on row order.
        min(owner_normalized)                           as owner_normalized,
        min(owner_raw)                                  as owner_raw,
        bool_or(owner_has_replacement_char)             as has_encoding_damage
    from {{ ref('fct_furnace_year') }}
    where owner_normalized is not null
    group by facility_key, furnace_type, facility_id, year
),

flagged as (
    select
        *,
        lag(owner_normalized) over (partition by facility_key order by year) as prev_owner,
        lag(year) over (partition by facility_key order by year)             as prev_year
    from facility_year
),

-- A new span starts at the first year, when the normalised owner changes, or when the
-- facility disappears from the data for a year or more (a gap is not continuity).
marked as (
    select
        *,
        case
            when prev_owner is null then 1
            when owner_normalized is distinct from prev_owner then 1
            when year - prev_year > 1 then 1
            else 0
        end as is_span_start
    from flagged
),

numbered as (
    select
        *,
        sum(is_span_start) over (
            partition by facility_key order by year
            rows between unbounded preceding and current row
        ) as span_number
    from marked
),

spans as (
    select
        facility_key,
        span_number,
        any_value(furnace_type)             as furnace_type,
        any_value(facility_id)              as facility_id,
        min(owner_normalized)               as owner_normalized,
        min(owner_raw)                      as owner_raw,
        min(year)                           as valid_from_year,
        max(year)                           as last_observed_year,
        bool_or(has_encoding_damage)        as has_encoding_damage
    from numbered
    group by facility_key, span_number
)

select
    facility_key || '-v' || cast(span_number as varchar) as ownership_key,
    facility_key,
    furnace_type,
    facility_id,
    span_number                                          as version_number,
    owner_normalized,
    owner_raw,
    valid_from_year,
    -- Half-open interval: a span runs up to, but not including, the year the next one
    -- starts, so no two spans for a facility can both contain the same year.
    lead(valid_from_year) over (
        partition by facility_key order by span_number
    )                                                    as valid_to_year,
    last_observed_year,
    lead(valid_from_year) over (partition by facility_key order by span_number) is null
                                                         as is_current,
    has_encoding_damage
from spans
