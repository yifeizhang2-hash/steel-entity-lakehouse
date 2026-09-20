-- Candidate links between GSPT and PLANTGEN plants. NOT a match table.
--
-- ============================================================================
-- THIS MODEL CONTAINS NO MATCH/NO-MATCH VERDICT, AND MUST NOT ACQUIRE ONE.
-- ============================================================================
--
-- There are zero human labels, so no threshold has been chosen. On top of that, the
-- linker's `zip5`, `street` and `county` comparators are geographically correlated by
-- construction, which violates the conditional-independence assumption and inflates
-- confident weights (see ADR-004). Binarising a probability into `is_match` in that
-- state would assert correctness on no evidence -- the failure mode this project exists
-- to eliminate. `tests/assert_bridge_has_no_verdict.sql` fails the build if a column
-- that looks like a verdict ever appears here.
--
-- WHY AN UNRESOLVED ROW CARRIES NO CANDIDATE ID
-- ---------------------------------------------
-- `matched_plantgen_plant_key` is populated ONLY for rows the model scored highly, and
-- even there it means "this is the candidate", not "this is the match". Any downstream
-- model that joins on it would otherwise pick up a link the model never endorsed, and
-- the provenance of that link would be invisible one hop later. Low-probability rows
-- carry their candidate in `plantgen_plant_key` for review, and nothing else.
with scored as (
    select
        c.unique_id_l                          as gspt_plant_id,
        c.unique_id_r                          as plantgen_plant_id,
        c.match_probability,
        c.match_weight,
        c.gamma_zip5,
        c.gamma_street,
        c.gamma_place,
        c.gamma_alias_place,
        c.gamma_start_year,
        c.gamma_county
    from {{ source('resolution', 'gspt_plantgen_candidates') }} c
),

-- Every North American GSPT plant, including the ones blocking never reached. A plant
-- that was never compared is a different outcome from one compared and scored low, and
-- reporting both as "no match" would hide 8 Canadian plants inside an accuracy figure.
all_gspt as (
    select distinct plant_id from {{ ref('stg_gspt_plant_unit') }}
),

with_status as (
    select
        g.plant_id                                  as gspt_plant_id,
        'gspt:' || g.plant_id                       as gspt_plant_key,
        s.plantgen_plant_id,
        case when s.plantgen_plant_id is not null
             then 'plantgen:' || s.plantgen_plant_id end as plantgen_plant_key,
        s.match_probability,
        s.match_weight,
        s.gamma_zip5,
        s.gamma_street,
        s.gamma_place,
        s.gamma_alias_place,
        s.gamma_start_year,
        s.gamma_county,
        case
            when s.plantgen_plant_id is null then 'never_compared'
            else 'candidate'
        end                                         as resolution_status
    from all_gspt g
    left join scored s on s.gspt_plant_id = g.plant_id
)

select
    w.*,
    -- Why this pair was ever considered, or why the plant was never reached at all.
    case
        when w.resolution_status = 'never_compared' then
            case
                when u.country_area = 'Canada'
                    then 'no US county, no US ZIP, no shared state: PLANTGEN is a US census'
                when not u.has_coordinates or u.coordinate_accuracy = 'approximate'
                    then 'no usable geography and no ZIP: placeholder coordinates'
                else 'no blocking key matched any PLANTGEN record'
            end
        when w.gamma_county >= 2 then 'county'
        when w.gamma_zip5 = 1 then 'zip'
        else 'state'
    end                                             as blocking_reason,
    u.country_area
from with_status w
join (
    select plant_id, any_value(country_area) as country_area,
           bool_or(has_coordinates) as has_coordinates,
           any_value(coordinate_accuracy) as coordinate_accuracy
    from {{ ref('stg_gspt_plant_unit') }} group by plant_id
) u on u.plant_id = w.gspt_plant_id
