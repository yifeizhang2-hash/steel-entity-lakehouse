-- One row per plant, from both sources, in one key space.
--
-- The two sources are NOT merged into single plants here. Nothing in this repository
-- has established that a GSPT plant and a PLANTGEN plant are the same facility, so
-- doing so would be asserting a match. `dim_plant` holds both populations side by side
-- with a source-qualified key; `bridge_plant_xref` holds the (unresolved) candidates
-- between them.
with gspt as (
    select
        'gspt:' || u.plant_id                     as plant_key,
        'gspt'                                    as source_system,
        u.plant_id                                as source_plant_id,
        any_value(u.plant_name)                   as plant_name,
        any_value(u.city)                         as city,
        any_value(u.state_name)                   as state_name,
        any_value(u.country_area)                 as country_area,
        any_value(u.location_address)             as street_address,
        cast(null as varchar)                     as zip_code,
        any_value(c.county_fips)                  as county_fips,
        any_value(c.county_name)                  as county_name,
        any_value(u.latitude)                     as latitude,
        any_value(u.longitude)                    as longitude,
        any_value(u.coordinate_accuracy)          as coordinate_accuracy,
        any_value(u.has_coordinates)              as has_coordinates,
        any_value(c.county_assignment_status)     as county_assignment_status,
        min(u.start_year)                         as start_year,
        max(u.retired_year)                       as retired_year,
        count(*)                                  as production_unit_count
    from {{ ref('stg_gspt_plant_unit') }} u
    left join {{ ref('stg_plant_county') }} c using (plant_id)
    group by u.plant_id
),

plantgen as (
    select
        'plantgen:' || plant_id     as plant_key,
        'plantgen'                  as source_system,
        plant_id                    as source_plant_id,
        plant_name,
        city,
        state                       as state_name,
        'United States'             as country_area,
        street_address,
        zip_code,
        county_fips,
        county                      as county_name,
        cast(null as double)        as latitude,
        cast(null as double)        as longitude,
        cast(null as varchar)       as coordinate_accuracy,
        false                       as has_coordinates,
        'not_applicable'            as county_assignment_status,
        start_year,
        shutdown_year               as retired_year,
        1                           as production_unit_count
    from {{ ref('stg_plantgen') }}
)

select * from gspt
union all
select * from plantgen
