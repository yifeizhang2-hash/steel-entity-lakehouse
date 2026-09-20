-- One row per GSPT plant x production unit -- the sheet's true grain, 111 North
-- American rows over 93 plant ids.
--
-- NORTH AMERICA IS FILTERED HERE, NOT IN BRONZE. Bronze holds all 1,848 global rows so
-- the scope stays reversible and auditable; narrowing is a modelling decision and
-- belongs where it can be read.
select
    plant_id,
    source_row                                        as unit_id,
    plant_name_english                                as plant_name,
    other_plant_names_english                         as other_plant_names,
    owner,
    owner_permid,
    owner_gem_id,
    parent,
    municipality                                      as city,
    subnational_unit_province_state                   as state_name,
    country_area,
    location_address,
    coordinates,
    coordinate_accuracy,
    has_coordinates,
    latitude,
    longitude,
    capacity_operating_status,
    main_production_process,
    main_production_equipment,
    {{ plausible_year('start_date') }}                as start_year,
    {{ sentinel_status('start_date') }}               as start_year_status,
    {{ plausible_year('retired_date') }}              as retired_year,

    -- Capacity: value and status side by side, for every sentinel-bearing column.
    {{ sentinel_value('nominal_crude_steel_capacity_ttpa') }} as crude_steel_capacity_ttpa,
    {{ sentinel_status('nominal_crude_steel_capacity_ttpa') }} as crude_steel_capacity_status,
    {{ sentinel_value('nominal_bof_steel_capacity_ttpa') }}   as bof_steel_capacity_ttpa,
    {{ sentinel_status('nominal_bof_steel_capacity_ttpa') }}  as bof_steel_capacity_status,
    {{ sentinel_value('nominal_eaf_steel_capacity_ttpa') }}   as eaf_steel_capacity_ttpa,
    {{ sentinel_status('nominal_eaf_steel_capacity_ttpa') }}  as eaf_steel_capacity_status,
    {{ sentinel_value('nominal_iron_capacity_ttpa') }}        as iron_capacity_ttpa,
    {{ sentinel_status('nominal_iron_capacity_ttpa') }}       as iron_capacity_status,
    {{ sentinel_value('workforce_size') }}                    as workforce_size,
    {{ sentinel_status('workforce_size') }}                   as workforce_size_status,

    source_file
from {{ source('raw', 'gspt_plants') }}
where country_area in ('United States', 'Canada')
