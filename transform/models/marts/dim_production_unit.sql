-- The grain GSPT actually publishes at: one row per plant x production unit.
-- 111 North American rows over 93 plants; ten plants have two units, one has three and
-- two have four. Capacity and operating status vary between a plant's units, which is
-- exactly why `dim_plant` cannot carry them.
select
    'gspt:' || plant_id || ':' || cast(unit_id as varchar) as production_unit_key,
    'gspt:' || plant_id                                    as plant_key,
    plant_id,
    unit_id,
    capacity_operating_status,
    main_production_process,
    main_production_equipment,
    start_year,
    start_year_status,
    retired_year,
    crude_steel_capacity_ttpa,
    crude_steel_capacity_status,
    bof_steel_capacity_ttpa,
    bof_steel_capacity_status,
    eaf_steel_capacity_ttpa,
    eaf_steel_capacity_status,
    iron_capacity_ttpa,
    iron_capacity_status,
    workforce_size,
    workforce_size_status
from {{ ref('stg_gspt_plant_unit') }}
