-- Yearly production, long. Scoped to North American plants by joining the plant list,
-- and the sentinel vocabulary resolved: `value_ttpa` is a number only when the sheet
-- reported one, and `value_status` says what the sheet actually said.
select
    p.plant_id,
    p.year,
    p.metric,
    p.value_ttpa,
    {{ sentinel_status('p.value_raw') }} as value_status,
    p.value_raw,
    p.source_file
from {{ source('raw', 'gspt_production') }} p
where p.plant_id in (select distinct plant_id from {{ ref('stg_gspt_plant_unit') }})
