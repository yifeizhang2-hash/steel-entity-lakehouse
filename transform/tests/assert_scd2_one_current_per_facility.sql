-- Exactly one open span per facility. Zero means history that stops nowhere; more than
-- one means two owners hold the same plant today.
select facility_key, count(*) as current_rows
from {{ ref('dim_facility_ownership_scd2') }}
where is_current
group by facility_key
having count(*) <> 1
