-- Plant x year x metric production, 2019-2022, for North American plants.
--
-- `value_ttpa` is null wherever the sheet did not report a number, and `value_status`
-- says why: `reported`, `unknown` (the tracker looked and could not find out),
-- `bounded` (">0"), or `unparsed`. Collapsing all of those to null would make "we asked
-- and nobody knew" indistinguishable from "we never asked".
select
    'gspt:' || p.plant_id     as plant_key,
    p.plant_id,
    p.year,
    p.metric,
    p.value_ttpa,
    p.value_status,
    p.value_raw
from {{ ref('stg_gspt_production') }} p
