-- "Reported as unknown" and "never reported" must stay different states. If every
-- status collapsed to one value, the distinction would have been lost somewhere in
-- staging and nobody would be able to tell afterwards.
select count(distinct value_status) as distinct_statuses
from {{ ref('fct_plant_production_year') }}
having count(distinct value_status) < 2
