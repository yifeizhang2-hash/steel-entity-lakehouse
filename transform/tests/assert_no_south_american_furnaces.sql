-- The 22 two South American facilities rows carry no ID at all and can
-- never be joined. Bronze keeps them; the model must not.
select furnace_key, facility_id
from {{ ref('fct_furnace_year') }}
where facility_id is null
