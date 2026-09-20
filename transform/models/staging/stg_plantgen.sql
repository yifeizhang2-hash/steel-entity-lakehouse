-- PLANTGEN, scoped and cleaned. It is US-only by construction, so there is no
-- geographic filter here; the only correction is the 9999 start-year sentinel.
select
    plant_id,
    plant_name,
    city,
    county,
    state,
    st_cnty_fips                                  as county_fips,
    zip_code,
    street_address,
    {{ plausible_year('plant_start_yr') }}        as start_year,
    -- 8 of the 89 populated values are 9999, PLANTGEN's "unknown". Recording which
    -- state each row is in keeps "reported as unknown" separate from "never reported".
    case
        when plant_start_yr is null then 'absent'
        when plant_start_yr = 9999 then 'unknown'
        else 'reported'
    end                                           as start_year_status,
    {{ plausible_year('shutdown_yr') }}           as shutdown_year,
    aisi_code,
    specialty_grade,
    source_file
from {{ source('raw', 'plantgen') }}
