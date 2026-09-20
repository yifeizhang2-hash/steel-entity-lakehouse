-- Geographic enrichment, unchanged. Carried into staging so the marts do not reach past
-- the staging layer into a source.
select
    plant_id,
    county_fips,
    county_name,
    county_state,
    nearest_county_fips,
    nearest_county_distance_m,
    county_assignment_status,
    in_plantgen_county,
    coordinate_accuracy,
    has_coordinates,
    country_area
from {{ source('geo', 'gspt_plant_county') }}
