-- Two ownership spans for one facility must never both contain the same year.
-- The interval is half-open: [valid_from_year, valid_to_year).
select
    a.facility_key,
    a.version_number as version_a,
    b.version_number as version_b,
    a.valid_from_year,
    a.valid_to_year
from {{ ref('dim_facility_ownership_scd2') }} a
join {{ ref('dim_facility_ownership_scd2') }} b
    on a.facility_key = b.facility_key
   and a.version_number < b.version_number
where a.valid_from_year < coalesce(b.valid_to_year, 9999)
  and b.valid_from_year < coalesce(a.valid_to_year, 9999)
