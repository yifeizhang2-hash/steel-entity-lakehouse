-- Every furnace, every year, 2012-2023.
--
-- Grain is (furnace_type, facility_id, year, fid). All four parts are needed: `ID`
-- repeats across furnace types, and `(ID, Year)` alone leaves 529 duplicates in the EAF
-- data because a facility runs several furnaces.
select
    d.furnace_key,
    d.facility_key,
    f.furnace_type,
    f.facility_id,
    f.fid,
    f.year,
    f.owner_raw,
    f.owner_normalized,
    f.owner_has_replacement_char,
    f.facility_aliases,
    f.encoding_degraded,
    f.encoding_lossy,
    f.main_product_line,
    f.no_of_furnaces,
    f.not_melting,
    f.power_kwh_per_metric_ton,
    f.source_version
from {{ ref('stg_furnace_year') }} f
join {{ ref('dim_furnace') }} d
    on d.furnace_type = f.furnace_type
   and d.facility_id = f.facility_id
   and coalesce(d.fid, 'x') = coalesce(f.fid, 'x')
