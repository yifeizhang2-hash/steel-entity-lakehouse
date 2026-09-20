-- One row per furnace.
--
-- The surrogate key is `<furnace_type>-<facility_id>-<fid>`, and the type is in it
-- because `ID` is only unique WITHIN a furnace type: 18 ids appear in both the BOF and
-- the EAF file and denote different facilities. Id 70 is Severstal Dearborn as a BOF
-- and American Cast Iron Pipe Birmingham as an EAF. Keying on the bare id would merge
-- those two plants.
select
    furnace_type || '-' || facility_id || '-' || coalesce(fid, 'x') as furnace_key,
    furnace_type || '-' || facility_id                              as facility_key,
    furnace_type,
    facility_id,
    fid,
    min(year)                                    as first_year_seen,
    max(year)                                    as last_year_seen,
    count(*)                                     as observed_years,
    any_value(facility_aliases)                  as facility_aliases,
    bool_or(encoding_degraded)                   as any_name_encoding_degraded,
    bool_or(encoding_lossy)                      as any_name_encoding_lossy
from {{ ref('stg_furnace_year') }}
group by furnace_type, facility_id, fid
