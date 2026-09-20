-- Furnace-level annual records, authoritative vintage only, North America only.
--
-- TWO FILTERS, BOTH DELIBERATE:
--
-- 1. `source_version = 'owner_filled_2025'`. It is the only vintage carrying `owner`.
--    Its facility names are encoding-damaged, so the intact name comes across from the
--    other vintage through `corrected_cl_clean`, which the ingest layer aligned on
--    (furnace_type, id, year, fid). See ADR-003.
--
-- 2. `alignment_status <> 'unaligned_no_key'`. Those 22 rows are a South American facility
--    (Montevideo) and another South American facility (Caracas) -- South American facilities in a dataset that
--    claims to be North American, carrying no ID, no FID and no Data_ID. They cannot be
--    joined to anything, ever. Bronze keeps them; the model cannot use them.
-- 3. Repeated furnace-years for one facility.
--
--    `eaf_owner_filled.csv` contains facility EAF:79 48 times where it should appear
--
--    The two lines of a pair are NOT identical. Measured on the raw CSV, only 2 of the
--    24 pairs match byte for byte; the other 22 differ in `Owner`, which is spelled
--    "AKSteelCorp." on one line and "AKSteelCorp. " -- with a trailing space -- on the
--    other. Full-row deduplication therefore removes 2 rows (48 -> 46); deduplicating on
--    the grain removes 24 (48 -> 24).
--
--    So the justification is narrower than "the duplicates carry nothing new": they do
--    carry different bytes. The claim is that the ONLY difference is trailing
--    whitespace, which carries no meaning, and that the correct row count is known
--    rather than guessed because the other vintage settles it -- `eaf_mini.csv` holds
--    exactly 24 rows for this facility.
--
--    The `order by` carries a tiebreak on `owner` after `source_row`. Source order alone
--    is deterministic today, but it is a property of how the file was written, not of
--    the data; if the upstream row order ever changed, `owner_raw` would change silently
--    with it. (For this facility the tiebreak is inert, because ingest trims whitespace
--    before bronze and both spellings arrive identical -- `owner_verbatim` preserves the
--    difference. It is here for the next case, where the differing column is not
--    whitespace.)
--
--    `tests/assert_furnace_dedup_removed_expected_rows.sql` pins the removal at 24, so a
--    future vintage duplicating something else fails the build rather than being
--    absorbed silently by the `qualify`.
select
    furnace_key,
    furnace_type,
    facility_id,
    fid,
    year,
    owner                                    as owner_raw,
    owner_verbatim                           as owner_verbatim,
    {{ normalize_owner('owner') }}           as owner_normalized,
    owner_has_replacement_char,
    corrected_cl_clean                       as facility_aliases,
    corrected_cl                             as facility_aliases_as_written,
    encoding_degraded,
    encoding_lossy,
    main_product_line,
    no_of_furnaces,
    not_melting,
    power_kwh_per_metric_ton,
    year_list,
    data_id,
    source_version,
    source_file
from {{ source('raw', 'furnace_years') }}
where source_version = 'owner_filled_2025'
  and alignment_status <> 'unaligned_no_key'
qualify row_number() over (
    partition by furnace_type, facility_id, year, fid
    order by source_row, owner
) = 1
