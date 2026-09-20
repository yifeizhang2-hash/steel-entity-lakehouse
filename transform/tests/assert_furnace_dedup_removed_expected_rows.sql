-- Exactly 24 exact-duplicate rows are expected to be removed, all from EAF:79.
-- Pinned so that a future vintage duplicating something else fails the build rather
-- than being absorbed silently by the `qualify` in `stg_furnace_year`.
with bronze as (
    select count(*) as n
    from {{ source('raw', 'furnace_years') }}
    where source_version = 'owner_filled_2025'
      and alignment_status <> 'unaligned_no_key'
),
staged as (
    select count(*) as n from {{ ref('stg_furnace_year') }}
)
select bronze.n as bronze_rows, staged.n as staged_rows, bronze.n - staged.n as removed
from bronze, staged
where bronze.n - staged.n <> 24
