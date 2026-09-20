-- Owners as GSPT identifies them: `Owner PermID` is populated on all 111 North American
-- rows and resolves to 45 distinct owners, which makes it the only stable external
-- identifier anywhere in this project.
--
-- PLANTGEN contributes nothing here: it has no owner column at all.
select
    owner_permid                       as owner_permid,
    any_value(owner_gem_id)            as owner_gem_id,
    any_value(owner)                   as owner_name,
    {{ normalize_owner('any_value(owner)') }} as owner_name_normalized,
    any_value(parent)                  as parent_name,
    count(distinct plant_id)           as plant_count
from {{ ref('stg_gspt_plant_unit') }}
where owner_permid is not null
group by owner_permid
