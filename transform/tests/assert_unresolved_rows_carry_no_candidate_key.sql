-- A row the model never compared must not carry a candidate PLANTGEN key. If it did,
-- a downstream join would pick up a link the model never endorsed, and one hop later
-- nothing would show where the link came from.
select gspt_plant_id, resolution_status, plantgen_plant_key
from {{ ref('bridge_plant_xref') }}
where resolution_status = 'never_compared'
  and plantgen_plant_key is not null
