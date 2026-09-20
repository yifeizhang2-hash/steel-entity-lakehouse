-- The bridge must never carry a match/no-match decision. See ADR-004 and ADR-005:
-- there are zero labels and no threshold, and the linker's geographic comparators are
-- correlated by construction, so binarising a probability here would assert correctness
-- on no evidence.
--
-- This fails the build if a column whose name looks like a verdict ever appears.
--
-- The empty CTE below is not dead code: it is what makes dbt see this test as depending
-- on `bridge_plant_xref`, so that selecting the model also selects this test. Without
-- it the test still runs on a full `dbt build`, but `--select bridge_plant_xref+` would
-- quietly skip the one assertion that matters most about that model.
with model_dependency as (
    select * from {{ ref('bridge_plant_xref') }} limit 0
)

select column_name
from information_schema.columns
where table_name = '{{ ref('bridge_plant_xref').identifier }}'
  and (
      lower(column_name) in ('is_match', 'matched', 'is_matched', 'match', 'accepted',
                             'is_accepted', 'decision', 'verdict', 'link_confirmed',
                             'is_link', 'resolved_plant_id', 'matched_plant_key')
      or lower(column_name) like 'is\_match%' escape '\'
      or lower(column_name) like '%\_verdict' escape '\'
  )
  and (select count(*) from model_dependency) = 0
