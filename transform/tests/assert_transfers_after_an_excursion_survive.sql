-- A real transfer that happens AFTER a round-trip excursion must survive it.
--
-- This is the thing most easily broken while fixing round-trip suppression. BOF-70 goes
-- A -> B -> A (an excursion) and then A -> C in 2022.
-- Suppressing the excursion is correct; suppressing 2022 as well is not, and an
-- implementation that collapsed the facility's sequence to its modal owner would do
-- exactly that and then report that 100% of BOF ownership changes are noise.
select facility_key, change_year, from_owner_raw, to_owner_raw, change_class
from {{ ref('fct_ownership_change') }}
where facility_key = 'BOF-70'
  and change_year = 2022
  and change_class <> 'credible_transfer'
