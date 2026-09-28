-- A named HubSpot contact must come with a count of at least one. The count is
-- derived from the person graph while the id can resolve from the user's own
-- record, so the two can disagree unless the count is floored.
select user_id, hubspot_key_source, hubspot_contact_count
from {{ ref("int__user_resolved_keys") }}
where hubspot_key_source <> 'none' and hubspot_contact_count < 1
