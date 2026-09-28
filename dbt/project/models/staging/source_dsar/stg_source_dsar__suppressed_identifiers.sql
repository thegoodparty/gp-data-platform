-- Only the columns a filter needs. subject_name and notes stay in the source table so
-- the requester's name is not copied into a schema more people can read.
select request_id, identifier_type, identifier_value, received_at
from {{ source("source_dsar", "suppressed_identifiers") }}
