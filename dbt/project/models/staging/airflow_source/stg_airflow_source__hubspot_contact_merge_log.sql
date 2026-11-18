-- The merge job's record of what it tried, one row per pair. No filter and no
-- deduplication: the current state of a pair is derived from every row it has.
-- The snapshots are the pre-merge read of each contact, kept so a merge can be
-- reconstructed by hand, and arrive as JSON text.
select
    run_id,
    object_type,
    primary_id,
    secondary_id,
    status,
    survivor_id,
    gp_person_id,
    match_evidence,
    error,
    from_json(primary_snapshot, 'map<string, string>') as primary_snapshot,
    from_json(secondary_snapshot, 'map<string, string>') as secondary_snapshot,
    attempted_at
from {{ source("airflow_source", "hubspot_contact_merge_log") }}
