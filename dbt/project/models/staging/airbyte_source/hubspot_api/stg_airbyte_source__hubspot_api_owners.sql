-- Reads int__hubspot_owners_history, not the Airbyte source directly: that table
-- is the never-delete layer over a frozen Airbyte feed plus Lakeflow.
--
-- Active membership comes from the live Lakeflow snapshot, not from the history
-- row's own `archived` flag. Lakeflow returns no archived owners, so an owner
-- archived after the seed simply stops appearing there and its history row keeps
-- its last `archived = false`; the flag alone would report that owner as active
-- forever. The history row still supplies the name and the teams.
--
-- The transaction columns are kept so the view shape does not change. They are
-- loader metadata from Airbyte and have no Lakeflow equivalent (_commit_timestamp,
-- _ingest_time, _etl_timestamp and _load_timestamp all fail to resolve), so a
-- Lakeflow-sourced owner has NULL there and a frozen one keeps its value.
select
    history._airbyte_raw_id,
    history._airbyte_extracted_at,
    history._airbyte_meta,
    history._airbyte_generation_id,
    history.id,
    history.email,
    history.teams,
    history.user_id,
    history.archived,
    history.last_name,
    history.created_at,
    history.first_name,
    history.updated_at,
    history.user_id_including_inactive
from {{ ref("int__hubspot_owners_history") }} as history
inner join
    (select distinct id from {{ source("lakeflow_source", "owners") }}) as live
    on history.id = live.id
