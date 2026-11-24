-- Reads int__hubspot_owners_history, not the Airbyte source directly: that table
-- is the never-delete layer over a frozen Airbyte feed plus Lakeflow, and it is
-- the only place an archived owner survives, because Lakeflow returns none.
--
-- Archived here means absent from the live Lakeflow snapshot, not `archived = true`
-- on the history row: an owner archived after the seed keeps its last
-- `archived = false` in the history layer and would otherwise read as active.
--
-- The transaction columns are kept so the view shape does not change. They are
-- loader metadata from Airbyte and have no Lakeflow equivalent, so a
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
left join
    (select distinct id from {{ source("lakeflow_source", "owners") }}) as live
    on history.id = live.id
where live.id is null
