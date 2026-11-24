-- Reads int__hubspot_owners_history: the never-delete layer, and the only place
-- an archived owner survives because Lakeflow returns none.
--
-- Archived means absent from the live Lakeflow snapshot, not `archived = true` on
-- the history row, which keeps its last value and would read as active forever.
-- The Airbyte transaction columns stay so the view shape is unchanged; they have
-- no Lakeflow equivalent, so a Lakeflow owner has NULL there.
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
