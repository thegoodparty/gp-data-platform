-- Reads int__hubspot_owners_history: the never-delete layer, over a frozen Airbyte
-- feed plus Lakeflow.
--
-- Active means present in the live Lakeflow snapshot, not `archived = false` on the
-- history row, which keeps its last value and would read as active forever.
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
inner join
    (select distinct id from {{ source("lakeflow_source", "owners") }}) as live
    on history.id = live.id
