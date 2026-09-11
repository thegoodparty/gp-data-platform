-- Canonical person groups published by matcha's person lane
-- (scripts/person_clustering.py). One row per person-graph record key with
-- the deterministic identity it closed into, the group the admitted Splink
-- merges put it in, and why a proposed merge was refused when one was. The
-- mint left-joins this to the record universe, so a record the vintage has not
-- seen stands alone under its own key.
with

    source as (select * from {{ source("er_source", "person_groups") }}),

    renamed as (

        select
            record_key,
            source_name,
            identity_key,
            person_group_key,
            nullif(rejected_reason, '') as rejected_reason
        from source

    )

select *
from renamed
