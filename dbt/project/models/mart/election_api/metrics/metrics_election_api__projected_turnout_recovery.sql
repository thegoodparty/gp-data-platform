{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        on_schema_change="append",
    )
}}

with
    missing_before as (
        select get_json_object(details, '$.positionId')::string as br_position_id
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}
        where
            get_json_object(data, '$.hubSpotUpdates.path_to_victory_status')
            in ('Locked', 'Waiting', 'Failed')
    ),
    recovered as (
        select count(distinct br_position_id) as recovered_cnt
        from {{ ref("m_election_api__projected_turnout") }} as cur
        where
            cur.br_position_id in (select br_position_id from missing_before)
            and cur.projected_turnout is not null
    ),
    tot as (select count(distinct br_position_id) as total_cnt from missing_before)
select
    {{ dbt.current_timestamp() }} as run_timestamp,
    total_cnt,
    recovered_cnt,
    case
        when total_cnt = 0 then null else recovered_cnt::decimal / total_cnt
    end as recovery_rate
from recovered, tot
