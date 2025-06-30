{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        on_schema_change="append",
    )
}}

with
    br_positions as (
        select distinct br_position_id from {{ ref("m_election_api__race") }}
    ),
    l2_matches as (
        select distinct br_position_id
        from {{ ref("m_election_api__projected_turnout") }}
    ),
    counts as (select count(*) as br_position_cnt from br_positions),
    matches as (select count(*) as matched_cnt from l2_matches)

select
    {{ dbt.current_timestamp() }} as run_timestamp,
    c.br_position_cnt,
    m.matched_cnt,
    m.matched_cnt::decimal / nullif(c.br_position_cnt, 0) as coverage_rate
from counts c
cross join matches m
