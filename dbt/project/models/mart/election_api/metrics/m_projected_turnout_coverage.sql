{{ config(materialized='incremental', on_schema_change='append') }}

with br_positions as (
    select distinct br_position_id
    from {{ ref('m_election_api__race')}}
),
l2_matches as (
    select distinct br_position_id
    from 'dbt_hugh.m_election-api_projected_turnout'
)

select 
    {{ dbt.current_timestamp() }}           as run_timestamp
    (select count(*) from br_races)         as br_race_cnt,
    (select count(*) from l2_matches)       as matched_cnt,
    matched_cnt::decimal / nullif()         as coverage_rate;