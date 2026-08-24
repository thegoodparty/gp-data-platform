-- Explicit because this directory sets no materialization default: an empty
-- config block would silently make this a view.
{{ config(materialized="table") }}

with
    br_offices as (
        select
            database_id as br_database_id,
            name,
            mtfcc,
            geo_id,
            sub_area_name,
            sub_area_value,
            is_judicial,
            has_unknown_boundaries,
            state
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    latest_attempt as (
        select
            br_database_id, l2_state, l2_district_type, l2_district_name, attempted_at
        from {{ source("model_predictions", "llm_l2_br_match_results") }}
        qualify
            row_number() over (partition by br_database_id order by attempted_at desc)
            = 1
    ),

    states_in_universe as (
        select distinct state_postal_code from {{ ref("int__l2_district_universe") }}
    )

select
    br_offices.br_database_id,
    br_offices.name,
    br_offices.mtfcc,
    br_offices.geo_id,
    br_offices.sub_area_name,
    br_offices.sub_area_value,
    br_offices.is_judicial,
    br_offices.has_unknown_boundaries,
    br_offices.state
from br_offices
left join latest_attempt on br_offices.br_database_id = latest_attempt.br_database_id
left join
    {{ ref("int__l2_district_universe") }} as universe
    on latest_attempt.l2_state = universe.state_postal_code
    -- Not redundant: a position that changes state must not keep a match to a
    -- same-named district in the state it left.
    and latest_attempt.l2_state = br_offices.state
    and latest_attempt.l2_district_type = universe.district_type
    and latest_attempt.l2_district_name = universe.district_name
where
    (
        -- A null attempted_at (never attempted) must reopen immediately, so the
        -- sentinel is older than the 30-day cutoff rather than newer.
        latest_attempt.l2_district_name is null
        and coalesce(latest_attempt.attempted_at, timestamp '1900-01-01')
        < current_date() - interval 30 days
    )
    or (
        latest_attempt.l2_district_name is not null
        and universe.district_name is null
        -- Without this, a stalled L2 load for one state would flood every
        -- matched office there back onto the list.
        and br_offices.state in (select state_postal_code from states_in_universe)
    )
