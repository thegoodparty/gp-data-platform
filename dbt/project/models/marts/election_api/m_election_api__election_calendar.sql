{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
    )
}}

-- Full rebuild every run, same as m_election_api__projected_turnout: this table is
-- delivered to Postgres via the sync_election_api DAG's atomic swap (build ->
-- staging -> quality gate -> swap), not the upsert-by-id writer, so there's no
-- supersession bookkeeping to preserve here.
--
-- General comes from the election_calendar seed (dbt/project/seeds/
-- election_calendar.csv) unconditionally -- computed once from a fixed federal
-- rule that never changes, so there's nothing to keep live.
--
-- Primary is authoritative from int__election_calendar_primary_ballotready,
-- which recomputes live from BallotReady's scheduled-election data every run --
-- an edit there (a corrected date, a newly scheduled state) flows through on
-- the next sync automatically. The seed's own Primary rows are used only as a
-- fallback for a (state, year) the live model doesn't resolve.
with general as (
    select state, election_date, election_code
    from {{ ref("election_calendar") }}
    where election_code = 'General'
),

seed_primary_fallback as (
    select state, election_date, year(election_date) as election_year
    from {{ ref("election_calendar") }}
    where election_code = 'Primary'
),

live_primary as (
    select state, election_date, year(election_date) as election_year
    from {{ ref("int__election_calendar_primary_ballotready") }}
),

primary_final as (
    select
        coalesce(live.state, fallback.state) as state,
        coalesce(live.election_date, fallback.election_date) as election_date,
        'Primary' as election_code
    from live_primary live
    full outer join seed_primary_fallback fallback
        on live.state = fallback.state and live.election_year = fallback.election_year
),

combined as (
    select * from general
    union all
    select * from primary_final
)

select
    {{
        generate_salted_uuid(
            fields=[
                "state",
                "election_date",
            ]
        )
    }} as id,
    now() as created_at,
    current_timestamp() as updated_at,
    state,
    cast(election_date as date) as election_date,
    election_code
from combined
