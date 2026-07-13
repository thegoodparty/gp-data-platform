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
-- Source is the election_calendar seed (dbt/project/seeds/election_calendar.csv),
-- not a model_predictions source table: this reference data is small (~150 rows)
-- and checked into git, same pattern as election_api_race_filing_address_overrides.
-- General rows are computed (Tuesday after the first Monday in November); Primary
-- rows come from each state's dominant L2 Primary_YYYY_MM_DD column for that year
-- (see seeds_schema.yaml for per-row provenance in source_note). CA is absent --
-- its L2 vote-history staging view is currently broken -- and only 6 states have a
-- confirmed 2026 Primary date (the rest haven't held theirs yet); both are
-- follow-up items, not bugs in this model.
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
from {{ ref("election_calendar") }}
