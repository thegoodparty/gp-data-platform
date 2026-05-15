with
    source as (
        select *
        from {{ source("airbyte_source", "ddhq_elections_gsheet_upcoming_races") }}
    )

select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,
    cast(race_id as int) as ddhq_race_id,
    state as state_postal_code,
    race_name,
    cast(election_date as date) as election_date,
    election_type,
    -- election_type carries both stage and special indicators
    -- (e.g. "Special Election Primary", "Special General Election").
    -- Map them onto boolean-like expressions for the shared macro.
    lower(
        {{
            derive_election_stage(
                "lower(election_type) like '%primary%'",
                "lower(election_type) like '%runoff%'",
                "election_type",
            )
        }}
    ) as election_stage
from source
