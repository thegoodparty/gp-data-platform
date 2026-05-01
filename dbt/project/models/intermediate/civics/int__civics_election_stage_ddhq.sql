-- DDHQ → Civics mart election_stage
-- Derived from: int__civics_candidacy_stage_ddhq
--
-- Grain: One row per DDHQ race (gp_election_stage_id / ddhq_race_id)
--
-- Aggregates from candidate-level candidacy_stage rows to race-level.
with
    source as (select * from {{ ref("int__civics_candidacy_stage_ddhq") }}),

    election_stages as (
        select
            gp_election_stage_id,
            any_value(gp_election_id) as gp_election_id,

            cast(null as string) as br_race_id,
            cast(null as string) as br_election_id,
            cast(null as bigint) as br_position_id,
            any_value(source_race_id) as ddhq_race_id,

            any_value(election_stage) as stage_type,
            any_value(election_date) as election_date,

            any_value(state) as state,
            any_value(state_postal_code) as state_postal_code,

            any_value(state_postal_code)
            || ' '
            || cast(year(any_value(election_date)) as string)
            || ' '
            || initcap(any_value(election_stage)) as election_name,
            any_value(race_name) as race_name,

            any_value(election_stage) in ('primary', 'primary runoff') as is_primary,
            any_value(election_stage)
            in ('general runoff', 'primary runoff') as is_runoff,
            false as is_retention,
            any_value(number_of_seats_in_election) as number_of_seats,
            any_value(total_number_of_ballots_in_race) as total_votes_cast,
            cast(null as string) as partisan_type,
            cast(null as date) as filing_period_start_on,
            cast(null as date) as filing_period_end_on,
            cast(null as string) as filing_requirements,
            cast(null as string) as filing_address,
            cast(null as string) as filing_phone,
            min(created_at) as created_at,
            max(updated_at) as updated_at

        from source
        group by gp_election_stage_id
    )

select *
from election_stages
