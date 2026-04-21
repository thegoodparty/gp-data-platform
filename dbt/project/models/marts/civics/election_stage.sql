-- Civics mart election_stage table
-- Union of 2025 HubSpot archive and 2026+ BallotReady data
with
    combined as (
        select
            gp_election_stage_id,
            gp_election_id,
            cast(null as string) as br_race_id,
            cast(null as string) as br_election_id,
            br_position_id,
            ddhq_race_id,
            election_stage as stage_type,
            ddhq_election_stage_date as election_date,
            cast(null as string) as election_name,
            ddhq_race_name as race_name,
            election_stage in ('primary', 'primary runoff') as is_primary,
            election_stage in ('general runoff', 'primary runoff') as is_runoff,
            cast(null as boolean) as is_retention,
            cast(null as int) as number_of_seats,
            total_votes_cast,
            cast(null as string) as partisan_type,
            cast(null as date) as filing_period_start_on,
            cast(null as date) as filing_period_end_on,
            cast(null as string) as filing_requirements,
            cast(null as string) as filing_address,
            cast(null as string) as filing_phone,
            created_at,
            cast(null as timestamp) as updated_at
        from {{ ref("int__civics_election_stage_2025") }}

        union all

        select
            gp_election_stage_id,
            gp_election_id,
            br_race_id,
            br_election_id,
            br_position_id,
            ddhq_race_id,
            stage_type,
            election_date,
            election_name,
            race_name,
            is_primary,
            is_runoff,
            is_retention,
            number_of_seats,
            total_votes_cast,
            partisan_type,
            filing_period_start_on,
            filing_period_end_on,
            filing_requirements,
            filing_address,
            filing_phone,
            created_at,
            updated_at
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (
                partition by gp_election_stage_id order by created_at desc
            )
            = 1
    )

select
    deduplicated.gp_election_stage_id,
    deduplicated.gp_election_id,
    deduplicated.br_race_id,
    deduplicated.br_election_id,
    deduplicated.br_position_id,
    deduplicated.ddhq_race_id,
    deduplicated.stage_type,
    deduplicated.election_date,
    deduplicated.election_name,
    deduplicated.race_name,
    deduplicated.is_primary,
    deduplicated.is_runoff,
    deduplicated.is_retention,
    deduplicated.number_of_seats,
    deduplicated.total_votes_cast,
    deduplicated.partisan_type,
    deduplicated.filing_period_start_on,
    deduplicated.filing_period_end_on,
    deduplicated.filing_requirements,
    deduplicated.filing_address,
    deduplicated.filing_phone,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                deduplicated.election_date is null
                or deduplicated.election_date < icp.icp_win_effective_date
            )
        then false
        else icp.icp_office_win
    end as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                deduplicated.election_date is null
                or deduplicated.election_date < icp.icp_win_effective_date
            )
        then false
        else icp.icp_win_supersize
    end as is_win_supersize_icp,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_id = icp.br_database_position_id
