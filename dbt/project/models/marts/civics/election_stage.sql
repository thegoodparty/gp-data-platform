-- Civics mart election_stage table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
with
    -- =========================================================================
    -- 2025 archive (pass-through)
    -- =========================================================================
    archive_2025 as (
        select
            gp_election_stage_id,
            gp_election_id,
            cast(null as string) as br_race_id,
            cast(null as string) as br_election_id,
            cast(null as int) as br_position_id,
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
            cast(null as timestamp) as updated_at,
            array('hubspot') as source_systems
        from {{ ref("int__civics_election_stage_2025") }}
    ),

    -- =========================================================================
    -- Crosswalk pairs + FK remaps from shared intermediate
    -- =========================================================================
    election_stage_pairs as (
        select distinct br_election_stage_id as br_id, ts_election_stage_id as ts_id
        from {{ ref("int__civics_crosswalk") }}
    ),

    election_remap as (
        select distinct ts_election_id as ts_id, br_election_id as br_id
        from {{ ref("int__civics_crosswalk") }}
    ),

    -- =========================================================================
    -- Merge 2026+ BR + TS with survivorship (BR wins by default)
    -- =========================================================================
    merged_2026 as (
        select
            coalesce(
                br.gp_election_stage_id, ts.gp_election_stage_id
            ) as gp_election_stage_id,
            coalesce(
                br.gp_election_id, elec_r.br_id, ts.gp_election_id
            ) as gp_election_id,
            coalesce(br.br_race_id, ts.br_race_id) as br_race_id,
            coalesce(br.br_election_id, ts.br_election_id) as br_election_id,
            coalesce(br.br_position_id, ts.br_position_id) as br_position_id,
            coalesce(br.ddhq_race_id, ts.ddhq_race_id) as ddhq_race_id,
            coalesce(br.stage_type, ts.stage_type) as stage_type,
            coalesce(br.election_date, ts.election_date) as election_date,
            coalesce(br.election_name, ts.election_name) as election_name,
            coalesce(br.race_name, ts.race_name) as race_name,
            coalesce(br.is_primary, ts.is_primary) as is_primary,
            coalesce(br.is_runoff, ts.is_runoff) as is_runoff,
            coalesce(br.is_retention, ts.is_retention) as is_retention,
            coalesce(br.number_of_seats, ts.number_of_seats) as number_of_seats,
            coalesce(br.total_votes_cast, ts.total_votes_cast) as total_votes_cast,
            br.partisan_type,
            coalesce(
                br.filing_period_start_on, ts.filing_period_start_on
            ) as filing_period_start_on,
            coalesce(
                br.filing_period_end_on, ts.filing_period_end_on
            ) as filing_period_end_on,
            coalesce(
                br.filing_requirements, ts.filing_requirements
            ) as filing_requirements,
            coalesce(br.filing_address, ts.filing_address) as filing_address,
            coalesce(br.filing_phone, ts.filing_phone) as filing_phone,
            coalesce(br.created_at, ts.created_at) as created_at,
            coalesce(br.updated_at, ts.updated_at) as updated_at,
            array_compact(
                array(
                    case
                        when br.gp_election_stage_id is not null then 'ballotready'
                    end,
                    case when ts.gp_election_stage_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_election_stage_ballotready") }} as br
        full outer join election_stage_pairs as cw on br.gp_election_stage_id = cw.br_id
        full outer join
            {{ ref("int__civics_election_stage_techspeed") }} as ts
            on cw.ts_id = ts.gp_election_stage_id
        left join election_remap as elec_r on ts.gp_election_id = elec_r.ts_id
    ),

    -- =========================================================================
    -- Combine archive + merged 2026+
    -- =========================================================================
    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_2026
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
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    icp.icp_win_supersize as is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_id = icp.br_database_position_id
