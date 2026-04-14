-- Civics mart election_stage table
-- Union of 2025 archive, 2026+ BallotReady (full universe), and net-new
-- TechSpeed election_stages not matched to any BR record.
--
-- BallotReady is the authoritative spine for election_stages (races).
-- TechSpeed stages only appear when they represent races BR doesn't cover.
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
    -- BallotReady: full universe of election_stages (authoritative spine)
    -- =========================================================================
    br_2026 as (
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
            updated_at,
            array('ballotready') as source_systems
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    -- =========================================================================
    -- TechSpeed net-new: election_stages not matched to any BR via ER clusters
    -- =========================================================================
    ts_matched_election_stage_ids as (
        select distinct ts_m.gp_election_stage_id
        from {{ ref("int__civics_cluster_members") }} as br_m
        inner join {{ ref("int__civics_cluster_members") }} as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    -- Remap gp_election_id for net-new TS stages whose parent election IS
    -- matched to a BR election (ensures valid FK into the election mart)
    election_remap as (
        select distinct ts_m.gp_election_id as ts_id, br_m.gp_election_id as br_id
        from {{ ref("int__civics_cluster_members") }} as br_m
        inner join {{ ref("int__civics_cluster_members") }} as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    ts_net_new as (
        select
            ts.gp_election_stage_id,
            coalesce(er.br_id, ts.gp_election_id) as gp_election_id,
            ts.br_race_id,
            ts.br_election_id,
            ts.br_position_id,
            ts.ddhq_race_id,
            ts.stage_type,
            ts.election_date,
            ts.election_name,
            ts.race_name,
            ts.is_primary,
            ts.is_runoff,
            ts.is_retention,
            ts.number_of_seats,
            ts.total_votes_cast,
            cast(null as string) as partisan_type,
            ts.filing_period_start_on,
            ts.filing_period_end_on,
            ts.filing_requirements,
            ts.filing_address,
            ts.filing_phone,
            ts.created_at,
            ts.updated_at,
            array('techspeed') as source_systems
        from {{ ref("int__civics_election_stage_techspeed") }} as ts
        left join election_remap as er on ts.gp_election_id = er.ts_id
        where
            ts.gp_election_stage_id not in (
                select gp_election_stage_id
                from ts_matched_election_stage_ids
                where gp_election_stage_id is not null
            )
    ),

    -- =========================================================================
    -- Combine all sources
    -- =========================================================================
    combined as (
        select *
        from archive_2025
        union all
        select *
        from br_2026
        union all
        select *
        from ts_net_new
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
