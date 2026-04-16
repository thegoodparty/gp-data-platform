-- Civics mart election table
-- Union of 2025 archive, 2026+ BallotReady (full universe), and net-new
-- TechSpeed elections not matched to any BR record.
--
-- BallotReady is the authoritative spine for elections (positions).
-- TechSpeed elections only appear when they represent positions BR doesn't cover.
with
    -- =========================================================================
    -- 2025 archive (pass-through)
    -- =========================================================================
    archive_2025 as (
        select
            gp_election_id,
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            state,
            city,
            district,
            seat_name,
            election_date,
            election_year,
            filing_deadline,
            population,
            seats_available,
            term_start_date,
            is_uncontested,
            number_of_opponents,
            is_open_seat,
            has_ddhq_match,
            br_position_database_id,
            is_judicial,
            is_appointed,
            br_normalized_position_type,
            created_at,
            updated_at,
            array('hubspot') as source_systems
        from {{ ref("int__civics_election_2025") }}
    ),

    -- =========================================================================
    -- BallotReady: full universe of elections (authoritative spine)
    -- =========================================================================
    br_2026 as (
        select
            gp_election_id,
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            state,
            city,
            district,
            seat_name,
            election_date,
            election_year,
            filing_deadline,
            population,
            seats_available,
            term_start_date,
            is_uncontested,
            number_of_opponents,
            is_open_seat,
            has_ddhq_match,
            br_position_database_id,
            is_judicial,
            is_appointed,
            br_normalized_position_type,
            created_at,
            updated_at,
            array('ballotready') as source_systems
        from {{ ref("int__civics_election_ballotready") }}
    ),

    -- =========================================================================
    -- TechSpeed net-new: elections not matched to any BR via ER clusters
    -- =========================================================================
    -- Derived from pre-computed remaps on cluster_members (no self-join needed)
    ts_matched_election_ids as (
        select distinct gp_election_id
        from {{ ref("int__civics_cluster_members") }}
        where source_name = 'techspeed' and remap_election_id is not null
    ),

    ts_net_new as (
        select
            gp_election_id,
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            state,
            city,
            district,
            seat_name,
            election_date,
            election_year,
            filing_deadline,
            population,
            seats_available,
            term_start_date,
            is_uncontested,
            number_of_opponents,
            is_open_seat,
            has_ddhq_match,
            br_position_database_id,
            is_judicial,
            is_appointed,
            br_normalized_position_type,
            created_at,
            updated_at,
            array('techspeed') as source_systems
        from {{ ref("int__civics_election_techspeed") }}
        where
            gp_election_id not in (
                select gp_election_id
                from ts_matched_election_ids
                where gp_election_id is not null
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
            row_number() over (partition by gp_election_id order by updated_at desc) = 1
    ),

    -- Pivot election stage dates by type
    stage_dates as (
        select
            gp_election_id,
            max(
                case when stage_type = 'primary' then election_date end
            ) as primary_election_date,
            max(
                case when stage_type = 'general' then election_date end
            ) as general_election_date,
            max(
                case when stage_type = 'primary runoff' then election_date end
            ) as primary_runoff_election_date,
            max(
                case when stage_type = 'general runoff' then election_date end
            ) as general_runoff_election_date
        from {{ ref("election_stage") }}
        group by gp_election_id
    )

select
    deduplicated.gp_election_id,
    deduplicated.official_office_name,
    deduplicated.candidate_office,
    deduplicated.office_level,
    deduplicated.office_type,
    deduplicated.state,
    deduplicated.city,
    deduplicated.district,
    deduplicated.seat_name,
    deduplicated.election_date,
    deduplicated.election_year,
    deduplicated.filing_deadline,
    deduplicated.population,
    deduplicated.seats_available,
    deduplicated.term_start_date,
    deduplicated.is_uncontested,
    deduplicated.number_of_opponents,
    deduplicated.is_open_seat,
    deduplicated.has_ddhq_match,
    deduplicated.br_position_database_id,
    deduplicated.is_judicial,
    deduplicated.is_appointed,
    deduplicated.br_normalized_position_type,
    stage_dates.primary_election_date,
    stage_dates.general_election_date,
    stage_dates.primary_runoff_election_date,
    stage_dates.general_runoff_election_date,
    icp.voter_count as icp_voter_count,
    icp.normalized_position_type as icp_normalized_position_name,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    icp.icp_win_supersize as is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_database_id = icp.br_database_position_id
left join stage_dates on deduplicated.gp_election_id = stage_dates.gp_election_id
