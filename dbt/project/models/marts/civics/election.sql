-- Civics mart election table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
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
    -- Crosswalk pairs from shared intermediate
    -- =========================================================================
    election_pairs as (
        select distinct br_election_id as br_id, ts_election_id as ts_id
        from {{ ref("int__civics_crosswalk") }}
    ),

    -- =========================================================================
    -- Merge 2026+ BR + TS with survivorship
    -- BR wins for most fields; TS wins for filing_deadline
    -- =========================================================================
    merged_2026 as (
        select
            coalesce(br.gp_election_id, ts.gp_election_id) as gp_election_id,
            coalesce(
                br.official_office_name, ts.official_office_name
            ) as official_office_name,
            coalesce(br.candidate_office, ts.candidate_office) as candidate_office,
            coalesce(br.office_level, ts.office_level) as office_level,
            coalesce(br.office_type, ts.office_type) as office_type,
            coalesce(br.state, ts.state) as state,
            coalesce(br.city, ts.city) as city,
            coalesce(br.district, ts.district) as district,
            coalesce(br.seat_name, ts.seat_name) as seat_name,
            coalesce(br.election_date, ts.election_date) as election_date,
            coalesce(br.election_year, ts.election_year) as election_year,
            -- TS wins for filing_deadline (TS: 25k, BR: 0)
            coalesce(ts.filing_deadline, br.filing_deadline) as filing_deadline,
            coalesce(br.population, ts.population) as population,
            coalesce(br.seats_available, ts.seats_available) as seats_available,
            coalesce(br.term_start_date, ts.term_start_date) as term_start_date,
            coalesce(br.is_uncontested, ts.is_uncontested) as is_uncontested,
            coalesce(
                br.number_of_opponents, ts.number_of_opponents
            ) as number_of_opponents,
            coalesce(br.is_open_seat, ts.is_open_seat) as is_open_seat,
            coalesce(br.has_ddhq_match, ts.has_ddhq_match) as has_ddhq_match,
            coalesce(
                br.br_position_database_id, ts.br_position_database_id
            ) as br_position_database_id,
            coalesce(br.is_judicial, ts.is_judicial) as is_judicial,
            coalesce(br.is_appointed, ts.is_appointed) as is_appointed,
            coalesce(
                br.br_normalized_position_type, ts.br_normalized_position_type
            ) as br_normalized_position_type,
            coalesce(br.created_at, ts.created_at) as created_at,
            coalesce(br.updated_at, ts.updated_at) as updated_at,
            array_compact(
                array(
                    case when br.gp_election_id is not null then 'ballotready' end,
                    case when ts.gp_election_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_election_ballotready") }} as br
        full outer join election_pairs as cw on br.gp_election_id = cw.br_id
        full outer join
            {{ ref("int__civics_election_techspeed") }} as ts
            on cw.ts_id = ts.gp_election_id
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
    case
        when
            icp.icp_win_effective_date is not null
            and (
                coalesce(stage_dates.general_election_date, deduplicated.election_date)
                is null
                or coalesce(
                    stage_dates.general_election_date, deduplicated.election_date
                )
                < icp.icp_win_effective_date
            )
        then false
        else icp.icp_office_win
    end as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                coalesce(stage_dates.general_election_date, deduplicated.election_date)
                is null
                or coalesce(
                    stage_dates.general_election_date, deduplicated.election_date
                )
                < icp.icp_win_effective_date
            )
        then false
        else icp.icp_win_supersize
    end as is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_database_id = icp.br_database_position_id
left join stage_dates on deduplicated.gp_election_id = stage_dates.gp_election_id
