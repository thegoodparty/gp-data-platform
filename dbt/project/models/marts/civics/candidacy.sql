-- Civics mart candidacy table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
with
    -- =========================================================================
    -- 2025 archive (pass-through)
    -- =========================================================================
    archive_2025 as (
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_incumbent,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            br_position_database_id,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at,
            array('hubspot') as source_systems
        from {{ ref("int__civics_candidacy_2025") }}
    ),

    -- =========================================================================
    -- Crosswalk pairs + FK remaps from shared intermediate
    -- =========================================================================
    candidacy_pairs as (
        select distinct br_candidacy_id as br_id, ts_candidacy_id as ts_id
        from {{ ref("int__civics_crosswalk") }}
    ),

    candidate_remap as (
        select distinct ts_candidate_id as ts_id, br_candidate_id as br_id
        from {{ ref("int__civics_crosswalk") }}
    ),

    election_remap as (
        select distinct ts_election_id as ts_id, br_election_id as br_id
        from {{ ref("int__civics_crosswalk") }}
    ),

    -- =========================================================================
    -- Merge 2026+ BR + TS with survivorship
    -- BR wins by default; TS wins for is_incumbent
    -- =========================================================================
    merged_2026 as (
        select
            coalesce(br.gp_candidacy_id, ts.gp_candidacy_id) as gp_candidacy_id,
            coalesce(
                br.gp_candidate_id, cand_r.br_id, ts.gp_candidate_id
            ) as gp_candidate_id,
            coalesce(
                br.gp_election_id, elec_r.br_id, ts.gp_election_id
            ) as gp_election_id,
            coalesce(
                br.product_campaign_id, ts.product_campaign_id
            ) as product_campaign_id,
            coalesce(
                br.hubspot_contact_id, ts.hubspot_contact_id
            ) as hubspot_contact_id,
            coalesce(
                br.hubspot_company_ids, ts.hubspot_company_ids
            ) as hubspot_company_ids,
            coalesce(
                br.candidate_id_source, ts.candidate_id_source
            ) as candidate_id_source,
            coalesce(br.party_affiliation, ts.party_affiliation) as party_affiliation,
            -- TS wins for is_incumbent (TS: 51k, BR: 0)
            coalesce(ts.is_incumbent, br.is_incumbent) as is_incumbent,
            coalesce(br.is_open_seat, ts.is_open_seat) as is_open_seat,
            coalesce(br.candidate_office, ts.candidate_office) as candidate_office,
            coalesce(
                br.official_office_name, ts.official_office_name
            ) as official_office_name,
            coalesce(br.office_level, ts.office_level) as office_level,
            br.office_type,
            coalesce(br.candidacy_result, ts.candidacy_result) as candidacy_result,
            coalesce(br.is_pledged, ts.is_pledged) as is_pledged,
            coalesce(br.is_verified, ts.is_verified) as is_verified,
            coalesce(
                br.verification_status_reason, ts.verification_status_reason
            ) as verification_status_reason,
            coalesce(br.is_partisan, ts.is_partisan) as is_partisan,
            coalesce(
                br.primary_election_date, ts.primary_election_date
            ) as primary_election_date,
            coalesce(
                br.primary_runoff_election_date, ts.primary_runoff_election_date
            ) as primary_runoff_election_date,
            coalesce(
                br.general_election_date, ts.general_election_date
            ) as general_election_date,
            coalesce(
                br.general_runoff_election_date, ts.general_runoff_election_date
            ) as general_runoff_election_date,
            br.br_position_database_id,
            coalesce(br.viability_score, ts.viability_score) as viability_score,
            coalesce(br.win_number, ts.win_number) as win_number,
            coalesce(br.win_number_model, ts.win_number_model) as win_number_model,
            coalesce(br.created_at, ts.created_at) as created_at,
            coalesce(br.updated_at, ts.updated_at) as updated_at,
            array_compact(
                array(
                    case when br.gp_candidacy_id is not null then 'ballotready' end,
                    case when ts.gp_candidacy_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidacy_ballotready") }} as br
        full outer join candidacy_pairs as cw on br.gp_candidacy_id = cw.br_id
        full outer join
            {{ ref("int__civics_candidacy_techspeed") }} as ts
            on cw.ts_id = ts.gp_candidacy_id
        left join candidate_remap as cand_r on ts.gp_candidate_id = cand_r.ts_id
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
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    deduplicated.gp_candidacy_id,
    deduplicated.gp_candidate_id,
    deduplicated.gp_election_id,
    deduplicated.product_campaign_id,
    deduplicated.hubspot_contact_id,
    deduplicated.hubspot_company_ids,
    deduplicated.candidate_id_source,
    deduplicated.party_affiliation,
    deduplicated.is_incumbent,
    deduplicated.is_open_seat,
    deduplicated.candidate_office,
    deduplicated.official_office_name,
    deduplicated.office_level,
    deduplicated.office_type,
    deduplicated.candidacy_result,
    deduplicated.is_pledged,
    deduplicated.is_verified,
    deduplicated.verification_status_reason,
    deduplicated.is_partisan,
    deduplicated.primary_election_date,
    deduplicated.primary_runoff_election_date,
    deduplicated.general_election_date,
    deduplicated.general_runoff_election_date,
    deduplicated.br_position_database_id,
    deduplicated.viability_score,
    deduplicated.win_number,
    deduplicated.win_number_model,
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
