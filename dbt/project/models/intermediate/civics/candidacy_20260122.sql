{{
    config(
        materialized="table",
        tags=["intermediate", "civics", "candidacy", "archive"],
    )
}}

-- Historical archive of candidacies from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Inlines m_general__candidacy_v2 logic for self-contained archive
with
    candidacies as (
        select
            -- Identifiers
            tbl_contacts.gp_candidacy_id,
            'candidacy_id-tbd' as candidacy_id,
            tbl_candidates.gp_candidate_id as gp_candidate_id,
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_contacts.product_campaign_id,
            tbl_contacts.contact_id as hubspot_contact_id,
            tbl_contacts.extra_companies as hubspot_company_ids,
            tbl_contacts.candidate_id_source,

            -- candidacy information
            tbl_contacts.party_affiliation,
            tbl_contacts.is_incumbent,
            tbl_contacts.is_open_seat,
            tbl_contacts.candidate_office,
            tbl_contacts.official_office_name,
            tbl_contacts.office_level,
            tbl_contacts.candidacy_result,
            tbl_contacts.pledge_status,
            tbl_contacts.verified_candidate,
            tbl_contacts.is_partisan,
            tbl_contacts.primary_election_date,
            tbl_contacts.general_election_date,
            tbl_contacts.runoff_election_date,

            -- assessments
            viability_scores.viability_rating_2_0 as viability_score,
            cast(cast(tbl_contacts.win_number as float) as int) as win_number,
            cast(
                cast(tbl_contacts.win_number_model as float) as int
            ) as win_number_model,

            -- loading data
            tbl_contacts.created_at,
            tbl_contacts.updated_at

        from {{ ref("int__hubspot_contacts_w_companies_20260122") }} as tbl_contacts
        left join
            {{ ref("candidate_20260122") }} as tbl_candidates
            on tbl_contacts.contact_id = tbl_candidates.hubspot_contact_id
        left join
            {{ ref("int__hubspot_contest_20260122") }} as tbl_contest
            on tbl_contest.contact_id = tbl_contacts.contact_id
        left join
            {{ ref("stg_model_predictions__viability_scores") }} as viability_scores
            on tbl_contacts.company_id = viability_scores.id
    ),

    -- Filter to elections on or before 2025-12-31
    archived_candidacies as (
        select *
        from candidacies
        where
            general_election_date <= '2025-12-31'
            and general_election_date >= '1900-01-01'
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    gp_candidacy_id,
    candidacy_id,
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
    pledge_status,
    verified_candidate,
    is_partisan,
    primary_election_date,
    general_election_date,
    runoff_election_date,
    viability_score,
    win_number,
    win_number_model,
    created_at,
    updated_at

from archived_candidacies
