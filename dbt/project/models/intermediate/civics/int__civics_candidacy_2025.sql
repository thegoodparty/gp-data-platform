{{ config(tags=["archive"]) }}

-- Historical archive of candidacies from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Uses companies-based model for better coverage (joins via companies.contacts field)
with
    candidacies as (
        select
            -- Identifiers
            tbl_companies.gp_candidacy_id,
            'candidacy_id-tbd' as candidacy_id,
            tbl_candidates.gp_candidate_id as gp_candidate_id,
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_companies.product_campaign_id,
            tbl_companies.contact_id as hubspot_contact_id,
            tbl_companies.extra_companies as hubspot_company_ids,
            tbl_companies.candidate_id_source,

            -- candidacy information
            tbl_companies.party_affiliation,
            tbl_companies.is_incumbent,
            tbl_companies.is_open_seat,
            tbl_companies.candidate_office,
            tbl_companies.official_office_name,
            tbl_companies.office_level,
            tbl_companies.candidacy_result,
            tbl_companies.is_pledged,
            case
                when tbl_companies.verified_candidate = 'Yes' then true else false
            end as is_verified,
            case
                when tbl_companies.verified_candidate != 'Yes'
                then tbl_companies.verified_candidate
                else null
            end as verification_status_reason,
            tbl_companies.is_partisan,
            tbl_companies.primary_election_date,
            tbl_companies.general_election_date,
            tbl_companies.runoff_election_date,

            -- BallotReady
            tbl_companies.br_position_database_id,

            -- assessments
            viability_scores.viability_rating_2_0 as viability_score,
            cast(cast(tbl_companies.win_number as float) as int) as win_number,
            tbl_companies.win_number_model,

            -- loading data
            tbl_companies.created_at,
            tbl_companies.updated_at

        from {{ ref("int__hubspot_companies_w_contacts_2025") }} as tbl_companies
        inner join
            {{ ref("int__civics_candidate_2025") }} as tbl_candidates
            on tbl_companies.contact_id = tbl_candidates.hubspot_contact_id
        left join
            {{ ref("int__hubspot_contest_2025") }} as tbl_contest
            on tbl_contest.contact_id = tbl_companies.contact_id
        left join
            {{ ref("stg_model_predictions__viability_scores") }} as viability_scores
            on tbl_companies.company_id = viability_scores.id
    ),

    -- Filter to elections on or before 2025-12-31
    -- INNER JOIN to candidates excludes companies without contacts and contacts
    -- that deduplicate to another candidate
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
    -- Normalize candidacy_result: empty strings to NULL, Won/Lost General to Won/Lost
    case
        when candidacy_result = ''
        then null
        when candidacy_result = 'Won General'
        then 'Won'
        when candidacy_result = 'Lost General'
        then 'Lost'
        else candidacy_result
    end as candidacy_result,
    is_pledged,
    is_verified,
    verification_status_reason,
    is_partisan,
    primary_election_date,
    general_election_date,
    runoff_election_date,
    br_position_database_id,
    viability_score,
    win_number,
    win_number_model,
    created_at,
    updated_at

from archived_candidacies
