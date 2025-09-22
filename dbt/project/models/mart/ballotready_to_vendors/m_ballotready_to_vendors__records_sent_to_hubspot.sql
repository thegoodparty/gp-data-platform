{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="candidacy_id",
        auto_liquid_cluster=true,
        tags=["mart", "ballotready", "hubspot", "historical"],
    )
}}

-- Historical tracking table for BallotReady records sent to HubSpot
-- This table maintains an audit trail of all candidates that have been processed
-- for HubSpot upload, including fuzzy match results and viability scores
with
    br_fuzzy_deduped as (
        select * from {{ ref("int__ballotready_candidates_fuzzy_deduped") }}
    ),

    br_final_candidacies as (
        select * from {{ ref("int__ballotready_final_candidacies") }}
    ),

    -- Combine fuzzy match results with final candidacies
    combined_records as (
        select
            fc.id,
            fc.election_date,
            fc.official_office_name,
            fc.candidate_office,
            fc.office_level,
            fc.geographic_tier,
            fc.number_of_seats_available,
            fc.election_type,
            fc.party_affiliation,
            fc.party_list,
            fc.parties,
            fc.first_name,
            fc.middle_name,
            fc.last_name,
            fc.state,
            fc.phone,
            fc.email,
            fc.city,
            fc.district,
            fc.seat,
            fc.office_type,
            fc.type,
            fc.contact_owner,
            fc.owner_name,
            fc.candidate_id_source,
            fc.candidacy_id,
            fc.ballotready_race_id,
            fc.br_contest_id,
            fc.br_candidate_code,
            fc.uncontested,
            fc.number_of_candidates,
            fc.candidate_slug,
            fc.candidacy_created_at,
            fc.candidacy_updated_at,
            fc.created_at,
            fd.fuzzy_matched_hubspot_candidate_code,
            fd.fuzzy_match_score,
            fd.fuzzy_match_rank,
            fd.fuzzy_matched_hubspot_contact_id,
            fd.fuzzy_matched_first_name,
            fd.fuzzy_matched_last_name,
            fd.fuzzy_matched_state,
            fd.fuzzy_matched_office_type,
            fd.match_type,
            -- Placeholder for future viability score calculation
            cast(null as string) as viability_score
        from br_final_candidacies fc
        left join br_fuzzy_deduped fd on fc.br_candidate_code = fd.br_candidate_code
    )

select
    candidacy_id,
    ballotready_race_id,
    cast(election_date as date) as election_date,
    official_office_name,
    candidate_office,
    office_level,
    cast(geographic_tier as int) as geographic_tier,
    cast(number_of_seats_available as int) as number_of_seats_available,
    election_type,
    party_affiliation,
    party_list,
    parties,
    first_name,
    middle_name,
    last_name,
    state,
    phone,
    email,
    city,
    -- Apply district transformation here (from notebook)
    case
        when district like '%, %'
        then left(district, position(', ' in district) - 1)
        else district
    end as district,
    seat,
    office_type,
    type,
    contact_owner,
    owner_name,
    candidate_id_source,
    br_contest_id,
    br_candidate_code,
    uncontested,
    number_of_candidates,
    candidate_slug,
    -- Fuzzy match fields
    match_type,
    fuzzy_match_score,
    fuzzy_matched_hubspot_candidate_code,
    fuzzy_matched_hubspot_contact_id,
    fuzzy_matched_first_name,
    fuzzy_matched_last_name,
    fuzzy_matched_state,
    fuzzy_matched_office_type,
    -- Viability score placeholder
    viability_score,
    current_timestamp() as upload_timestamp
from combined_records
-- Only include records that haven't been processed yet
where
    candidacy_id not in (
        select candidacy_id
        from {{ ref("stg_historical__ballotready_records_sent_to_hubspot") }}
    )
    {% if is_incremental() %}
        and candidacy_id
        not in (select candidacy_id from {{ this }} where candidacy_id is not null)
    {% endif %}
qualify
    row_number() over (partition by candidacy_id order by candidacy_updated_at desc) = 1
