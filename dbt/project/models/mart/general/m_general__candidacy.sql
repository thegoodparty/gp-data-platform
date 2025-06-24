{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="companies_id_main",
        merge_update_condition="DBT_INTERNAL_SOURCE.updated_at > DBT_INTERNAL_DEST.updated_at",
        auto_liquid_cluster=true,
        tags=["mart", "candidacy", "hubspot"],
    )
}}

-- Final candidacy objects with viability scores
select
    -- Identifiers
    companies_with_contacts.gp_candidacy_id,
    companies_with_contacts.candidacy_id,
    companies_with_contacts.gp_user_id,
    companies_with_contacts.gp_constest_id,
    companies_with_contacts.companies_id_main,
    companies_with_contacts.candidate_id_source,
    companies_with_contacts.candidate_id_tier,

    -- Personal information
    companies_with_contacts.first_name,
    companies_with_contacts.last_name,
    companies_with_contacts.full_name,
    companies_with_contacts.birth_date,
    companies_with_contacts.email,
    companies_with_contacts.phone_number,

    -- Digital presence
    companies_with_contacts.website_url,
    companies_with_contacts.linkedin_url,
    companies_with_contacts.twitter_handle,
    companies_with_contacts.facebook_url,
    companies_with_contacts.instagram_handle,

    -- Location
    companies_with_contacts.street_address,

    -- Office information
    companies_with_contacts.official_office_name,
    companies_with_contacts.candidate_office,
    companies_with_contacts.office_level,
    companies_with_contacts.office_type,
    companies_with_contacts.party_affiliation,
    companies_with_contacts.is_partisan,

    -- Geographic representation
    companies_with_contacts.state,
    companies_with_contacts.city,
    companies_with_contacts.district,
    companies_with_contacts.seat,
    companies_with_contacts.population,

    -- Election timeline
    companies_with_contacts.filing_deadline,
    companies_with_contacts.primary_election_date,
    companies_with_contacts.general_election_date,
    companies_with_contacts.runoff_election_date,

    -- Election context
    companies_with_contacts.is_incumbent,
    companies_with_contacts.is_uncontested,
    companies_with_contacts.number_of_opponents,
    companies_with_contacts.is_open_seat,
    companies_with_contacts.candidacy_result,

    -- Viability assessment
    viability_scores.viability_rating_2_0 as viability_score,

    -- Metadata
    companies_with_contacts.created_at,
    companies_with_contacts.updated_at

from {{ ref("int__companies_with_contacts") }} as companies_with_contacts
left join
    {{ ref("stg_model_predictions__viability_scores") }} as viability_scores
    on companies_with_contacts.companies_id_main = viability_scores.id

{% if is_incremental() %}
    where companies_with_contacts.updated_at > (select max(updated_at) from {{ this }})
{% endif %}
