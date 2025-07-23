{{
    config(
        materialized="incremental",
        unique_key="gp_candidacy_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "candidacy", "hubspot"],
    )
}}

-- Final candidacy objects with viability scores
select
    -- Identifiers
    tbl_contacts.gp_candidacy_id,
    "candidacy_id-tbd" as candidacy_id,
    "gp_user_id-tbd" as gp_user_id,
    "gp_contest_id-tbd" as gp_contest_id,
    tbl_contacts.company_id as company_id,
    tbl_contacts.company_id as companies_id_main,
    tbl_contacts.candidate_id_source,
    tbl_contacts.candidate_id_tier,

    -- Personal information
    tbl_contacts.first_name,
    tbl_contacts.last_name,
    tbl_contacts.full_name,
    tbl_contacts.birth_date,
    tbl_contacts.email,
    tbl_contacts.phone_number,

    -- Digital presence
    tbl_contacts.website_url,
    tbl_contacts.linkedin_url,
    tbl_contacts.twitter_handle,
    tbl_contacts.facebook_url,
    tbl_contacts.instagram_handle,

    -- Location
    tbl_contacts.street_address,

    -- Office information
    tbl_contacts.official_office_name,
    tbl_contacts.candidate_office,
    tbl_contacts.office_level,
    tbl_contacts.office_type,
    tbl_contacts.party_affiliation,
    tbl_contacts.is_partisan,

    -- Geographic representation
    tbl_contacts.state,
    tbl_contacts.city,
    tbl_contacts.district,
    tbl_contacts.seat,
    tbl_contacts.population,

    -- Election timeline
    tbl_contacts.filing_deadline,
    tbl_contacts.primary_election_date,
    tbl_contacts.general_election_date,
    tbl_contacts.runoff_election_date,

    -- Election context
    tbl_contacts.is_incumbent,
    tbl_contacts.is_uncontested,
    tbl_contacts.number_of_opponents,
    tbl_contacts.is_open_seat,
    tbl_contacts.candidacy_result,

    -- Viability assessment
    viability_scores.viability_rating_2_0 as viability_score,

    -- Metadata
    tbl_contacts.created_at,
    tbl_contacts.updated_at

from {{ ref("int__hubspot_contacts_w_companies") }} as tbl_contacts
left join
    {{ ref("stg_model_predictions__viability_scores") }} as viability_scores
    on tbl_contacts.company_id = viability_scores.id

{% if is_incremental() %}
    where tbl_contacts.updated_at > (select max(updated_at) from {{ this }})
{% endif %}
