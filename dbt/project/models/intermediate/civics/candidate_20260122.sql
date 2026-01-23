{{
    config(
        materialized="table",
        tags=["intermediate", "civics", "candidate", "archive"],
    )
}}

-- Historical archive of candidates from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Inlines m_general__candidate_v2 logic for self-contained archive
with
    ranked_candidates as (
        select
            tbl_hs_contacts.contact_id as hubspot_contact_id,
            tbl_gp_user.id as prod_db_user_id,
            tbl_hs_contacts.candidate_id_tier,

            -- Candidate information
            -- Data source follows the following hierarchy:
            -- gp db -> hs.companies -> hs.contact
            coalesce(tbl_gp_user.first_name, tbl_hs_contacts.first_name) as first_name,
            coalesce(tbl_gp_user.last_name, tbl_hs_contacts.last_name) as last_name,
            coalesce(tbl_gp_user.name, tbl_hs_contacts.full_name) as full_name,
            tbl_hs_contacts.birth_date,
            tbl_hs_contacts.state,
            coalesce(tbl_gp_user.email, tbl_hs_contacts.email) as email,
            coalesce(tbl_gp_user.phone, tbl_hs_contacts.phone_number) as phone_number,
            tbl_hs_contacts.street_address,

            -- Social media links
            tbl_hs_contacts.website_url,
            tbl_hs_contacts.linkedin_url,
            tbl_hs_contacts.twitter_handle,
            tbl_hs_contacts.facebook_url,
            tbl_hs_contacts.instagram_handle,

            -- metadata timestamps
            tbl_hs_contacts.created_at,
            tbl_hs_contacts.updated_at,

            -- Rank records by updated_at when prod_db_user_id is not null
            case
                when tbl_gp_user.id is not null
                then
                    row_number() over (
                        partition by
                            tbl_hs_contacts.contact_id,
                            tbl_hs_contacts.gp_candidacy_id,
                            tbl_gp_user.id
                        order by tbl_gp_user.updated_at desc
                    )
                else 1
            end as user_rank

        from {{ ref("int__hubspot_contacts_w_companies_20260122") }} as tbl_hs_contacts
        left join
            {{ ref("stg_airbyte_source__gp_api_db_user") }} as tbl_gp_user
            on tbl_hs_contacts.contact_id = tbl_gp_user.meta_data:hubspotid::string
        qualify
            row_number() over (
                partition by tbl_hs_contacts.gp_candidacy_id
                order by tbl_hs_contacts.updated_at desc
            )
            = 1
    ),

    candidates_with_id as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "birth_date",
                        "email",
                        "phone_number",
                    ]
                )
            }} as gp_candidate_id,
            hubspot_contact_id,
            prod_db_user_id,
            candidate_id_tier,
            first_name,
            last_name,
            full_name,
            birth_date,
            state,
            email,
            phone_number,
            street_address,
            website_url,
            linkedin_url,
            twitter_handle,
            facebook_url,
            instagram_handle,
            created_at,
            updated_at
        from ranked_candidates
        where user_rank = 1
    ),

    -- Filter to elections on or before 2025-12-31
    archived_candidates as (
        select *
        from candidates_with_id
        qualify
            row_number() over (partition by gp_candidate_id order by updated_at desc)
            = 1
    )

select
    gp_candidate_id,
    hubspot_contact_id,
    prod_db_user_id,
    candidate_id_tier,
    first_name,
    last_name,
    full_name,
    birth_date,
    state,
    email,
    phone_number,
    street_address,
    website_url,
    linkedin_url,
    twitter_handle,
    facebook_url,
    instagram_handle,
    created_at,
    updated_at

from archived_candidates
