{{ config(tags=["archive"]) }}

-- Historical archive of candidates from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Uses companies-based model for better coverage (joins via companies.contacts field)
with
    ranked_candidates as (
        select
            tbl_hs_companies.contact_id as hubspot_contact_id,
            tbl_gp_user.id as prod_db_user_id,
            tbl_hs_companies.candidate_id_tier,

            -- Candidate information
            -- Data source follows the following hierarchy:
            -- gp db -> hs.companies (which already includes hs.contact fallbacks)
            coalesce(tbl_gp_user.first_name, tbl_hs_companies.first_name) as first_name,
            coalesce(tbl_gp_user.last_name, tbl_hs_companies.last_name) as last_name,
            coalesce(tbl_gp_user.name, tbl_hs_companies.full_name) as full_name,
            tbl_hs_companies.birth_date,
            tbl_hs_companies.state,
            coalesce(tbl_gp_user.email, tbl_hs_companies.email) as email,
            coalesce(tbl_gp_user.phone, tbl_hs_companies.phone_number) as phone_number,
            tbl_hs_companies.street_address,

            -- Social media links
            tbl_hs_companies.website_url,
            tbl_hs_companies.linkedin_url,
            tbl_hs_companies.twitter_handle,
            tbl_hs_companies.facebook_url,
            tbl_hs_companies.instagram_handle,

            -- metadata timestamps
            tbl_hs_companies.created_at,
            tbl_hs_companies.updated_at,

            -- Rank records by updated_at when prod_db_user_id is not null
            case
                when tbl_gp_user.id is not null
                then
                    row_number() over (
                        partition by
                            tbl_hs_companies.contact_id,
                            tbl_hs_companies.gp_candidacy_id,
                            tbl_gp_user.id
                        order by tbl_gp_user.updated_at desc
                    )
                else 1
            end as user_rank

        from {{ ref("int__hubspot_companies_w_contacts_2025") }} as tbl_hs_companies
        left join
            {{ ref("stg_airbyte_source__gp_api_db_user") }} as tbl_gp_user
            on tbl_hs_companies.contact_id = tbl_gp_user.meta_data:hubspotid::string
        qualify
            row_number() over (
                partition by tbl_hs_companies.gp_candidacy_id
                order by tbl_hs_companies.updated_at desc
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
