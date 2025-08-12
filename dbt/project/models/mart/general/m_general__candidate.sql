{{
    config(
        materialized="incremental",
        unique_key=["hubspot_contact_id", "gp_candidacy_id"],
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "candidacy", "hubspot"],
    )
}}

-- TODO:
-- 1. confirm all id columns are set
-- 2. add tests for them:
-- id
-- gp_candidate_id_v1
-- hubspot_contact_id
-- prodbd_user_id
-- gp_candidacy_id
-- gp_contest_id
with
    ranked_candidates as (
        select
            tbl_hs_contacts.contact_id as hubspot_contact_id,
            tbl_hs_contacts.gp_candidacy_id,
            tbl_gp_user.id as prod_db_user_id,
            tbl_hs_contacts.candidate_id_source,
            tbl_contest.gp_contest_id,

            /*
        -- Candidate information
        -- Data source follows the following hierarchy:
        -- gp db -> hs.companies -> hs.contact
        */
            coalesce(tbl_gp_user.name, tbl_hs_contacts.full_name) as candidate_name,
            coalesce(tbl_gp_user.first_name, tbl_hs_contacts.first_name) as first_name,
            coalesce(tbl_gp_user.last_name, tbl_hs_contacts.last_name) as last_name,
            tbl_hs_contacts.birth_date,
            coalesce(tbl_gp_user.email, tbl_hs_contacts.email) as candidate_email,
            coalesce(tbl_gp_user.phone, tbl_hs_contacts.phone_number) as phone_number,

            /*
        -- Social media links
        -- Hubspot > HS contact
        */
            tbl_hs_contacts.linkedin_url,
            tbl_hs_contacts.instagram_handle,
            tbl_hs_contacts.twitter_handle,
            tbl_hs_contacts.facebook_url,
            tbl_hs_contacts.website_url,
            tbl_hs_contacts.street_address,

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

        from {{ ref("int__hubspot_contacts_w_companies") }} as tbl_hs_contacts
        left join
            {{ ref("stg_airbyte_source__gp_api_db_user") }} as tbl_gp_user
            on tbl_hs_contacts.contact_id = tbl_gp_user.meta_data:hubspotid::string
        left join
            {{ ref("m_general__contest") }} as tbl_contest
            on tbl_hs_contacts.contact_id = tbl_contest.contact_id
        {% if is_incremental() %}
            where tbl_hs_contacts.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )

select
    hubspot_contact_id,
    gp_candidacy_id,
    prod_db_user_id,
    candidate_id_source,
    gp_contest_id,
    candidate_name,
    first_name,
    last_name,
    birth_date,
    candidate_email,
    phone_number,
    linkedin_url,
    instagram_handle,
    twitter_handle,
    facebook_url,
    website_url,
    street_address,
    created_at,
    updated_at
from ranked_candidates
where user_rank = 1
