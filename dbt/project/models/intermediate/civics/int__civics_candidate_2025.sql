{{ config(tags=["archive"]) }}

-- Historical archive of candidates from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Uses companies-based model for better coverage (joins via companies.contacts field)
with
    -- Per-candidacy DDHQ match flag from the legacy LLM matcher. Joined on
    -- the company-row's gp_candidacy_id; propagated to candidate grain via
    -- a bool_or window once gp_candidate_id is known. We don't reuse
    -- int__civics_candidacy_2025 here to avoid a circular dependency
    -- (candidacy_2025 already imports candidate_2025).
    ddhq_match_by_candidacy as (
        select gp_candidacy_id, bool_or(coalesce(has_match, false)) as has_ddhq_match
        from {{ ref("int__gp_ai_election_match") }}
        where gp_candidacy_id is not null
        group by gp_candidacy_id
    ),

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

            -- Per-row DDHQ flag; rolled up to candidate grain in candidates_with_id.
            coalesce(tbl_ddhq_match.has_ddhq_match, false) as company_has_ddhq_match,

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
        left join
            ddhq_match_by_candidacy as tbl_ddhq_match
            on tbl_hs_companies.gp_candidacy_id = tbl_ddhq_match.gp_candidacy_id
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
            -- Candidate-grain DDHQ flag: true if any of the candidate's
            -- candidacies had a DDHQ match. Computed before the final
            -- gp_candidate_id dedup so all surviving rows agree.
            bool_or(company_has_ddhq_match) over (
                partition by
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
                    }}
            ) as has_ddhq_match,
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
    archived_candidates.gp_candidate_id,
    archived_candidates.hubspot_contact_id,
    archived_candidates.prod_db_user_id,
    archived_candidates.candidate_id_tier,
    archived_candidates.first_name,
    archived_candidates.last_name,
    archived_candidates.full_name,
    archived_candidates.birth_date,
    tbl_states.state_cleaned_postal_code as state,
    archived_candidates.email,
    archived_candidates.phone_number,
    archived_candidates.street_address,
    archived_candidates.website_url,
    archived_candidates.linkedin_url,
    archived_candidates.twitter_handle,
    archived_candidates.facebook_url,
    archived_candidates.instagram_handle,
    archived_candidates.has_ddhq_match,
    archived_candidates.created_at,
    archived_candidates.updated_at

from archived_candidates
left join
    {{ ref("clean_states") }} as tbl_states
    on trim(upper(archived_candidates.state)) = tbl_states.state_raw
