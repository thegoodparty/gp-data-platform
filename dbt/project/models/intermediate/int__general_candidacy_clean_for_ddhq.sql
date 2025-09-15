{{
    config(
        materialized="incremental",
        unique_key="gp_candidacy_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "candidacy", "hubspot", "ddhq"],
    )
}}

-- Final candidacy objects with viability scores
with
    candidacies as (
        select
            -- Identifiers
            gp_candidacy_id,
            product_campaign_id,
            candidacy_id,
            gp_contest_id,
            company_id,
            companies_id_main,
            contact_id,
            candidate_id_source,
            candidate_id_tier,

            -- Personal information
            {{ clean_name_for_ddhq("first_name") }} as first_name,
            {{ clean_name_for_ddhq("last_name") }} as last_name,
            full_name,
            birth_date,
            email,
            phone_number,

            -- Digital presence
            website_url,
            linkedin_url,
            twitter_handle,
            facebook_url,
            instagram_handle,

            -- Location
            street_address,

            -- Office information
            {{ clean_office_for_ddhq("official_office_name") }} as official_office_name,
            {{ clean_office_for_ddhq("candidate_office") }} as candidate_office,
            office_level,
            office_type,
            party_affiliation,
            is_partisan,

            -- Geographic representation
            state,
            city,
            district,
            seat,
            population,

            -- Election timeline
            filing_deadline,
            primary_election_date,
            general_election_date,
            runoff_election_date,

            -- Election context
            is_incumbent,
            is_uncontested,
            number_of_opponents,
            is_open_seat,
            candidacy_result,

            -- Assessments
            viability_score,
            win_number,
            win_number_model,

            -- -- DDHQ matches
            -- tbl_ddhq_matches.ddhq_candidate,
            -- tbl_ddhq_matches.ddhq_race_name,
            -- tbl_ddhq_matches.ddhq_candidate_party,
            -- tbl_ddhq_matches.ddhq_is_winner,
            -- tbl_ddhq_matches.ddhq_race_id,
            -- tbl_ddhq_matches.ddhq_election_type,
            -- tbl_ddhq_matches.ddhq_date,
            -- tbl_ddhq_matches.llm_confidence as ddhq_llm_confidence,
            -- tbl_ddhq_matches.llm_reasoning as ddhq_llm_reasoning,
            -- tbl_ddhq_matches.top_10_candidates as ddhq_top_10_candidates,
            -- tbl_ddhq_matches.has_match as ddhq_has_match,
            -- Metadata
            created_at,
            updated_at

        from {{ ref("int__general_candidacy") }} as tbl_contacts
        -- left join
        -- {{ ref("stg_model_predictions__candidacy_ddhq_matches_20250909") }}
        -- as tbl_ddhq_matches
        -- on tbl_contacts.gp_candidacy_id = tbl_ddhq_matches.gp_candidacy_id
        where
            state in (
                select distinct state_postal_code
                from {{ ref("int__general_states_zip_code_range") }}
            )
            and first_name is not null
            and last_name is not null
            {% if is_incremental() %}
                and updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    ),
    primary_candidacies as (
        select
            * except (general_election_date, runoff_election_date),
            "primary" as election_type,
            primary_election_date as election_date,
            cast(null as date) as general_election_date,
            cast(null as date) as runoff_election_date
        from candidacies
        where primary_election_date is not null
    ),
    general_candidacies as (
        select
            * except (primary_election_date, runoff_election_date),
            "general" as election_type,
            general_election_date as election_date,
            cast(null as date) as primary_election_date,
            cast(null as date) as runoff_election_date
        from candidacies
        where general_election_date is not null
    ),
    runoff_candidacies as (
        select
            * except (primary_election_date, general_election_date),
            "runoff" as election_type,
            runoff_election_date as election_date,
            cast(null as date) as primary_election_date,
            cast(null as date) as general_election_date
        from candidacies
        where runoff_election_date is not null
    ),
    election_fixed_candidacies as (
        select *
        from primary_candidacies
        union all
        select *
        from general_candidacies
        union all
        select *
        from runoff_candidacies
    )
select *
from election_fixed_candidacies
