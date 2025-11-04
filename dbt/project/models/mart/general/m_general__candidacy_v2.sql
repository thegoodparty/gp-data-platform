{{
    config(
        materialized="incremental",
        unique_key="gp_candidacy_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "candidacy"],
    )
}}

-- Final candidacy objects with viability scores
with
    candidacies as (
        select
            -- Identifiers
            tbl_contacts.gp_candidacy_id,
            "candidacy_id-tbd" as candidacy_id,
            tbl_candidates.gp_candidate_id as gp_candidate_id,
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_contacts.product_campaign_id,
            tbl_contacts.extra_companies as hubspot_company_ids,
            tbl_contacts.candidate_id_source,

            -- candidacy information
            tbl_contacts.party_affiliation,
            tbl_contacts.is_incumbent,
            tbl_contacts.is_open_seat,
            tbl_contacts.candidacy_result,
            tbl_contacts.pledge_status,
            tbl_contacts.verified_candidate,
            tbl_contacts.is_partisan,

            -- assessments
            viability_scores.viability_rating_2_0 as viability_score,
            cast(cast(tbl_contacts.win_number as float) as int) as win_number,
            cast(
                cast(tbl_contacts.win_number_model as float) as int
            ) as win_number_model,

            -- loading data
            tbl_contacts.created_at,
            tbl_contacts.updated_at

        from {{ ref("int__hubspot_contacts_w_companies") }} as tbl_contacts
        left join
            {{ ref("m_general__candidate_v2") }} as tbl_candidates
            on tbl_contacts.contact_id = tbl_candidates.hubspot_contact_id
        left join
            {{ ref("int__hubspot_contest") }} as tbl_contest
            on tbl_contest.contact_id = tbl_contacts.contact_id
        left join
            {{ ref("stg_model_predictions__viability_scores") }} as viability_scores
            on tbl_contacts.company_id = viability_scores.id
        {% if is_incremental() %}
            where tbl_contacts.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )
select *
from candidacies
qualify row_number() over (partition by gp_candidacy_id order by updated_at desc) = 1
