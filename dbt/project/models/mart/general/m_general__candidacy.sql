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
with
    candidacies as (
        select
            -- Identifiers
            tbl_contacts.gp_candidacy_id,
            tbl_contacts.product_campaign_id,
            "candidacy_id-tbd" as candidacy_id,
            "gp_user_id-tbd" as gp_user_id,
            {{
                generate_salted_uuid(
                    fields=[
                        "coalesce(tbl_contest.official_office_name, '')",
                        "coalesce(tbl_contest.candidate_office, '')",
                        "coalesce(tbl_contest.office_type, '')",
                        "coalesce(tbl_contest.office_level, '')",
                        "coalesce(tbl_contest.state, '')",
                        "coalesce(tbl_contest.city, '')",
                        "coalesce(tbl_contest.district, '')",
                        "coalesce(tbl_contest.seat_name, '')",
                    ]
                )
            }} as gp_contest_id,
            tbl_contacts.company_id as company_id,
            tbl_contacts.company_id as companies_id_main,
            tbl_contacts.contact_id,
            tbl_contacts.candidate_id_source,
            tbl_contacts.candidate_id_tier,
            tbl_contacts.extra_companies,

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
            tbl_contacts.verified_candidate,
            tbl_contacts.pledge_status,

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

            -- Assessments
            viability_scores.viability_rating_2_0 as viability_score,
            cast(cast(tbl_contacts.win_number as float) as int) as win_number,
            tbl_contacts.win_number_model,

            -- DDHQ matches
            tbl_ddhq_matches.ddhq_candidate,
            tbl_ddhq_matches.ddhq_candidate_id,
            tbl_ddhq_matches.ddhq_race_name,
            tbl_ddhq_matches.ddhq_candidate_party,
            tbl_ddhq_matches.ddhq_is_winner,
            case
                when lower(tbl_ddhq_matches.ddhq_election_type) like '%general%'
                then
                    case
                        when tbl_ddhq_matches.ddhq_is_winner = 'Y'
                        then 'Won General'
                        when tbl_ddhq_matches.ddhq_is_winner = 'N'
                        then 'Lost General'
                        else tbl_ddhq_matches.ddhq_is_winner
                    end
                when lower(tbl_ddhq_matches.ddhq_election_type) like '%runoff%'
                then 'Runoff'
                else null
            end as ddhq_general_election_result,
            case
                when lower(tbl_ddhq_matches.ddhq_election_type) like '%primary%'
                then
                    case
                        when tbl_ddhq_matches.ddhq_is_winner = 'Y'
                        then 'Won Primary'
                        when tbl_ddhq_matches.ddhq_is_winner = 'N'
                        then 'Lost Primary'
                        else tbl_ddhq_matches.ddhq_is_winner
                    end
                else null
            end as ddhq_primary_election_result,
            case
                when lower(tbl_ddhq_matches.ddhq_election_type) like '%runoff%'
                then
                    case
                        when tbl_ddhq_matches.ddhq_is_winner = 'Y'
                        then 'Won Runoff'
                        when tbl_ddhq_matches.ddhq_is_winner = 'N'
                        then 'Lost Runoff'
                        else tbl_ddhq_matches.ddhq_is_winner
                    end
                else null
            end as ddhq_runoff_election_result,
            tbl_ddhq_matches.ddhq_race_id,
            tbl_ddhq_matches.ddhq_election_type,
            tbl_ddhq_matches.ddhq_date,
            tbl_ddhq_matches.llm_confidence as ddhq_llm_confidence,
            tbl_ddhq_matches.llm_reasoning as ddhq_llm_reasoning,
            tbl_ddhq_matches.top_10_candidates as ddhq_top_10_candidates,
            tbl_ddhq_matches.has_match as ddhq_has_match,

            -- adding back some DDHQ data
            case
                when lower(tbl_ddhq_matches.ddhq_election_type) like '%general%'
                then tbl_ddhq_election_results_source.votes
                else null
            end as ddhq_votes,
            case
                when lower(tbl_ddhq_matches.ddhq_election_type) like '%general%'
                then tbl_ddhq_election_results_source.total_number_of_ballots_in_race
                else null
            end as ddhq_ballots_cast,
            case
                when lower(tbl_ddhq_matches.ddhq_election_type) like '%general%'
                then tbl_ddhq_election_results_source.number_of_seats_in_election
                else null
            end as ddhq_number_of_seats_in_election,

            -- Metadata
            tbl_contacts.created_at,
            tbl_contacts.updated_at

        from {{ ref("int__hubspot_contacts_w_companies") }} as tbl_contacts
        left join
            {{ ref("stg_model_predictions__viability_scores") }} as viability_scores
            on tbl_contacts.company_id = viability_scores.id
        left join
            {{ ref("stg_model_predictions__candidacy_ddhq_matches_20251016") }}
            as tbl_ddhq_matches
            on tbl_contacts.gp_candidacy_id = tbl_ddhq_matches.gp_candidacy_id
        left join
            {{ ref("int__hubspot_contest") }} as tbl_contest
            on tbl_contest.contact_id = tbl_contacts.contact_id
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
            as tbl_ddhq_election_results_source
            on tbl_ddhq_election_results_source.race_id = tbl_ddhq_matches.ddhq_race_id
            and tbl_ddhq_election_results_source.candidate_id
            = tbl_ddhq_matches.ddhq_candidate_id
            {% if is_incremental() %}
                and tbl_contacts.updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    )
select *
from candidacies
qualify row_number() over (partition by gp_candidacy_id order by updated_at desc) = 1
