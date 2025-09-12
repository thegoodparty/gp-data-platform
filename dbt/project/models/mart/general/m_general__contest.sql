{{
    config(
        materialized="incremental",
        unique_key="gp_contest_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "contest", "hubspot"],
    )
}}

with
    constests as (
        select
            -- Identifiers
            tbl_contest.contact_id,
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

            -- office information
            tbl_contest.official_office_name,
            tbl_contest.candidate_office,
            tbl_contest.office_type,
            tbl_contest.office_level,
            tbl_contest.partisan_type as partisanship_type,

            -- geographic information
            tbl_contest.state,
            tbl_contest.city,
            tbl_contest.district,
            tbl_contest.seat_name,

            -- election context
            tbl_contest.number_of_opponents,
            tbl_contest.seats_available,
            tbl_contest.uncontested,
            tbl_contest.open_seat,
            tbl_contest.term_start_date,
            -- term_length_years, -- to add
            -- election dates
            tbl_contest.filing_deadline,
            tbl_contest.primary_election_date,
            tbl_contest.general_election_date,
            tbl_contest.runoff_election_date,

            -- election results
            tbl_contest.primary_results,
            tbl_contest.general_election_results,
            tbl_contest.runoff_election_results,
            tbl_contest.contest_status,
            tbl_contest.contest_results,

            -- DDHQ matches
            tbl_candidacy.gp_candidacy_id,
            tbl_candidacy.ddhq_candidate,
            tbl_candidacy.ddhq_race_name,
            tbl_candidacy.ddhq_candidate_party,
            tbl_candidacy.ddhq_is_winner,
            tbl_candidacy.ddhq_race_id,
            tbl_candidacy.ddhq_election_type,
            tbl_candidacy.ddhq_date,
            tbl_candidacy.ddhq_llm_confidence,
            tbl_candidacy.ddhq_llm_reasoning,
            tbl_candidacy.ddhq_top_10_candidates,
            tbl_candidacy.ddhq_has_match,

            -- metadata
            tbl_contest.created_at,
            tbl_contest.updated_at

        from {{ ref("int__hubspot_contest") }} as tbl_contest
        left join
            {{ ref("m_general__candidacy") }} as tbl_candidacy
            on tbl_contest.contact_id = tbl_candidacy.contact_id
        where
            tbl_contest.contest_status is not null
            {% if is_incremental() %}
                and tbl_contest.updated_at >= (select max(updated_at) from {{ this }})
            {% endif %}
    )

select *
from constests
qualify row_number() over (partition by gp_contest_id order by updated_at desc) = 1
