{{
    config(
        materialized="incremental",
        unique_key="gp_contest_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "contest", "hubspot"],
    )
}}

select
    -- Identifiers
    contact_id,
    {{
        generate_salted_uuid(
            fields=[
                "official_office_name",
                "candidate_office",
                "office_type",
                "office_level",
                "state",
                "city",
                "district",
                "seat_name",
            ]
        )
    }} as gp_contest_id,

    -- office information
    official_office_name,
    candidate_office,
    office_type,
    office_level,
    partisan_type as partisanship_type,

    -- geographic information
    state,
    city,
    district,
    seat_name,

    -- election context
    number_of_opponents,
    seats_available,
    uncontested,
    open_seat,
    term_start_date,
    -- term_length_years, -- to add
    -- election dates
    filing_deadline,
    primary_election_date,
    general_election_date,
    runoff_election_date,

    -- election results
    primary_results,
    general_election_results,
    runoff_election_results,
    contest_status,
    contest_results,

    -- DDHQ matches
    tbl_ddhq_matches.gp_candidacy_id,
    tbl_ddhq_matches.ddhq_candidate,
    tbl_ddhq_matches.ddhq_race_name,
    tbl_ddhq_matches.ddhq_candidate_party,
    tbl_ddhq_matches.ddhq_is_winner,
    tbl_ddhq_matches.ddhq_race_id,
    tbl_ddhq_matches.ddhq_election_type,
    tbl_ddhq_matches.ddhq_date,
    tbl_ddhq_matches.llm_confidence as ddhq_llm_confidence,
    tbl_ddhq_matches.llm_reasoning as ddhq_llm_reasoning,
    tbl_ddhq_matches.top_10_candidates as ddhq_top_10_candidates,
    tbl_ddhq_matches.has_match as ddhq_has_match,

    -- metadata
    created_at,
    updated_at

from {{ ref("int__hubspot_contest") }}
left join
    {{ ref("m_general__candidacy") }} as tbl_candidacy
    on tbl_contest.contact_id = tbl_candidacy.contact_id
where
    1 = 1 and contest_status is not null
    {% if is_incremental() %}
        and updated_at >= (select max(updated_at) from {{ this }})
    {% endif %}
qualify row_number() over (partition by gp_contest_id order by updated_at desc) = 1
