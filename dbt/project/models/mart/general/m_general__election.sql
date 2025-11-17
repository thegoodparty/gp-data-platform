{{
    config(
        materialized="incremental",
        unique_key="gp_election_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "election"],
    )
}}

with
    elections as (
        select
            -- Identifiers
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_contest.official_office_name,
            tbl_contest.candidate_office,
            tbl_contest.office_level,
            tbl_contest.office_type,
            tbl_contest.state,
            tbl_contest.city,
            tbl_contest.district,
            tbl_contest.seat_name,
            tbl_contest.election_date,
            tbl_contest.election_year,
            tbl_contest.filing_deadline,
            tbl_contest.population,
            tbl_contest.seats_available,
            tbl_contest.term_start_date,
            tbl_contest.uncontested as is_uncontested,
            tbl_contest.number_of_opponents,
            tbl_contest.open_seat,
            case
                when tbl_ddhq_matches.ddhq_race_id is not null then true else false
            end as has_ddhq_match,
            tbl_contest.created_at,
            tbl_contest.updated_at
        from {{ ref("int__hubspot_contest") }} as tbl_contest
        left join
            {{ ref("m_general__candidacy") }} as tbl_candidacy
            on tbl_candidacy.contact_id = tbl_contest.contact_id
        left join
            {{ ref("stg_model_predictions__candidacy_ddhq_matches_20251016") }}
            as tbl_ddhq_matches
            on tbl_ddhq_matches.gp_candidacy_id = tbl_candidacy.gp_candidacy_id
        where
            1 = 1
            {% if is_incremental() %}
                and tbl_contest.updated_at >= (select max(updated_at) from {{ this }})
            {% endif %}
    )

select *
from elections
qualify
    row_number() over (
        partition by gp_election_id order by has_ddhq_match desc, updated_at desc
    )
    = 1
