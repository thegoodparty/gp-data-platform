-- Civics mart candidacy table.
-- 2025 HubSpot archive UNION 2026+ 4-way FOJ over BR + TS + DDHQ + gp_api,
-- joined on gp_candidacy_id (matched providers adopt BR's canonical via
-- int__civics_er_canonical_ids). Per-column precedence rules: see the
-- candidacy model description in m_civics.yaml.
{%- set gp_api_wins_cols = [
    "hubspot_contact_id",
    "candidate_id_source",
    "party_affiliation",
    "candidate_office",
    "official_office_name",
    "office_level",
    "is_partisan",
    "general_election_date",
    "created_at",
    "updated_at",
] %}
{# Subset of gp_api_wins_cols that DDHQ also supplies. The loop adds DDHQ
   to the coalesce chain only for cols listed here; cols in gp_api_wins_cols
   but NOT here render a 3-way coalesce(gp_api, br, ts) instead. #}
{%- set ddhq_fallback_cols = [
    "candidate_id_source",
    "party_affiliation",
    "candidate_office",
    "official_office_name",
    "office_level",
    "general_election_date",
    "created_at",
    "updated_at",
] %}

with
    archive_2025 as (
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            -- Column order must match merged_since_2026
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at,
            is_incumbent,
            office_type,
            br_position_database_id,
            array_compact(
                array('hubspot', case when has_ddhq_match then 'ddhq' end)
            ) as source_systems
        from {{ ref("int__civics_candidacy_2025") }}
    ),

    -- Four-way FOJ. TS / DDHQ / gp_api int models all remap clustered rows
    -- to BR's gp_candidacy_id via int__civics_er_canonical_ids, so a FOJ on
    -- gp_candidacy_id auto-merges matched quadruples. Unmatched rows on any
    -- side pass through with NULLs on absent providers.
    merged_since_2026 as (
        select
            coalesce(
                gp_api.gp_candidacy_id,
                br.gp_candidacy_id,
                ts.gp_candidacy_id,
                ddhq.gp_candidacy_id
            ) as gp_candidacy_id,
            coalesce(
                gp_api.gp_candidate_id,
                br.gp_candidate_id,
                ts.gp_candidate_id,
                ddhq.gp_candidate_id
            ) as gp_candidate_id,
            coalesce(
                gp_api.gp_election_id,
                br.gp_election_id,
                ts.gp_election_id,
                ddhq.gp_election_id
            ) as gp_election_id,
            -- gp_api-only columns
            gp_api.product_campaign_id,
            -- gp_api wins, then BR > TS > DDHQ (where applicable)
            {% for col in gp_api_wins_cols %}
                {% if col in ddhq_fallback_cols %}
                    coalesce(
                        gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}
                    ) as {{ col }},
                {% else %}
                    coalesce(gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}) as {{ col }},
                {% endif %}
            {% endfor %}
            -- hubspot_company_ids: BR-only (gp_api / TS / DDHQ never set it).
            br.hubspot_company_ids,
            -- candidacy_result: DDHQ remains authoritative for results.
            coalesce(
                ddhq.candidacy_result,
                br.candidacy_result,
                ts.candidacy_result,
                gp_api.candidacy_result
            ) as candidacy_result,
            -- is_open_seat: BR > TS > DDHQ; gp_api carries no value.
            coalesce(
                br.is_open_seat, ts.is_open_seat, ddhq.is_open_seat
            ) as is_open_seat,
            -- gp_api wins (only source) for these PD-native flags.
            gp_api.is_pledged,
            gp_api.is_verified,
            gp_api.verification_status_reason,
            -- TS wins for is_incumbent (TS: 51k populated, BR: 0); gp_api/DDHQ
            -- excluded.
            coalesce(ts.is_incumbent, br.is_incumbent) as is_incumbent,
            -- office_type: gp_api > BR > DDHQ (TS doesn't carry it at this grain).
            coalesce(
                gp_api.office_type, br.office_type, ddhq.office_type
            ) as office_type,
            -- br_position_database_id: gp_api > BR > TS. DDHQ doesn't carry it.
            coalesce(
                gp_api.br_position_database_id,
                br.br_position_database_id,
                ts.br_position_database_id
            ) as br_position_database_id,
            -- BR-only stage dates (gp_api only carries general_election_date)
            coalesce(
                br.primary_election_date, ts.primary_election_date
            ) as primary_election_date,
            coalesce(
                br.primary_runoff_election_date, ts.primary_runoff_election_date
            ) as primary_runoff_election_date,
            coalesce(
                br.general_runoff_election_date, ts.general_runoff_election_date
            ) as general_runoff_election_date,
            -- BR-only viability fields
            br.viability_score,
            br.win_number,
            br.win_number_model,
            array_compact(
                array(
                    case when br.gp_candidacy_id is not null then 'ballotready' end,
                    case when ts.gp_candidacy_id is not null then 'techspeed' end,
                    case when ddhq.gp_candidacy_id is not null then 'ddhq' end,
                    case when gp_api.gp_candidacy_id is not null then 'gp_api' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidacy_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidacy_techspeed") }} as ts
            on br.gp_candidacy_id = ts.gp_candidacy_id
        full outer join
            {{ ref("int__civics_candidacy_ddhq") }} as ddhq
            on coalesce(br.gp_candidacy_id, ts.gp_candidacy_id) = ddhq.gp_candidacy_id
        full outer join
            {{ ref("int__civics_candidacy_gp_api") }} as gp_api
            on coalesce(br.gp_candidacy_id, ts.gp_candidacy_id, ddhq.gp_candidacy_id)
            = gp_api.gp_candidacy_id
    ),

    combined as (
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at,
            is_incumbent,
            office_type,
            br_position_database_id,
            source_systems
        from archive_2025
        union all
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at,
            is_incumbent,
            office_type,
            br_position_database_id,
            source_systems
        from merged_since_2026
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    deduplicated.gp_candidacy_id,
    deduplicated.gp_candidate_id,
    deduplicated.gp_election_id,
    deduplicated.product_campaign_id,
    deduplicated.hubspot_contact_id,
    deduplicated.hubspot_company_ids,
    deduplicated.candidate_id_source,
    deduplicated.party_affiliation,
    deduplicated.is_incumbent,
    deduplicated.is_open_seat,
    deduplicated.candidate_office,
    deduplicated.official_office_name,
    deduplicated.office_level,
    deduplicated.office_type,
    deduplicated.candidacy_result,
    deduplicated.is_pledged,
    deduplicated.is_verified,
    deduplicated.verification_status_reason,
    deduplicated.is_partisan,
    deduplicated.primary_election_date,
    deduplicated.primary_runoff_election_date,
    deduplicated.general_election_date,
    deduplicated.general_runoff_election_date,
    deduplicated.br_position_database_id,
    deduplicated.viability_score,
    deduplicated.win_number,
    deduplicated.win_number_model,
    {{
        win_icp_date_gate(
            icp_attribute="icp.icp_office_win",
            primary_date="deduplicated.primary_election_date",
            primary_runoff_date="deduplicated.primary_runoff_election_date",
            general_date="deduplicated.general_election_date",
            general_runoff_date="deduplicated.general_runoff_election_date",
            effective_date="icp.icp_win_effective_date",
        )
    }} as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    {{
        win_icp_date_gate(
            icp_attribute="icp.icp_win_supersize",
            primary_date="deduplicated.primary_election_date",
            primary_runoff_date="deduplicated.primary_runoff_election_date",
            general_date="deduplicated.general_election_date",
            general_runoff_date="deduplicated.general_runoff_election_date",
            effective_date="icp.icp_win_effective_date",
        )
    }} as is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_database_id = icp.br_database_position_id
