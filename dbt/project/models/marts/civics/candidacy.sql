-- Civics mart candidacy table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
{%- set br_wins_cols = [
    "product_campaign_id",
    "hubspot_contact_id",
    "hubspot_company_ids",
    "candidate_id_source",
    "party_affiliation",
    "is_open_seat",
    "candidate_office",
    "official_office_name",
    "office_level",
    "candidacy_result",
    "is_pledged",
    "is_verified",
    "verification_status_reason",
    "is_partisan",
    "primary_election_date",
    "primary_runoff_election_date",
    "general_election_date",
    "general_runoff_election_date",
    "viability_score",
    "win_number",
    "win_number_model",
    "created_at",
    "updated_at",
] %}

with
    archive_2025 as (
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            -- Column order must match merged_2026 (br_wins_cols loop order,
            -- then TS-wins, then BR-only, then source_systems)
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

    -- TS int models remap clustered rows to BR's gp_* IDs (via
    -- int__civics_er_canonical_ids), so a full outer join on gp_candidacy_id
    -- merges matched pairs automatically. Unmatched rows on either side pass
    -- through with NULLs on the opposite side.
    merged_2026 as (
        select
            coalesce(br.gp_candidacy_id, ts.gp_candidacy_id) as gp_candidacy_id,
            coalesce(br.gp_candidate_id, ts.gp_candidate_id) as gp_candidate_id,
            coalesce(br.gp_election_id, ts.gp_election_id) as gp_election_id,
            {% for col in br_wins_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            -- TS wins for is_incumbent (TS: 51k populated, BR: 0)
            coalesce(ts.is_incumbent, br.is_incumbent) as is_incumbent,
            br.office_type,
            coalesce(
                br.br_position_database_id, ts.br_position_database_id
            ) as br_position_database_id,
            array_compact(
                array(
                    case when br.gp_candidacy_id is not null then 'ballotready' end,
                    case when ts.gp_candidacy_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidacy_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidacy_techspeed") }} as ts
            on br.gp_candidacy_id = ts.gp_candidacy_id
    ),

    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_2026
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
    case
        when
            icp.icp_win_effective_date is not null
            and coalesce(
                deduplicated.primary_election_date < icp.icp_win_effective_date, true
            )
            and coalesce(
                deduplicated.primary_runoff_election_date < icp.icp_win_effective_date,
                true
            )
            and coalesce(
                deduplicated.general_election_date < icp.icp_win_effective_date, true
            )
            and coalesce(
                deduplicated.general_runoff_election_date < icp.icp_win_effective_date,
                true
            )
        then false
        else icp.icp_office_win
    end as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    case
        when
            icp.icp_win_effective_date is not null
            and coalesce(
                deduplicated.primary_election_date < icp.icp_win_effective_date, true
            )
            and coalesce(
                deduplicated.primary_runoff_election_date < icp.icp_win_effective_date,
                true
            )
            and coalesce(
                deduplicated.general_election_date < icp.icp_win_effective_date, true
            )
            and coalesce(
                deduplicated.general_runoff_election_date < icp.icp_win_effective_date,
                true
            )
        then false
        else icp.icp_win_supersize
    end as is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_database_id = icp.br_database_position_id
