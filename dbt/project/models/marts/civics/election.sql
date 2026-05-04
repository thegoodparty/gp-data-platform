-- Civics mart election table.
-- 2025 HubSpot archive UNION 2026+ 4-way FOJ over BR + TS + DDHQ + gp_api,
-- joined on gp_election_id. BR is the authoritative spine; gp_api carries
-- only IDs + FK pointers (br_position_database_id) and contributes to
-- source_systems membership only — its descriptive columns are NULL by
-- design and FOJ coalesce always picks BR's values when present.
-- Per-column precedence rules: see the election model description in
-- m_civics.yaml.
--
-- has_ddhq_match: true when either the 2025 archive linked a DDHQ race or a
-- 2026+ DDHQ row clustered into this election via Splink. NULL otherwise.
{%- set br_wins_cols = [
    "official_office_name",
    "candidate_office",
    "office_level",
    "office_type",
    "state",
    "city",
    "district",
    "seat_name",
    "election_date",
    "election_year",
    "seats_available",
    "term_start_date",
    "number_of_opponents",
    "created_at",
    "updated_at",
] %}

{%- set ts_wins_cols = [
    "filing_deadline",
    "population",
    "is_uncontested",
    "is_open_seat",
] %}

with
    archive_2025 as (
        select
            gp_election_id,
            -- Column order must match merged_since_2026 (br_wins_cols loop order,
            -- has_ddhq_match, ts_wins_cols, BR-only, source_systems)
            {% for col in br_wins_cols %} {{ col }}, {% endfor %}
            has_ddhq_match,
            {% for col in ts_wins_cols %} {{ col }}, {% endfor %}
            br_position_database_id,
            is_judicial,
            is_appointed,
            br_normalized_position_type,
            array_compact(
                array('hubspot', case when has_ddhq_match then 'ddhq' end)
            ) as source_systems
        from {{ ref("int__civics_election_2025") }}
    ),

    -- DDHQ projected into the mart-row schema. The mart's `state` column
    -- holds 2-letter codes (BR/TS-staging convention) — pull DDHQ's
    -- state_postal_code through, not its `state` column (which is the
    -- human-readable name per int_model convention).
    ddhq as (
        select * except (state), state_postal_code as state
        from {{ ref("int__civics_election_ddhq") }}
    ),

    merged_since_2026 as (
        select
            coalesce(
                br.gp_election_id,
                ts.gp_election_id,
                ddhq.gp_election_id,
                gp_api.gp_election_id
            ) as gp_election_id,
            {% for col in br_wins_cols %}
                coalesce(
                    br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}, gp_api.{{ col }}
                ) as {{ col }},
            {% endfor %}
            -- has_ddhq_match must be handled outside the coalesce loop: BR,
            -- TS, and gp_api hardcode it to false (non-null), so a coalesce
            -- would always pick BR's false on Splink-matched BR+DDHQ rows.
            -- Derive directly from join presence instead.
            ddhq.gp_election_id is not null as has_ddhq_match,
            -- TS wins for these (BR always NULL on 2026+ for population/
            -- filing_deadline; TS populated from techspeed source). DDHQ
            -- supplies is_uncontested as a fallback. gp_api is NULL by
            -- design at the descriptive layer and contributes nothing here.
            {% for col in ts_wins_cols %}
                coalesce(
                    ts.{{ col }}, br.{{ col }}, ddhq.{{ col }}, gp_api.{{ col }}
                ) as {{ col }},
            {% endfor %}
            coalesce(
                br.br_position_database_id,
                ts.br_position_database_id,
                gp_api.br_position_database_id
            ) as br_position_database_id,
            br.is_judicial,
            br.is_appointed,
            br.br_normalized_position_type,
            array_compact(
                array(
                    case when br.gp_election_id is not null then 'ballotready' end,
                    case when ts.gp_election_id is not null then 'techspeed' end,
                    case when ddhq.gp_election_id is not null then 'ddhq' end,
                    case when gp_api.gp_election_id is not null then 'gp_api' end
                )
            ) as source_systems
        from {{ ref("int__civics_election_ballotready") }} as br
        full outer join
            {{ ref("int__civics_election_techspeed") }} as ts
            on br.gp_election_id = ts.gp_election_id
        full outer join
            ddhq on coalesce(br.gp_election_id, ts.gp_election_id) = ddhq.gp_election_id
        full outer join
            {{ ref("int__civics_election_gp_api") }} as gp_api
            on coalesce(br.gp_election_id, ts.gp_election_id, ddhq.gp_election_id)
            = gp_api.gp_election_id
    ),

    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_since_2026
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (
                partition by gp_election_id order by updated_at desc nulls last
            )
            = 1
    ),

    -- Pivot election stage dates by type
    stage_dates as (
        select
            gp_election_id,
            max(
                case when stage_type = 'primary' then election_date end
            ) as primary_election_date,
            max(
                case when stage_type = 'general' then election_date end
            ) as general_election_date,
            max(
                case when stage_type = 'primary runoff' then election_date end
            ) as primary_runoff_election_date,
            max(
                case when stage_type = 'general runoff' then election_date end
            ) as general_runoff_election_date
        from {{ ref("election_stage") }}
        group by gp_election_id
    )

select
    deduplicated.gp_election_id,
    deduplicated.official_office_name,
    deduplicated.candidate_office,
    deduplicated.office_level,
    deduplicated.office_type,
    deduplicated.state,
    deduplicated.city,
    deduplicated.district,
    deduplicated.seat_name,
    deduplicated.election_date,
    deduplicated.election_year,
    deduplicated.filing_deadline,
    deduplicated.population,
    deduplicated.seats_available,
    deduplicated.term_start_date,
    deduplicated.is_uncontested,
    deduplicated.number_of_opponents,
    deduplicated.is_open_seat,
    deduplicated.has_ddhq_match,
    deduplicated.br_position_database_id,
    deduplicated.is_judicial,
    deduplicated.is_appointed,
    deduplicated.br_normalized_position_type,
    stage_dates.primary_election_date,
    stage_dates.general_election_date,
    stage_dates.primary_runoff_election_date,
    stage_dates.general_runoff_election_date,
    icp.voter_count as icp_voter_count,
    icp.normalized_position_type as icp_normalized_position_name,
    {{
        win_icp_date_gate(
            icp_attribute="icp.icp_office_win",
            primary_date="stage_dates.primary_election_date",
            primary_runoff_date="stage_dates.primary_runoff_election_date",
            general_date="coalesce(stage_dates.general_election_date, deduplicated.election_date)",
            general_runoff_date="stage_dates.general_runoff_election_date",
            effective_date="icp.icp_win_effective_date",
        )
    }}
    as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    {{
        win_icp_date_gate(
            icp_attribute="icp.icp_win_supersize",
            primary_date="stage_dates.primary_election_date",
            primary_runoff_date="stage_dates.primary_runoff_election_date",
            general_date="coalesce(stage_dates.general_election_date, deduplicated.election_date)",
            general_runoff_date="stage_dates.general_runoff_election_date",
            effective_date="icp.icp_win_effective_date",
        )
    }}
    as is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_database_id = icp.br_database_position_id
left join stage_dates on deduplicated.gp_election_id = stage_dates.gp_election_id
