-- Civics mart election table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
--
-- BallotReady is the authoritative spine for elections (positions). TS int
-- models remap clustered rows to BR's gp_election_id via
-- int__civics_er_canonical_ids, so a full outer join on gp_election_id merges
-- matched pairs automatically. Unmatched TS elections pass through as net-new.
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
    "has_ddhq_match",
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
            -- then ts_wins_cols, then BR-only, then source_systems)
            {% for col in br_wins_cols %} {{ col }}, {% endfor %}
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

    merged_since_2026 as (
        select
            coalesce(br.gp_election_id, ts.gp_election_id) as gp_election_id,
            {% for col in br_wins_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            -- TS wins: BR always NULL, TS populated from techspeed source
            {% for col in ts_wins_cols %}
                coalesce(ts.{{ col }}, br.{{ col }}) as {{ col }},
            {% endfor %}
            br.br_position_database_id,
            br.is_judicial,
            br.is_appointed,
            br.br_normalized_position_type,
            array_compact(
                array(
                    case when br.gp_election_id is not null then 'ballotready' end,
                    case when ts.gp_election_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_election_ballotready") }} as br
        full outer join
            {{ ref("int__civics_election_techspeed") }} as ts
            on br.gp_election_id = ts.gp_election_id
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
    case
        when
            icp.icp_win_effective_date is not null
            and (
                coalesce(stage_dates.general_election_date, deduplicated.election_date)
                is null
                or coalesce(
                    stage_dates.general_election_date, deduplicated.election_date
                )
                < icp.icp_win_effective_date
            )
        then false
        else icp.icp_office_win
    end as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                coalesce(stage_dates.general_election_date, deduplicated.election_date)
                is null
                or coalesce(
                    stage_dates.general_election_date, deduplicated.election_date
                )
                < icp.icp_win_effective_date
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
left join stage_dates on deduplicated.gp_election_id = stage_dates.gp_election_id
