-- Civics mart election_stage table.
-- 2025 HubSpot archive UNION 2026+ 4-way FOJ over BR + TS + DDHQ + gp_api,
-- joined on gp_election_stage_id. BR is the authoritative spine; gp_api
-- carries only IDs + FK pointers (br_race_id, br_position_id) and
-- contributes to source_systems membership only — its descriptive columns
-- are NULL by design and FOJ coalesce always picks BR's values when present.
-- Per-column precedence rules: see the election_stage model description in
-- m_civics.yaml. Notable exceptions: total_votes_cast is BR-first only when
-- BR has a numeric count (BR's 'uncontested' literal yields to DDHQ's
-- actual count); ddhq_race_id falls back to DDHQ when BR's null.
{%- set br_wins_cols = [
    "gp_election_id",
    "br_race_id",
    "stage_type",
    "election_date",
    "election_name",
    "race_name",
    "is_primary",
    "is_runoff",
    "is_retention",
    "number_of_seats",
    "created_at",
    "updated_at",
] %}

with
    archive_2025 as (
        select
            gp_election_stage_id,
            -- Column order must match merged_since_2026 (br_wins_cols loop order,
            -- then BR-only, then source_systems)
            gp_election_id,
            cast(null as string) as br_race_id,
            election_stage as stage_type,
            ddhq_election_stage_date as election_date,
            cast(null as string) as election_name,
            ddhq_race_name as race_name,
            election_stage in ('primary', 'primary runoff') as is_primary,
            election_stage in ('general runoff', 'primary runoff') as is_runoff,
            cast(null as boolean) as is_retention,
            cast(null as int) as number_of_seats,
            created_at,
            cast(null as timestamp) as updated_at,
            cast(null as string) as br_election_id,
            br_position_id,
            ddhq_race_id,
            total_votes_cast,
            cast(null as string) as partisan_type,
            cast(null as date) as filing_period_start_on,
            cast(null as date) as filing_period_end_on,
            cast(null as string) as filing_requirements,
            cast(null as string) as filing_address,
            cast(null as string) as filing_phone,
            array_compact(
                array('hubspot', case when ddhq_race_id is not null then 'ddhq' end)
            ) as source_systems
        from {{ ref("int__civics_election_stage_2025") }}
    ),

    merged_since_2026 as (
        select
            coalesce(
                br.gp_election_stage_id,
                ts.gp_election_stage_id,
                ddhq.gp_election_stage_id,
                gp_api.gp_election_stage_id
            ) as gp_election_stage_id,
            {% for col in br_wins_cols %}
                coalesce(
                    br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}, gp_api.{{ col }}
                ) as {{ col }},
            {% endfor %}
            br.br_election_id,
            -- br_position_id: BR-authoritative when present. gp_api carries
            -- this same value for BR-anchored stages, so coalesce surfaces
            -- it for gp_api-only rows that joined on a BR-mapped stage id.
            coalesce(br.br_position_id, gp_api.br_position_id) as br_position_id,
            -- ddhq_race_id: DDHQ's own ID is more reliable than BR's
            -- (which is sometimes null for 2026+ rows even when DDHQ has data).
            coalesce(br.ddhq_race_id, ddhq.ddhq_race_id) as ddhq_race_id,
            -- total_votes_cast: BR can carry the literal 'uncontested' as a
            -- sentinel; treat that as missing and prefer DDHQ's numeric count.
            coalesce(
                nullif(br.total_votes_cast, 'uncontested'),
                ddhq.total_votes_cast,
                br.total_votes_cast
            ) as total_votes_cast,
            br.partisan_type,
            br.filing_period_start_on,
            br.filing_period_end_on,
            br.filing_requirements,
            br.filing_address,
            br.filing_phone,
            array_compact(
                array(
                    case
                        when br.gp_election_stage_id is not null then 'ballotready'
                    end,
                    case when ts.gp_election_stage_id is not null then 'techspeed' end,
                    case when ddhq.gp_election_stage_id is not null then 'ddhq' end,
                    case when gp_api.gp_election_stage_id is not null then 'gp_api' end
                )
            ) as source_systems
        from {{ ref("int__civics_election_stage_ballotready") }} as br
        full outer join
            {{ ref("int__civics_election_stage_techspeed") }} as ts
            on br.gp_election_stage_id = ts.gp_election_stage_id
        full outer join
            {{ ref("int__civics_election_stage_ddhq") }} as ddhq
            on coalesce(br.gp_election_stage_id, ts.gp_election_stage_id)
            = ddhq.gp_election_stage_id
        full outer join
            {{ ref("int__civics_election_stage_gp_api") }} as gp_api
            on coalesce(
                br.gp_election_stage_id,
                ts.gp_election_stage_id,
                ddhq.gp_election_stage_id
            )
            = gp_api.gp_election_stage_id
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
                partition by gp_election_stage_id order by updated_at desc nulls last
            )
            = 1
    )

select
    deduplicated.gp_election_stage_id,
    deduplicated.gp_election_id,
    deduplicated.br_race_id,
    deduplicated.br_election_id,
    deduplicated.br_position_id,
    deduplicated.ddhq_race_id,
    deduplicated.stage_type,
    deduplicated.election_date,
    deduplicated.election_name,
    deduplicated.race_name,
    deduplicated.is_primary,
    deduplicated.is_runoff,
    deduplicated.is_retention,
    deduplicated.number_of_seats,
    deduplicated.total_votes_cast,
    deduplicated.partisan_type,
    deduplicated.filing_period_start_on,
    deduplicated.filing_period_end_on,
    deduplicated.filing_requirements,
    deduplicated.filing_address,
    deduplicated.filing_phone,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                deduplicated.election_date is null
                or deduplicated.election_date < icp.icp_win_effective_date
            )
        then false
        else icp.icp_office_win
    end as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                deduplicated.election_date is null
                or deduplicated.election_date < icp.icp_win_effective_date
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
    on deduplicated.br_position_id = icp.br_database_position_id
