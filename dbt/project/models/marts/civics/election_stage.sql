-- Civics mart election_stage table.
-- 2025 HubSpot archive UNION 2026+ 3-way FOJ over BR + TS + DDHQ joined on
-- gp_election_stage_id, with a left-join membership lookup that appends
-- 'gp_api' to source_systems for stages any PD candidacy_stage maps to.
-- gp_api int models adopt BR's natural gp_election_stage_id whenever the gp_api
-- filter passes, so the membership lookup hits BR's spine row directly.
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
    "office_level",
    "office_type",
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
            -- 2025 archive carries no office taxonomy; null to match
            -- merged_since_2026 column order (br_wins_cols loop).
            cast(null as string) as office_level,
            cast(null as string) as office_type,
            election_stage in (
                'primary', 'primary runoff', 'primary special', 'primary special runoff'
            ) as is_primary,
            election_stage in (
                'general runoff',
                'primary runoff',
                'general special runoff',
                'primary special runoff'
            ) as is_runoff,
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
                ddhq.gp_election_stage_id
            ) as gp_election_stage_id,
            {% for col in br_wins_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}) as {{ col }},
            {% endfor %}
            br.br_election_id,
            br.br_position_id,
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
                    case when ddhq.gp_election_stage_id is not null then 'ddhq' end
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
    ),

    gp_api_membership as (
        -- gp_api participation marker. No new int model at election_stage grain
        -- (per design: gp_api contributes no field values BR/TS/DDHQ don't
        -- already author better here). gp_api stages adopt BR's natural
        -- gp_election_stage_id (the gp_api filter ensures ballotready_position_id),
        -- so this lookup hits BR's spine row directly.
        select distinct gp_election_stage_id
        from {{ ref("int__civics_candidacy_stage_gp_api") }}
        where gp_election_stage_id is not null
    ),

    -- BR candidacies per race (BR-side candidate count). Sourced from the
    -- race-grain S3 candidacies so every stage's br_race_id gets its own count;
    -- the rolled-up candidacy model collapses stages onto one any_value
    -- br_race_id and would zero-out the non-selected stages.
    br_candidacy_counts as (
        select
            cast(br_race_id as string) as br_race_id,
            count(distinct cast(br_candidate_id as string)) as br_candidate_count
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_race_id is not null
        group by br_race_id
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
    deduplicated.office_level,
    deduplicated.office_type,
    deduplicated.is_primary,
    deduplicated.is_runoff,
    deduplicated.is_retention,
    deduplicated.number_of_seats,
    deduplicated.total_votes_cast,
    coalesce(bcc.br_candidate_count, 0) as br_candidate_count,
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
    array_compact(
        array_append(
            deduplicated.source_systems,
            case when gp.gp_election_stage_id is not null then 'gp_api' end
        )
    ) as source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_id = icp.br_database_position_id
left join
    gp_api_membership as gp
    on deduplicated.gp_election_stage_id = gp.gp_election_stage_id
left join br_candidacy_counts as bcc on deduplicated.br_race_id = bcc.br_race_id
