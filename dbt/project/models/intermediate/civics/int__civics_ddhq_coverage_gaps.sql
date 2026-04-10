-- Election stages from BallotReady / TechSpeed without DDHQ coverage.
--
-- Uses the Splink ER clustered candidacy-stage data to identify races
-- where no candidate was matched to a DDHQ record.
-- Race-level attributes prefer BallotReady values, falling back to TechSpeed.
-- Enriched with ICP flags from the election_stage mart.
--
-- Includes both:
-- 1. Races with a br_race_id (from BallotReady, or TechSpeed with BR linkage)
-- 2. TechSpeed-only races without a br_race_id
--
-- Grain: one row per election stage without DDHQ coverage (keyed by race_key).
with
    er as (select * from {{ ref("stg_er_source__clustered_candidacy_stages") }}),

    -- Clusters containing at least one DDHQ record
    ddhq_clusters as (select distinct cluster_id from er where source_name = 'ddhq'),

    -- Clusters that matched to DDHQ
    matched_clusters as (
        select distinct er.cluster_id
        from er
        inner join ddhq_clusters dc on er.cluster_id = dc.cluster_id
    ),

    -- Races with a br_race_id, preferring BallotReady over TechSpeed.
    -- max() is used instead of any_value() because the CASE expression
    -- produces NULLs for non-matching source rows; max() skips those NULLs
    -- while any_value() could return one. Within a source, values for a
    -- given br_race_id are identical so max() just picks the single value.
    br_races as (
        select
            cast(br_race_id as string) as race_key,
            br_race_id,
            coalesce(
                max(case when source_name = 'ballotready' then state end),
                max(case when source_name = 'techspeed' then state end)
            ) as state,
            coalesce(
                max(case when source_name = 'ballotready' then candidate_office end),
                max(case when source_name = 'techspeed' then candidate_office end)
            ) as candidate_office,
            coalesce(
                max(case when source_name = 'ballotready' then office_level end),
                max(case when source_name = 'techspeed' then office_level end)
            ) as office_level,
            coalesce(
                max(case when source_name = 'ballotready' then office_type end),
                max(case when source_name = 'techspeed' then office_type end)
            ) as office_type,
            coalesce(
                max(case when source_name = 'ballotready' then election_date end),
                max(case when source_name = 'techspeed' then election_date end)
            ) as election_date,
            coalesce(
                max(case when source_name = 'ballotready' then election_stage end),
                max(case when source_name = 'techspeed' then election_stage end)
            ) as election_stage,
            coalesce(
                max(
                    case when source_name = 'ballotready' then official_office_name end
                ),
                max(case when source_name = 'techspeed' then official_office_name end)
            ) as official_office_name,
            coalesce(
                max(case when source_name = 'ballotready' then partisan_type end),
                max(case when source_name = 'techspeed' then partisan_type end)
            ) as partisan_type,
            coalesce(
                max(case when source_name = 'ballotready' then seat_name end),
                max(case when source_name = 'techspeed' then seat_name end)
            ) as seat_name,
            count(
                distinct case when source_name = 'ballotready' then er.cluster_id end
            ) as br_candidate_count,
            count(
                distinct case when source_name = 'techspeed' then er.cluster_id end
            ) as ts_candidate_count,
            -- A race has DDHQ coverage if any candidate's cluster matched DDHQ
            count(distinct mc.cluster_id) > 0 as has_ddhq_match
        from er
        left join matched_clusters mc on er.cluster_id = mc.cluster_id
        where source_name in ('ballotready', 'techspeed') and br_race_id is not null
        group by br_race_id
    ),

    -- TechSpeed-only races without a br_race_id
    ts_only_races as (
        select
            md5(
                er.state
                || '|'
                || er.candidate_office
                || '|'
                || cast(er.election_date as string)
                || '|'
                || er.election_stage
                || '|'
                || coalesce(er.official_office_name, '')
            ) as race_key,
            cast(null as int) as br_race_id,
            er.state,
            max(er.candidate_office) as candidate_office,
            max(er.office_level) as office_level,
            max(er.office_type) as office_type,
            er.election_date,
            er.election_stage,
            er.official_office_name,
            max(er.partisan_type) as partisan_type,
            max(er.seat_name) as seat_name,
            0 as br_candidate_count,
            count(distinct er.cluster_id) as ts_candidate_count,
            count(distinct mc.cluster_id) > 0 as has_ddhq_match
        from er
        left join matched_clusters mc on er.cluster_id = mc.cluster_id
        where er.source_name = 'techspeed' and er.br_race_id is null
        group by
            er.state,
            er.candidate_office,
            er.election_date,
            er.election_stage,
            er.official_office_name
    ),

    all_races as (
        select *
        from br_races
        where not has_ddhq_match

        union all

        select *
        from ts_only_races
        where not has_ddhq_match
    )

select
    all_races.race_key,
    all_races.br_race_id,
    all_races.state,
    all_races.candidate_office,
    all_races.office_level,
    all_races.office_type,
    all_races.election_date,
    all_races.election_stage,
    all_races.official_office_name,
    all_races.partisan_type,
    all_races.seat_name,
    all_races.br_candidate_count,
    all_races.ts_candidate_count,
    es.is_win_icp,
    es.is_serve_icp,
    es.is_win_supersize_icp
from all_races
left join
    {{ ref("election_stage") }} as es
    on cast(all_races.br_race_id as string) = es.br_race_id
