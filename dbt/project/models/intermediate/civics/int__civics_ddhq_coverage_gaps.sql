-- Election stages from BallotReady / TechSpeed without DDHQ coverage.
--
-- Uses the Splink ER clustered candidacy-stage data to identify races
-- (by br_race_id) where no candidate was matched to a DDHQ record.
-- Race-level attributes prefer BallotReady values, falling back to TechSpeed.
-- Enriched with ICP flags from the election_stage mart.
--
-- Grain: one row per br_race_id (election stage) without DDHQ coverage.
with
    er as (select * from {{ ref("stg_er_source__clustered_candidacy_stages") }}),

    -- Clusters containing at least one DDHQ record
    ddhq_clusters as (select distinct cluster_id from er where source_name = 'ddhq'),

    -- BR race IDs where at least one candidate matched to DDHQ
    br_races_with_ddhq as (
        select distinct er.br_race_id
        from er
        inner join ddhq_clusters dc on er.cluster_id = dc.cluster_id
        where er.br_race_id is not null
    ),

    -- Aggregate to race level, preferring BallotReady over TechSpeed
    races as (
        select
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
                distinct case when source_name = 'ballotready' then cluster_id end
            ) as br_candidate_count,
            count(
                distinct case when source_name = 'techspeed' then cluster_id end
            ) as ts_candidate_count
        from er
        where source_name in ('ballotready', 'techspeed') and br_race_id is not null
        group by br_race_id
    )

select
    races.br_race_id,
    races.state,
    races.candidate_office,
    races.office_level,
    races.office_type,
    races.election_date,
    races.election_stage,
    races.official_office_name,
    races.partisan_type,
    races.seat_name,
    races.br_candidate_count,
    races.ts_candidate_count,
    coalesce(es.is_win_icp, false) as is_win_icp,
    coalesce(es.is_serve_icp, false) as is_serve_icp,
    coalesce(es.is_win_supersize_icp, false) as is_win_supersize_icp
from races
left join br_races_with_ddhq as matched on races.br_race_id = matched.br_race_id
left join
    {{ ref("election_stage") }} as es
    on cast(races.br_race_id as string) = es.br_race_id
where matched.br_race_id is null
