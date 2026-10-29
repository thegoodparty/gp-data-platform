-- Which ballot data sources reported each user's election result, and the day
-- each report reached us: one row per user per source. Internal only.
--
-- The civics stage row says which source the result shows (DDHQ before
-- BallotReady); the days come from the feeds. A DDHQ day is the date of the
-- first delivery file holding the result; results only the one-time master
-- backfill holds have no day. BallotReady keeps only the latest version of a record,
-- so its
-- day is the last update: a later edit moves it. TechSpeed's is its latest load.
with
    journey as (
        select
            user_id,
            candidacy_campaign_id,
            to_date(candidacy_election_date) as election_date
        from {{ ref("user_journey") }}
        where candidacy_result is not null
    ),

    -- The same anchor match as the user's candidacy profile: the run's date
    -- must be one of the candidacy's stage dates, and a general-date match wins.
    anchor_candidacy as (
        select j.user_id, c.gp_candidacy_id
        from journey as j
        inner join
            {{ ref("candidacy") }} as c
            on c.product_campaign_id = j.candidacy_campaign_id
            and j.election_date in (
                c.general_election_date,
                c.primary_election_date,
                c.primary_runoff_election_date,
                c.general_runoff_election_date
            )
        qualify
            row_number() over (
                partition by j.user_id
                order by
                    case
                        when c.general_election_date = j.election_date then 0 else 1
                    end,
                    c.gp_candidacy_id
            )
            = 1
    ),

    -- The latest stage with a result is the one the user's result comes from.
    result_stage as (
        select
            a.user_id,
            s.gp_candidacy_id,
            s.gp_candidacy_stage_id,
            s.election_stage,
            s.election_stage_date,
            s.election_result,
            s.election_result_source,
            s.br_candidacy_id,
            cast(s.ddhq_candidate_id as int) as ddhq_candidate_id,
            cast(s.ddhq_race_id as int) as ddhq_race_id,
            to_date(s.updated_at) as techspeed_loaded_on
        from anchor_candidacy as a
        inner join
            {{ ref("candidacy_stage") }} as s on s.gp_candidacy_id = a.gp_candidacy_id
        where s.election_result is not null
        qualify
            row_number() over (
                partition by a.user_id order by s.election_stage_date desc
            )
            = 1
    ),

    ddhq_reports as (
        select candidate_id, ddhq_race_id, min(delivered_on) as reported_on
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_result_deliveries") }}
        where is_winner is not null
        group by candidate_id, ddhq_race_id
    ),

    ballotready_reports as (
        select br_candidacy_id, to_date(candidacy_updated_at) as reported_on
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where election_result is not null
    ),

    reports as (
        select rs.user_id, 'ddhq' as source, d.reported_on
        from result_stage as rs
        inner join
            ddhq_reports as d
            on d.candidate_id = rs.ddhq_candidate_id
            and d.ddhq_race_id = rs.ddhq_race_id
        union all
        select rs.user_id, 'ballotready' as source, b.reported_on
        from result_stage as rs
        inner join ballotready_reports as b on b.br_candidacy_id = rs.br_candidacy_id
        union all
        select user_id, 'techspeed' as source, techspeed_loaded_on as reported_on
        from result_stage
        where election_result_source = 'techspeed'
    ),

    -- The shown source keeps a row even when its feed row can't be found, so
    -- every user with a result says where it came from.
    shown_without_report as (
        select
            rs.user_id,
            rs.election_result_source as source,
            cast(null as date) as reported_on
        from result_stage as rs
        left anti join
            reports as r
            on r.user_id = rs.user_id
            and r.source = rs.election_result_source
    ),

    all_reports as (
        select *
        from reports
        union all
        select *
        from shown_without_report
    )

select
    r.user_id,
    rs.gp_candidacy_id,
    rs.gp_candidacy_stage_id,
    rs.election_stage,
    rs.election_stage_date,
    rs.election_result,
    r.source,
    r.reported_on,
    r.source = rs.election_result_source as is_shown_source
from all_reports as r
inner join result_stage as rs on rs.user_id = r.user_id
