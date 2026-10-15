-- No upcoming BallotReady candidacy may surface in a candidacy reader unless the
-- API lists it as running (a current record, not withdrawn, not hidden). The
-- CSV readers inherit this from int__ballotready_s3_candidacies_gated; a reader
-- that goes back to the raw feed fails here. The feed's election_day dates the
-- CSV rows; the public API dates its rows by race.
with
    feed as (
        select br_candidacy_id, election_day
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    api_status as (
        select database_id, withdrawn, is_hidden
        from {{ ref("int__ballotready_candidacy") }}
    ),

    csv_reader_ids as (
        select 'int__ballotready_s3_candidacies_gated' as model, br_candidacy_id
        from {{ ref("int__ballotready_s3_candidacies_gated") }}
        union all
        select 'int__civics_candidacy_ballotready', br_candidacy_id
        from {{ ref("int__civics_candidacy_ballotready") }}
        union all
        select 'int__civics_candidacy_stage_ballotready', br_candidacy_id
        from {{ ref("int__civics_candidacy_stage_ballotready") }}
        union all
        select 'int__civics_campaign_br_primary_result', br_candidacy_id
        from {{ ref("int__civics_campaign_br_primary_result") }}
        union all
        select 'int__er_prematch_candidacy_stages', source_id
        from {{ ref("int__er_prematch_candidacy_stages") }}
        where source_name = 'ballotready'
    ),

    csv_reader_rows as (
        select r.model, r.br_candidacy_id, feed.election_day
        from csv_reader_ids as r
        inner join feed on cast(r.br_candidacy_id as string) = feed.br_candidacy_id
    ),

    api_reader_rows as (
        select
            'm_election_api__candidacy' as model,
            cast(c.br_database_id as string) as br_candidacy_id,
            cast(race.election_date as date) as election_day
        from {{ ref("m_election_api__candidacy") }} as c
        inner join {{ ref("m_election_api__race") }} as race on c.race_id = race.id
    ),

    reader_rows as (
        select *
        from csv_reader_rows
        union all
        select *
        from api_reader_rows
    )

select reader_rows.model, reader_rows.br_candidacy_id, reader_rows.election_day
from reader_rows
left join
    api_status
    on try_cast(reader_rows.br_candidacy_id as bigint) = api_status.database_id
where
    not {{
        br_candidacy_is_current(
            "reader_rows.election_day",
            "api_status.database_id",
            "api_status.withdrawn",
            "api_status.is_hidden",
        )
    }}
