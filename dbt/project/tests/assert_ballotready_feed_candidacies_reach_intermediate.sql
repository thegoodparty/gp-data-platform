{{ config(severity="warn") }}

-- Feed candidacies recently ingested that never reached
-- int__ballotready_candidacy (a gate regression or a dropped enrichment),
-- scoped to upcoming or unknown-date elections. Warn-only: the count is an
-- ops signal, not a build blocker. The 2-day grace skips rows newer than the
-- latest nightly build; a stranded row's signal ages out with the 14-day window.
select feed.br_candidacy_id, feed.state, feed.election_day
from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }} as feed
left join
    {{ ref("int__ballotready_candidacy") }} as candidacy
    on try_cast(feed.br_candidacy_id as bigint) = candidacy.database_id
where
    (feed.election_day >= current_date() or feed.election_day is null)
    and feed._airbyte_extracted_at
    between current_timestamp()
    - interval 14 days and current_timestamp()
    - interval 2 days
    and candidacy.database_id is null
