-- A view, not the folder's table default: a thin filter over the staging view,
-- so it does not copy the feed's contact fields into another table, and every
-- reader evaluates current_date() when it builds.
{{ config(materialized="view") }}

-- The weekly CSV feed as an existence source. Airbyte dedup means the feed
-- never forgets a candidacy BallotReady deleted or withdrew, so an upcoming row
-- survives only when the API still lists it as running. Every column stays the
-- feed's own; the API supplies only the status. Read this, not the staging
-- view, wherever a CSV row decides that a candidacy exists.
with
    feed as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    api_status as (
        select database_id, withdrawn, is_hidden
        from {{ ref("int__ballotready_candidacy") }}
    )

select feed.*
from feed
left join
    api_status on try_cast(feed.br_candidacy_id as bigint) = api_status.database_id
where
    {{
        br_candidacy_is_current(
            "feed.election_day",
            "api_status.database_id",
            "api_status.withdrawn",
            "api_status.is_hidden",
        )
    }}
