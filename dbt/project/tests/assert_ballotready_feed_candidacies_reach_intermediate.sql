{{ config(severity="warn") }}

-- The candidacy enrichment intermediate is incremental, gated on a global
-- max(updated_at) watermark. Vendor backfills republish rows with older
-- timestamps, which the watermark skips forever, so feed candidacies can go
-- silently missing from everything downstream. This reconcile makes those
-- strandings visible in nightly runs.
--
-- Scope is structural, not cosmetic:
-- * upcoming elections only - past candidacies no longer matter to the
-- product. Null election days stay in scope: staging nulls far-future
-- placeholder dates, so null means unknown, not past;
-- * candidacy still present in the vendor's current export - a candidacy the
-- vendor deleted is intentionally absent from the intermediate and must not
-- warn. The window anchors on the newest delivered file, not wall clock, so
-- a delivery outage cannot empty the feed side and vacuously green the test.
-- If no file timestamp parses at all, the exclusion fails open so a
-- feed-wide format change warns loudly instead of greening silently;
-- * synced at least 2 days ago - grace so rows landed after the latest
-- nightly build don't false-warn.
with
    latest_export as (
        select
            max(try_cast(_ab_source_file_last_modified as timestamp)) as latest_file_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    feed_candidacies as (
        select
            feed.br_candidacy_id,
            try_cast(feed.br_candidacy_id as bigint) as br_candidacy_id_int,
            feed.state,
            feed.election_day
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }} as feed
        cross join latest_export
        where
            (feed.election_day >= current_date() or feed.election_day is null)
            and (
                latest_export.latest_file_at is null
                or try_cast(feed._ab_source_file_last_modified as timestamp)
                >= latest_export.latest_file_at - interval 14 days
            )
            and feed._airbyte_extracted_at <= current_timestamp() - interval 2 days
        -- one row per candidacy: the feed re-ships candidacies across weekly
        -- export files
        qualify
            row_number() over (
                partition by feed.br_candidacy_id
                order by try_cast(feed._ab_source_file_last_modified as timestamp) desc
            )
            = 1
    )

select feed.br_candidacy_id, feed.state, feed.election_day
from feed_candidacies as feed
left join
    {{ ref("int__ballotready_candidacy") }} as candidacy
    on feed.br_candidacy_id_int = candidacy.database_id
where candidacy.database_id is null
