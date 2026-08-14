{{ config(severity="warn") }}

-- The candidacy enrichment intermediate is incremental, gated on a global
-- max(updated_at) watermark. Vendor backfills republish rows with older
-- timestamps, which the watermark skips forever, so feed candidacies can go
-- silently missing from everything downstream. This reconcile makes those
-- strandings visible in nightly runs.
--
-- Scope is structural, not cosmetic:
-- * upcoming elections only - past candidacies no longer matter to the
-- product;
-- * candidacy still present in the vendor's current weekly export - a
-- candidacy the vendor deleted is intentionally absent from the
-- intermediate and must not warn;
-- * synced at least 2 days ago - grace so rows landed after the latest
-- nightly build don't false-warn.
with
    feed_candidacies as (
        select
            br_candidacy_id,
            try_cast(br_candidacy_id as bigint) as br_candidacy_id_int,
            state,
            election_day
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where
            election_day >= current_date()
            and try_cast(_ab_source_file_last_modified as timestamp)
            >= current_timestamp() - interval 14 days
            and _airbyte_extracted_at <= current_timestamp() - interval 2 days
        -- one row per candidacy: the feed re-ships candidacies across weekly
        -- export files
        qualify
            row_number() over (
                partition by br_candidacy_id order by _ab_source_file_last_modified desc
            )
            = 1
    )

select feed.br_candidacy_id, feed.state, feed.election_day
from feed_candidacies as feed
left join
    {{ ref("int__ballotready_candidacy") }} as candidacy
    on feed.br_candidacy_id_int = candidacy.database_id
where candidacy.database_id is null
