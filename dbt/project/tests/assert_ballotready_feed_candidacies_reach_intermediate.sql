{{ config(severity="warn") }}

-- Every upcoming-election candidacy still in the vendor's current export
-- (null election_day = unknown, so in scope) must exist in
-- int__ballotready_candidacy. The vendor-timestamp watermark that stranded
-- feed rows was fixed alongside this test; this monitor guards the residual
-- classes: vendor export-vs-API inconsistencies and gate regressions. A
-- stranded row warns for ~14 days, then ages out of the export window and
-- the signal decays with it.
with
    latest_export as (
        select
            max(try_cast(_ab_source_file_last_modified as timestamp)) as latest_file_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        -- same grace as the row filter below, so the window base can never
        -- sit inside a window whose rows are all excluded
        where _airbyte_extracted_at <= current_timestamp() - interval 2 days
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
            -- current-export window; fails open when a file timestamp does
            -- not parse (feed-wide or per row) so format drift warns loudly
            -- instead of silently dropping rows from scope
            and (
                latest_export.latest_file_at is null
                or try_cast(feed._ab_source_file_last_modified as timestamp) is null
                or try_cast(feed._ab_source_file_last_modified as timestamp)
                >= latest_export.latest_file_at - interval 14 days
            )
            -- grace so rows landed after the latest nightly build don't
            -- false-warn
            and feed._airbyte_extracted_at <= current_timestamp() - interval 2 days
    )

select feed.br_candidacy_id, feed.state, feed.election_day
from feed_candidacies as feed
left join
    {{ ref("int__ballotready_candidacy") }} as candidacy
    on feed.br_candidacy_id_int = candidacy.database_id
where candidacy.database_id is null
