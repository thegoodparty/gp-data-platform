{{
    config(
        severity="warn",
        warn_if=">0",
        error_if=">20",
        tags=["feed_invariant", "sales_reverse_etl"],
    )
}}
-- DATA-1523 / DATA-2011 cutover guard (30-day rolling window).
-- candidacy_hubspot keeps candidacies active in the last 30 days on
-- greatest(created_at, updated_at).
-- A few boundary rows can appear between the model build and the test run, so warn on
-- any and error
-- only on a material breach (a removed/broken window predicate).
select gp_candidacy_id
from {{ ref("candidacy_hubspot") }}
where last_activity_at < current_date() - interval 30 day
