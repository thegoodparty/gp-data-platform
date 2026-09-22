-- HubSpot coverage on the user spine, as a floor rather than an exact value.
-- The failure this guards is silent: if ingestion degrades, coverage falls,
-- every marketing-touch column downstream goes null for a slice of users, and
-- the tables still render perfectly. Returns a row only when coverage has
-- fallen, so a returned row is the alarm.
--
-- Coverage was 88.4% when this was written; the floor leaves a few points of
-- headroom for ordinary drift. The companion collapse test is the hard floor.
{{ config(severity="warn") }}

with
    coverage as (
        select
            count(*) as users,
            count(hubspot_contact_id) as resolved,
            count(hubspot_contact_id) * 100.0 / count(*) as pct
        from {{ ref("int__user_resolved_keys") }}
    )

select users, resolved, pct
from coverage
where pct < 85
