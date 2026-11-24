-- Pairs verification for the companion activity-to-contact association loader.
--
-- This is what decides whether an activity type may cut over to Lakeflow. The v4
-- association endpoint could return fewer links than the frozen v1 envelope's
-- `associations_contactids` did, and `int__user_hubspot_touches` drops unassociated
-- activities, so a shortfall silently shortens the touch history.
--
-- Counts only, never a contact value. A dbt analysis, so it compiles and is validated
-- against the sources but never materializes; run the compiled SQL by hand after the
-- first real loader run and compare the two pair counts on the overlapping ids.
--
-- The baseline it expects is recorded in the worker's report, not here: a pinned number
-- in committed SQL goes stale the first time HubSpot changes an activity, and the query
-- recomputes both sides anyway.
--
-- Only overlapping ids are compared. An activity the frozen feed holds and HubSpot has
-- since deleted has no loader row by design, so counting it would read as an endpoint
-- fault.
with
    -- One row per engagement id: the frozen stream re-emits a row per sync, so an
    -- undeduplicated read double-counts pairs.
    frozen as (
        select *
        from {{ source("airbyte_source", "hubspot_api_engagements") }}
        where type in ('CALL', 'MEETING', 'NOTE', 'TASK')
        qualify
            row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
    ),

    frozen_pairs as (
        select
            type as activity_type,
            cast(id as string) as activity_id,
            coalesce(
                array_distinct(from_json(associations_contactids, 'array<string>')),
                cast(array() as array<string>)
            ) as contact_ids
        from frozen
    ),

    -- The latest snapshot per activity. Append-only landing, so a re-read leaves more
    -- than one row for the same activity and only the newest is current.
    loader_latest as (
        select
            activity_type,
            activity_id,
            coalesce(
                array_distinct(from_json(contact_ids_json, 'array<string>')),
                cast(array() as array<string>)
            ) as contact_ids
        from
            {{
                source(
                    "hubspot_companion",
                    "companion_activity_contact_associations",
                )
            }}
        qualify
            row_number() over (
                partition by activity_type, activity_id
                order by loaded_at desc, source_changed_at desc
            )
            = 1
    ),

    comparison as (
        select
            f.activity_type,
            count(*) as overlapping_activities,
            sum(size(f.contact_ids)) as frozen_pairs,
            sum(size(l.contact_ids)) as loader_pairs,
            sum(
                case when size(f.contact_ids) > 0 then 1 else 0 end
            ) as frozen_linked_activities,
            -- The sharpest signal: the frozen feed reached a person for this activity
            -- and
            -- the loader did not, which is the row the touch history would lose.
            sum(
                case
                    when size(f.contact_ids) > 0 and size(l.contact_ids) = 0
                    then 1
                    else 0
                end
            ) as loader_lost_link_activities,
            count(l.activity_id) as activities_loader_returned
        from frozen_pairs as f
        left join
            loader_latest as l
            on l.activity_type = f.activity_type
            and l.activity_id = f.activity_id
        group by f.activity_type
    )

select
    activity_type,
    overlapping_activities,
    frozen_pairs,
    loader_pairs,
    loader_pairs - frozen_pairs as pair_delta,
    frozen_linked_activities,
    loader_lost_link_activities,
    activities_loader_returned
from comparison
-- A type with no loader rows yet reports a null pair count and a negative delta of the
-- whole baseline, which reads as a failure rather than as "not run yet".
order by activity_type
