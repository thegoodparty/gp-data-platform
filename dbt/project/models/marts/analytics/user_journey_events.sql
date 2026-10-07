-- The dated moments on each user's journey, one row per user per event, for a
-- single-user timeline. Internal only.
--
-- Reshaping only. Every date is a user_journey column, so this table cannot
-- disagree with it. An event with no date has no row.
--
-- Elections: the primary when the candidacy had one, then the general unless
-- the civics record says the run ended at the primary. latest_stage_reached
-- decides that, not the primary result, because the two disagree for some
-- candidacies. Without a civics candidacy the anchor date stands in for the
-- general; it is the general for nearly every matched run.
with
    journey as (
        select
            user_id,
            registered_at,
            onboarding_completed_at,
            free_product_output_at,
            first_payment_at,
            pro_since,
            activated_at,
            candidacy_primary_election_date,
            candidacy_primary_result,
            coalesce(
                candidacy_general_election_date, candidacy_election_date
            ) as general_election_date,
            candidacy_general_result,
            coalesce(
                candidacy_latest_stage_reached like 'primary%', false
            ) as ended_at_primary
        from {{ ref("user_journey") }}
    ),

    events as (
        select
            user_id,
            'Registered' as event,
            1 as event_order,
            registered_at as event_at,
            cast(null as string) as event_label
        from journey
        union all
        select user_id, 'Onboarded', 2, onboarding_completed_at, null
        from journey
        union all
        select user_id, 'Free product output', 3, free_product_output_at, null
        from journey
        union all
        select user_id, 'First payment', 4, first_payment_at, null
        from journey
        union all
        select user_id, 'Pro', 5, pro_since, null
        from journey
        union all
        select user_id, 'Activated', 6, activated_at, null
        from journey
        union all
        select
            user_id,
            'Primary election',
            7,
            cast(candidacy_primary_election_date as timestamp),
            candidacy_primary_result
        from journey
        union all
        select
            user_id,
            'General election',
            8,
            cast(general_election_date as timestamp),
            candidacy_general_result
        from journey
        where not ended_at_primary
    )

select user_id, event, event_order, event_at, event_label
from events
where event_at is not null
