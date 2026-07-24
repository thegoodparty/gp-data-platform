-- Drift guard for DATA-1945: every event_type that int__amplitude_user_milestones
-- aggregates must classify to a real (non-'other') family in the single-source
-- taxonomy. Classification is checked directly through the amplitude_event_family
-- macro (not via the materialized model), so this guards the pattern logic itself
-- regardless of whether each event has appeared in the current stream window.
--
-- Fails (returns rows) if a milestone event falls through to 'other', i.e. the
-- pattern set no longer covers it. Keep this list in sync with the event_type
-- filter in int__amplitude_user_milestones.sql.
with
    milestone_events(event_type) as (
        values
            ('Onboarding - Registration Completed'),
            ('onboarding_complete'),
            ('pro_upgrade_complete'),
            ('Voter Outreach - Campaign Completed'),
            ('Dashboard - Candidate Dashboard Viewed'),
            ('Dashboard - Campaign Plan Viewed'),
            ('Serve Onboarding - Getting Started Viewed'),
            ('Serve Onboarding - Constituency Profile Viewed'),
            ('Serve Onboarding - Poll Value Props Viewed'),
            ('Serve Onboarding - Poll Strategy Viewed'),
            ('Serve Onboarding - Add Image Viewed'),
            ('Serve Onboarding - Poll Preview Viewed'),
            ('Serve Onboarding - SMS Poll Sent')
    )

select event_type, {{ amplitude_event_family("event_type") }} as family
from milestone_events
where {{ amplitude_event_family("event_type") }} = 'other'
