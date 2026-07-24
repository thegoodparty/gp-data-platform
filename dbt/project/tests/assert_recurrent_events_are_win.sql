-- Drift guard for DATA-1945: every event_type in the amplitude_event_is_recurrent
-- allowlist must resolve to a win_* family via amplitude_event_family. This is a
-- static check (does not depend on the events appearing in the stream), and so
-- complements the int__amplitude.yaml `is_win or not is_recurrent` invariant, which
-- only fires against observed rows in the materialized table.
--
-- Fails (returns rows) if a recurrent-allowlist event classifies to a non-win family
-- (which would let int__amplitude_win_activity silently roll up non-Win events as Win
-- engagement). Keep this list in sync with amplitude_event_is_recurrent in
-- macros/amplitude_event_taxonomy.sql.
with
    recurrent_events(event_type) as (
        values
            ('Voter Outreach - Campaign Completed'),
            ('Dashboard - Candidate Dashboard Viewed'),
            ('Dashboard - Campaign Plan Viewed')
    )

select event_type, {{ amplitude_event_family("event_type") }} as family
from recurrent_events
where {{ amplitude_event_family("event_type") }} not like 'win_%'
