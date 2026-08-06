{% macro amplitude_event_family(event_type_col) %}
    {#
        Classify an Amplitude event_type into a product-feature family.

        Pattern-based (LIKE / IN), so new event_types within a known family
        classify automatically as the product ships them (e.g. the onboarding
        redesign of ~2026-05-06 added many 'Onboarding -%' events that land in
        win_onboarding with no change here). Patterns are grounded in the
        webapp event catalog (gp-webapp/helpers/analyticsHelper.ts EVENTS map)
        and the win-analytics-knowledge skill's references/engagement.md.

        Win families are prefixed `win_`; the is_win flag downstream is derived
        as `family like 'win_%'`. Serve is a single flat family, so is_serve is
        derived as `family = 'serve'`. Anything unmatched falls through to 'other'
        so unclassified events surface for triage rather than silently dropping.

        Args:
            event_type_col: SQL expression producing the event_type string.

        Usage:
            {{ amplitude_event_family('event_type') }} as family
    #}
    case
        -- Win product families
        when
            {{ event_type_col }} like 'Onboarding -%'
            or {{ event_type_col }} like 'Onboarding:%'
            -- 'Onboarding V2 -%' is the 2026-06 onboarding redesign (candidate
            -- steps: ballot status, office, votes-needed, voter insights); the
            -- 'V2' means it misses the 'Onboarding -%' pattern above.
            or {{ event_type_col }} like 'Onboarding V2 -%'
            or {{ event_type_col }}
            in ('onboarding_complete', 'Invalid Party', 'Sign Up Clicked')
        then 'win_onboarding'
        when {{ event_type_col }} like 'Dashboard -%'
        then 'win_dashboard'
        when {{ event_type_col }} like 'Voter Outreach -%'
        then 'win_voter_outreach'
        when {{ event_type_col }} like 'Outreach -%'
        then 'win_outreach_planning'
        when
            {{ event_type_col }} like 'Schedule Text Campaign%'
            or {{ event_type_col }} like 'schedule_campaign%'
        then 'win_outreach_scheduling'
        when
            {{ event_type_col }} like 'Content Builder%'
            or {{ event_type_col }} like 'ai_content_%'
            or {{ event_type_col }} like 'campaign_assistant%'
        then 'win_content_builder'
        when
            {{ event_type_col }} like 'Voter Data -%'
            or {{ event_type_col }} like 'Voter Data:%'
            or {{ event_type_col }} like 'Download Voter%'
            or {{ event_type_col }} like 'Custom Voter%'
        then 'win_voter_data'
        when {{ event_type_col }} like 'Profile -%'
        then 'win_candidate_profile'
        when
            {{ event_type_col }} like 'Pro Upgrade -%'
            or {{ event_type_col }} like 'Pro Upgrade:%'
            or {{ event_type_col }} = 'pro_upgrade_complete'
        then 'win_pro_upgrade'
        when {{ event_type_col }} like 'P2P Upgrade -%'
        then 'win_p2p_upgrade'
        when {{ event_type_col }} like 'Candidate Website%'
        then 'win_candidate_website'
        when {{ event_type_col }} like 'Candidacy -%'
        then 'win_candidacy_self_report'
        when
            {{ event_type_col }} like 'Campaign Verify%'
            or {{ event_type_col }} like 'Campaign Plan%'
            or {{ event_type_col }} like '10 DLC Compliance%'
            or {{ event_type_col }} like '10DLC%'
        then 'win_compliance_or_planning'
        when
            {{ event_type_col }} like 'AI Assistant%'
            or {{ event_type_col }} = 'question_complete'
        then 'win_ai_assistant'
        when {{ event_type_col }} like 'Briefings -%'
        then 'win_briefings'
        when {{ event_type_col }} like 'Contacts -%'
        then 'win_contacts'
        when {{ event_type_col }} like 'Resources -%'
        then 'win_resources'
        -- Non-Win / cross-product / noise
        when
            {{ event_type_col }} like 'Serve Onboarding%'
            or {{ event_type_col }} like 'Poll - %'
            or {{ event_type_col }} like 'Polls -%'
            or {{ event_type_col }} like 'Polls:%'
            or {{ event_type_col }} like 'Payment -%'
            -- 2026-05/06 Serve generation. Briefing Assistant and Org Switcher
            -- are live; Community Issues has no events in the stream yet, so its
            -- pattern is speculative and classifies nothing until it ships.
            -- Briefing Assistant includes server-emitted events (Agenda Created,
            -- session_id = -1): family = serve is correct, but engagement
            -- filtering of those stays with the consumer, not the family bucket.
            or {{ event_type_col }} like 'Briefing Assistant -%'
            or {{ event_type_col }} like 'Community Issues -%'
            or {{ event_type_col }} like 'Org Switcher -%'
        then 'serve'
        when
            {{ event_type_col }} like 'Sign In:%'
            or {{ event_type_col }} like 'Sign Up:%'
            or {{ event_type_col }} like 'Set Password:%'
            or {{ event_type_col }} like 'Account -%'
            or {{ event_type_col }} like 'Settings -%'
        then 'auth_or_settings'
        when
            {{ event_type_col }} like 'Navigation -%'
            or {{ event_type_col }} like 'Navigation Top -%'
        then 'navigation'
        when {{ event_type_col }} = 'Viewed'
        then 'viewed_generic'
        when {{ event_type_col }} like '[Amplitude]%'
        then 'amplitude_autotrack'
        when
            {{ event_type_col }} like '[Experiment]%'
            or {{ event_type_col }} = 'Experiment Viewed'
        then 'experiment_assignment'
        when
            {{ event_type_col }} in (
                'Scroll Depth',
                'session_start',
                'session_end',
                'page_view',
                'Page Viewed',
                'Page',
                'usersnap_submission'
            )
            or {{ event_type_col }} like 'Segment Consent%'
        then 'session_or_browser'
        else 'other'
    end
{% endmacro %}


{% macro amplitude_event_is_recurrent(event_type_col) %}
    {#
        Flag recurrent-activity events vs one-off lifecycle milestones.

        Recurrence is an event-level property (not a family property), so this
        is a short explicit allowlist rather than a pattern. The set matches
        exactly the events modeled by the Win activity rollups
        (int__amplitude_win_activity and its weekly variant). Extend this list
        when a genuinely recurrent activity event is added to those rollups.

        The allowlist now carries 3 events total (1 campaign-outreach event
        plus 2 dashboard-view events): the legacy
        'Dashboard - Candidate Dashboard Viewed' and its live replacement
        'Dashboard - Campaign Plan Viewed' co-fired Apr-Jun 2026 during the
        dashboard-surface migration (see is_dashboard_view_event /
        dashboard_view_is_new for the union and co-fire dedup).

        Args:
            event_type_col: SQL expression producing the event_type string.

        Usage:
            {{ amplitude_event_is_recurrent('event_type') }} as is_recurrent
    #}
    {{ event_type_col }} in (
        'Voter Outreach - Campaign Completed',
        'Dashboard - Candidate Dashboard Viewed',
        'Dashboard - Campaign Plan Viewed'
    )
{% endmacro %}

{% macro is_dashboard_view_event(event_type_col) %}
    {#
        Membership test for a candidate-dashboard view, spanning the 2026-05/06
        dashboard-surface migration. The legacy event
        'Dashboard - Candidate Dashboard Viewed' died in-data 2026-06-13 when the
        rebuild replaced the surface with the Campaign Plan view, which fires
        'Dashboard - Campaign Plan Viewed' (live 2026-04-09). The two co-fired
        2026-04-09 -> 2026-06-13, so raw counts over this union double-count that
        window (use dashboard_view_is_new for counts); MIN/MAX/EXISTS and
        COUNT(DISTINCT date) over the union are co-fire-safe.

        Args:
            event_type_col: SQL expression producing the event_type string.
    #}
    {{ event_type_col }}
    in ('Dashboard - Candidate Dashboard Viewed', 'Dashboard - Campaign Plan Viewed')
{% endmacro %}

{% macro dashboard_view_is_new(event_time_col, partition_col, gap_seconds=30) %}
    {#
        Time-gap sessionization for de-duplicating dashboard-view counts across
        the 2026-04-09 -> 2026-06-13 co-fire window, where a single visit fired
        both dashboard events. TRUE for a user's first dashboard view and for any
        view whose gap from the prior dashboard event exceeds gap_seconds; co-fired
        pairs collapse to one, genuine re-visits still count. Apply only to rows
        already filtered to is_dashboard_view_event, materialize the result as a
        boolean column in a CTE/subquery, then count_if that column in an outer
        query (a window function cannot be nested directly inside count_if).

        Args:
            event_time_col: SQL expression for the event timestamp.
            partition_col: SQL expression for the per-user partition key.
            gap_seconds: collapse window in seconds (co-fire threshold).
    #}
    case
        when
            lag({{ event_time_col }}) over (
                partition by {{ partition_col }} order by {{ event_time_col }}
            )
            is null
            or {{ event_time_col }} > lag({{ event_time_col }}) over (
                partition by {{ partition_col }} order by {{ event_time_col }}
            )
            + interval {{ gap_seconds }} seconds
        then true
        else false
    end
{% endmacro %}
