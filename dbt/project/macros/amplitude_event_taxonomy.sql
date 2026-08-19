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
        -- Third-generation dashboard-view event. Named 'Campaign Plan -%' by the
        -- product, so it would otherwise fall to win_compliance_or_planning and
        -- drop out of every family-based dashboard read. The sibling
        -- 'Campaign Plan - Weekly Tasks Digest' deliberately stays in
        -- compliance_or_planning: it is a server-emitted weekly digest
        -- (session_id = -1, ~1.3k recipients per batch), not a surface view.
        when {{ event_type_col }} = 'Campaign Plan - Campaign Tracker Viewed'
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

        The allowlist now carries 4 events total (1 campaign-outreach event plus
        the 3 generations of named dashboard-view event, each of which replaced
        its predecessor when the dashboard surface was rebuilt). This list stays
        event-name-based, so it does NOT cover the page-path leg of
        is_dashboard_view_event: 'Viewed' is a site-wide page event and only its
        '/dashboard' rows are dashboard views, which an event_type allowlist
        cannot express. Consumers that intake by is_recurrent must therefore
        admit the page-path leg explicitly alongside it.

        Args:
            event_type_col: SQL expression producing the event_type string.

        Usage:
            {{ amplitude_event_is_recurrent('event_type') }} as is_recurrent
    #}
    {{ event_type_col }} in (
        'Voter Outreach - Campaign Completed',
        'Dashboard - Candidate Dashboard Viewed',
        'Dashboard - Campaign Plan Viewed',
        'Campaign Plan - Campaign Tracker Viewed'
    )
{% endmacro %}

{% macro is_dashboard_view_event(event_type_col, page_path_col) %}
    {#
        Membership test for a candidate-dashboard view.

        Anchored on the page path, not on the surface event name. The product has
        renamed the dashboard-view event on every rebuild of the surface, and each
        rename silently zeroed every metric built on it:
          - 'Dashboard - Candidate Dashboard Viewed'  died in-data 2026-06-13
          - 'Dashboard - Campaign Plan Viewed'        died in-data 2026-07-31
          - 'Campaign Plan - Campaign Tracker Viewed' live from 2026-08-07
        The site-wide 'Viewed' page event with path '/dashboard' has run
        continuously since 2025-04-21, predating the first named event, and passes
        through both deaths with no discontinuity. Over a window where the legacy
        event was healthy (2025-08 -> 2025-10) the two agree on 98.6% of users
        (5,232 of 5,306 legacy users; 71 path-only), so the path leg is a
        like-for-like substitute rather than a broader proxy.

        The three named events are kept as an OR so the definition is additive and
        no history is lost. They contribute ~1.4% of users beyond the path leg.
        Only path '/dashboard' counts, not '/dashboard%': the sub-pages are
        distinct surfaces (the 2026-08 successor fires mainly on
        '/dashboard/campaign-plan'), and admitting them would silently widen this
        from "viewed the dashboard" to "used the app".

        Because the legs co-fire on a single visit, raw counts over this predicate
        over-count (use dashboard_view_is_new for counts); MIN/MAX/EXISTS and
        COUNT(DISTINCT date) are co-fire-safe.

        Args:
            event_type_col: SQL expression producing the event_type string.
            page_path_col: SQL expression producing the page path
                (event_properties:path::string).
    #}
    (
        ({{ event_type_col }} = 'Viewed' and {{ page_path_col }} = '/dashboard')
        or {{ event_type_col }} in (
            'Dashboard - Candidate Dashboard Viewed',
            'Dashboard - Campaign Plan Viewed',
            'Campaign Plan - Campaign Tracker Viewed'
        )
    )
{% endmacro %}

{% macro dashboard_view_is_new(event_time_col, partition_col, gap_seconds=30) %}
    {#
        Time-gap sessionization for de-duplicating dashboard-view counts where a
        single visit fires more than one member of the union: the page event plus
        whichever named surface event is live, and during 2026-04-09 -> 2026-06-13
        two named events as well. TRUE for a user's first dashboard view and for any
        view whose gap from the prior dashboard event exceeds gap_seconds; co-fired
        events collapse to one regardless of how many legs fire, genuine re-visits
        still count. The 30s gap is unchanged: the 2026-08 successor does not
        co-fire with its predecessor (1 of 19 consecutive pairs inside 30s), so the
        third era gave no reason to move it. Apply only to rows
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
