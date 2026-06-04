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
        as `family like 'win_%'`. Anything unmatched falls through to 'other'
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

        Args:
            event_type_col: SQL expression producing the event_type string.

        Usage:
            {{ amplitude_event_is_recurrent('event_type') }} as is_recurrent
    #}
    {{ event_type_col }}
    in ('Voter Outreach - Campaign Completed', 'Dashboard - Candidate Dashboard Viewed')
{% endmacro %}
