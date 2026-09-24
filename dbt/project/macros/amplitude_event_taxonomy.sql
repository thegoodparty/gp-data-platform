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
        when
            {{ event_type_col }} like 'Voter Outreach -%'
            -- Robocall is a voter-outreach channel that the product named
            -- without the prefix, so it fell through to 'other' and was
            -- invisible to every family-based Win read.
            or {{ event_type_col }} like 'Robocall -%'
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

        Recurrence is an event-level property (not a family property), so this is
        an allowlist rather than a pattern. The set is the union of the anchor
        declarations of the two governed metrics the Win activity rollups serve,
        read from the semantic layer rather than restated here. That direction
        matters: this allowlist is the rollups' intake gate, so a leg added to a
        metric but missing here never reaches the model that computes it, and the
        metric reads as unchanged while looking correctly declared.

        Pathed legs are deliberately dropped. A pathed leg is a slice of a
        site-wide event ('Viewed' at '/dashboard'), and an event-name allowlist
        admitting the bare name would pull in every row of a 4.5M-row event.
        Consumers that intake by is_recurrent must therefore admit the page-path
        leg explicitly alongside it, which is what the rollups do.

        Args:
            event_type_col: SQL expression producing the event_type string.

        Usage:
            {{ amplitude_event_is_recurrent('event_type') }} as is_recurrent
    #}
    {%- set anchored_metrics = ["win_active_candidates_30d", "win_activated_users"] -%}
    {%- if not execute -%}
        {#- Parse time only: graph is empty. Gate on `not execute`, never on an
            empty name list, which must raise at execute time. -#}
        (false)
    {%- else -%}
        {%- set names = [] -%}
        {%- for metric_name in anchored_metrics -%}
            {%- for leg in metric_anchored_events(metric_name) -%}
                {%- if not leg["path"] and leg["event"] not in names -%}
                    {%- do names.append(leg["event"]) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
        {%- if names | length == 0 -%}
            {{
                exceptions.raise_compiler_error(
                    "amplitude_event_is_recurrent: the anchored metrics resolved to zero "
                    "name-based legs at execute time. Refusing to emit a predicate that "
                    "would empty the Win activity rollups."
                )
            }}
        {%- endif -%}
        {{ event_type_col }} in (
            {%- for event in names | sort %}'{{ event }}'{{ "," if not loop.last }}
            {%- endfor %}
        )
    {%- endif -%}
{% endmacro %}

{% macro metric_anchored_events(metric_name) %}
    {#
        Legs of a governed metric's `config.meta.anchored_on` (DATA-2421), as dicts
        with keys event / path / era / excluding. The semantic layer is the kernel:
        this reads the declaration rather than restating it, so the macro cannot
        drift from the metric it serves.

        `path` narrows a leg to one page-path slice of a site-wide event.
        `excluding` narrows a leg by an event property, as {property: value};
        each consuming macro decides which property keys it can compile and must
        raise on one it cannot, so an exclusion can never be silently ignored.
        `era` is documentation, not a filter: dead legs stay in the predicate so
        history is preserved.

        Empty at parse time (execute=false), same as the seed accessors in
        hubspot_contact_property_columns.sql. Callers building a predicate MUST emit a
        parse-safe fallback for the empty case — see is_dashboard_view_event.

        The metric lives downstream of the models calling this. Reading `graph` creates
        no ref edge, so there is no cycle, but the direction is deliberate and worth
        knowing about before you move it.
    #}
    {%- set legs = [] -%}
    {%- if execute -%}
        {%- set matches = (
            graph.metrics.values()
            | selectattr("name", "equalto", metric_name)
            | list
        ) -%}
        {%- if matches | length == 0 -%}
            {{
                exceptions.raise_compiler_error(
                    "metric_anchored_events: no metric named '" ~ metric_name ~ "'"
                )
            }}
        {%- endif -%}
        {%- set declared = matches[0].config.meta.get("anchored_on") -%}
        {%- if not declared -%}
            {{
                exceptions.raise_compiler_error(
                    "metric_anchored_events: '"
                    ~ metric_name
                    ~ "' declares no anchored_on"
                )
            }}
        {%- endif -%}
        {%- for leg in declared -%}
            {%- do legs.append(
                {
                    "event": leg["event"],
                    "path": leg.get("path"),
                    "era": leg.get("era"),
                    "excluding": leg.get("excluding") or {},
                }
            ) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(legs) }}
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
    {%- set legs = metric_anchored_events("win_active_candidates_30d") -%}
    {%- if not execute -%}
        {#-
            Parse time only: graph is empty, and this SQL is validated but never run.
            Gate on `not execute`, NEVER on `legs | length == 0`. An empty leg list while
            execute is true must raise, because emitting (false) there would mark every
            user inactive and read Active Candidates as zero — which is exactly the
            2026-06-13 failure this whole ticket exists to prevent, reintroduced by its
            own fix.
        -#}
        (false)
    {%- elif legs | length == 0 -%}
        {{
            exceptions.raise_compiler_error(
                "is_dashboard_view_event: win_active_candidates_30d resolved to zero legs at "
                "execute time. Refusing to emit a predicate that would zero the metric."
            )
        }}
    {%- else -%}
        {%- set named = legs | rejectattr("path") | map(attribute="event") | list -%}
        {%- set pathed = legs | selectattr("path") | list -%}
        (
            {%- for leg in pathed %}
                (
                    {{ event_type_col }} = '{{ leg["event"] }}'
                    and {{ page_path_col }} = '{{ leg["path"] }}'
                )
                {%- if not loop.last or named | length > 0 %} or {% endif -%}
            {%- endfor %}
            {%- if named | length > 0 %}
                {{ event_type_col }} in (
                    {%- for event in named %}
                        '{{ event }}'{{ "," if not loop.last }}
                    {%- endfor %}
                )
            {%- endif %}
        )
    {%- endif -%}
{% endmacro %}

{% macro is_outreach_activation_event(event_type_col, method_col) %}
    {#
        Membership test for a voter-outreach send that the product observed.

        Anchored on the same declaration the metric publishes, for the same reason
        is_dashboard_view_event is: the outreach surface has been rebuilt twice and
        each rebuild silently retired the event the number was computed from. The
        legacy in-product send leg stopped firing on 2026-09-08 when the flow moved
        to outreach/v2/, and the count did not visibly fall, because the self-report
        modal shares the event name and absorbed it.

        Which is why the `method` property matters here. One event name covers three
        different moments: no `method` was the legacy product-executed send,
        'native' is a completed door-knocking walk, and 'manual' is a candidate
        typing in something they did elsewhere. Only the first two are outreach this
        product performed, so 'manual' is excluded by declaration. A null method
        passes, because the leg that predates the property is a real send.

        Excluding by property is narrow on purpose: this macro compiles a `method`
        exclusion and raises on any other key, so a declared exclusion this macro
        cannot express fails the build instead of quietly widening the metric.

        Args:
            event_type_col: SQL expression producing the event_type string.
            method_col: SQL expression producing the event's `method` property
                (event_properties:method::string).
    #}
    {%- set legs = metric_anchored_events("win_activated_users") -%}
    {%- if not execute -%}
        {#- Parse time only: graph is empty. Gate on `not execute`, never on an
            empty leg list, which must raise at execute time. -#}
        (false)
    {%- elif legs | length == 0 -%}
        {{
            exceptions.raise_compiler_error(
                "is_outreach_activation_event: win_activated_users resolved to zero legs "
                "at execute time. Refusing to emit a predicate that would read activation "
                "as zero."
            )
        }}
    {%- else -%}
        {%- set plain = [] -%}
        {%- set qualified = [] -%}
        {%- for leg in legs -%}
            {%- if leg["path"] -%}
                {{
                    exceptions.raise_compiler_error(
                        "is_outreach_activation_event: leg '"
                        ~ leg["event"]
                        ~ "' declares a page path, which this macro cannot compile."
                    )
                }}
            {%- elif leg["excluding"] -%}
                {%- for property_key in leg["excluding"] -%}
                    {%- if property_key != "method" -%}
                        {{
                            exceptions.raise_compiler_error(
                                "is_outreach_activation_event: leg '"
                                ~ leg["event"]
                                ~ "' excludes on '"
                                ~ property_key
                                ~ "', but this macro only compiles a 'method' exclusion."
                            )
                        }}
                    {%- endif -%}
                {%- endfor -%}
                {%- set excluded = leg["excluding"]["method"] -%}
                {%- do qualified.append(
                    {
                        "event": leg["event"],
                        "methods": (
                            excluded
                            if excluded is sequence
                            and excluded is not string
                            else [excluded]
                        ),
                    }
                ) -%}
            {%- else -%} {%- do plain.append(leg["event"]) -%}
            {%- endif -%}
        {%- endfor -%}
        (
            {%- for leg in qualified %}
                (
                    {{ event_type_col }} = '{{ leg["event"] }}'
                    and coalesce({{ method_col }}, '') not in (
                        {%- for method in leg["methods"] %}
                            '{{ method }}'{{ "," if not loop.last }}
                        {%- endfor %}
                    )
                )
                {%- if not loop.last or plain | length > 0 %} or {% endif -%}
            {%- endfor %}
            {%- if plain | length > 0 %}
                {{ event_type_col }} in (
                    {%- for event in plain | sort %}
                        '{{ event }}'{{ "," if not loop.last }}
                    {%- endfor %}
                )
            {%- endif %}
        )
    {%- endif -%}
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
