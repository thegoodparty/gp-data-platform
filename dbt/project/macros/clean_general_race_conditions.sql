{#
    Clean general-cycle race predicate shared by the viability fallback
    models: primaries, runoffs, recalls, disabled and unexpired-term races do
    not carry the cycle's seat count. Strict equality, not coalesce-to-false:
    a NULL flag is UNKNOWN and fails closed (verified 2026-08-10: all five
    flags are fully populated in staging, so strictness costs nothing today
    and guards a future partial load). `alias` prefixes every column.
#}
{% macro clean_general_race_conditions(alias) %}
    {{ alias }}.is_disabled = false
    and {{ alias }}.is_recall = false
    and {{ alias }}.is_primary = false
    and {{ alias }}.is_runoff = false
    and {{ alias }}.is_unexpired = false
    and {{ alias }}.seats > 0
{% endmacro %}
