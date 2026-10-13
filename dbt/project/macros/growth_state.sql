{% macro growth_state(days_col, prev_1_col, prev_2_col, is_first_col) %}
    {#
        The five monthly growth states, from a month's active days and the two
        months before it. One definition so the all-activity state and the Serve
        state cannot drift apart.

        'dormant' is kept apart from 'churned' because a user idle for three
        months or more is no longer one we just lost.
    #}
    case
        when {{ days_col }} > 0 and {{ is_first_col }}
        then 'new'
        when {{ days_col }} > 0 and {{ prev_1_col }} > 0
        then 'retained'
        when {{ days_col }} > 0
        then 'resurrected'
        when {{ prev_1_col }} > 0 or {{ prev_2_col }} > 0
        then 'churned'
        else 'dormant'
    end
{% endmacro %}
