{% macro november_general_election_day(year_expr) %}
    {#- The federal general-election day: the Tuesday next after the first
        Monday in November (2 U.S.C. 7). next_day(d, 'MON') returns the first
        Monday strictly AFTER d, so anchoring at Oct 31 (Nov 1 - 1 day) keeps
        a Nov-1-is-a-Monday year resolving to Nov 1 + 1 = Nov 2 correctly and
        never slides to Nov 8. -#}
    date_add(next_day(make_date({{ year_expr }}, 11, 1) - interval 1 day, 'MON'), 1)
{% endmacro %}
