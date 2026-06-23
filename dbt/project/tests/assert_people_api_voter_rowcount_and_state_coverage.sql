-- DATA-1906 pre-unload gate: the people-api voter mart must have a plausible total
-- row count
-- and cover every state the loader unloads (50 states + DC). Returns one row per
-- violation,
-- so the singular test fails iff any rows are returned.
--
-- Floor is a conservative band under the inspected ~218M; tune if the source grows
-- materially.
-- Built as a left-join spine from get_us_states_list (matching the repo's
-- coverage-test style),
-- avoiding explode() so it stays portable across the warehouse dialect.
{% set row_floor = 200000000 %}

with
    expected_states as (
        {% for state in get_us_states_list(include_DC=true, include_US=false) %}
            select '{{ state }}' as state
            {%- if not loop.last %}
                union all
            {%- endif %}
        {% endfor %}
    ),
    -- single scan of the 218M-row mart; both checks derive from this ~51-row aggregate
    state_counts as (
        select `State` as state, count(*) as n
        from {{ ref("m_people_api__voter") }}
        group by `State`
    ),
    missing_states as (
        select e.state
        from expected_states as e
        left join state_counts as c on c.state = e.state
        where c.state is null
    ),
    total as (select coalesce(sum(n), 0) as n from state_counts)
select 'missing_state' as violation, state as detail
from missing_states
union all
select 'row_count_below_floor' as violation, cast(n as string) as detail
from total
where n < {{ row_floor }}
