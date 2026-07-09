-- DATA-2015: every (election_year, election_code) slice of the nationwide turnout
-- inference must cover every state (50 + DC). The column-level distinct-values test
-- on `state` checks the table as a whole, which would miss one slice losing states
-- while another keeps them (e.g. a partial per-year rebuild). Full coverage per slice
-- is the contract: the model intentionally predicts for every precinct (hypothetical
-- turnout semantics — see the model docstring; research decision 2026-07-07).
-- Returns one row per (slice, missing state); the test fails iff any rows return.
with
    expected_states as (
        {% for state in get_us_states_list(include_DC=true, include_US=false) %}
            select '{{ state }}' as state
            {%- if not loop.last %}
                union all
            {%- endif %}
        {% endfor %}
    ),
    slices as (
        select distinct election_year, election_code
        from {{ ref("int__voter_turnout_lgbm_inference") }}
    ),
    observed as (
        select distinct election_year, election_code, state
        from {{ ref("int__voter_turnout_lgbm_inference") }}
    )
select s.election_year, s.election_code, e.state as missing_state
from slices as s
cross join expected_states as e
left join
    observed as o
    on o.election_year = s.election_year
    and o.election_code = s.election_code
    and o.state = e.state
where o.state is null
