{{ config(severity="warn") }}

-- Warns on a partial or wrong upload: too few rows for a cumulative file this late in
-- early voting, or too few ids matching the voter table. ~0.2% legitimately miss it,
-- being registered after the monthly L2 build.
{% set row_floor = 20000000 %}
{% set min_match_rate = 0.99 %}

with
    matched as (
        select count(*) as n_rows, count(v.id) as n_matched
        from {{ ref("gp_api_voter_eav_general_2026") }} as e
        left join {{ ref("gp_api_voters") }} as v on e.id = v.id
    )
select n_rows, n_matched, n_matched / n_rows as match_rate
from matched
where n_rows < {{ row_floor }} or n_matched / n_rows < {{ min_match_rate }}
