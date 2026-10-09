{{ config(severity="warn") }}

-- Distribution canary on the independent-targeting columns, including the
-- openness-to-an-independent experiment flags. All are
-- national aggregates over the whole voter file, so a monthly L2 vintage moves
-- them by a fraction of a point; a move of more than a point points at a
-- predicate input changing domain, or an edit dropping one of the coalesce
-- wrappers in the affinity or registration-openness unions, rather than at
-- genuine churn.
--
-- Bands rather than floors in both directions: affinity climbing is as much a
-- signal of a broken predicate as affinity falling, and ideology coverage is set
-- by how often the Haystaq model declines to label, which should not jump either
-- way without someone knowing.
--
-- Warns rather than errors. This model gates the people-db loader unload, and a
-- real shift in a Haystaq vintage should show up in run results without holding
-- the loader.
--
-- Single scan; every check derives from the same one-row aggregate.
{% set affinity_rate_min = 0.690 %}
{% set affinity_rate_max = 0.710 %}
{% set ideology_coverage_min = 0.589 %}
{% set ideology_coverage_max = 0.609 %}
{% set registration_rate_min = 0.441 %}
{% set registration_rate_max = 0.461 %}
{#- Haystaq flags 40-41% of scored voters; unscored voters are false, so ~37.5% of the file. #}
{% set haystaq_rate_min = 0.365 %}
{% set haystaq_rate_max = 0.385 %}
{#- Each state-leg flag is 40% of every state, so 40% nationally; a miss means the join
    to int__indep_openness_voter_scores lost voters or the table was left unscored. #}
{% set stateleg_rate_min = 0.39 %}
{% set stateleg_rate_max = 0.41 %}
{% set stateleg_flags = [
    "Indep_Openness_Single_Inc",
    "Indep_Openness_Single_Open",
    "Indep_Openness_Multi_Inc",
    "Indep_Openness_Multi_Open",
    "Indep_Openness_DOnly_Inc",
    "Indep_Openness_DOnly_Open",
    "Indep_Openness_ROnly_Inc",
    "Indep_Openness_ROnly_Open",
    "Indep_Openness_DOnly_Multi",
    "Indep_Openness_ROnly_Multi",
] %}

with
    rates as (
        select
            count_if(`Voter_Independent_Affinity`)
            * 1.0
            / nullif(count(*), 0) as affinity_rate,
            count(`hf_ideology_general`)
            * 1.0
            / nullif(count(*), 0) as ideology_coverage,
            count_if(`Indep_Openness_Registration`)
            * 1.0
            / nullif(count(*), 0) as registration_rate,
            count_if(`Indep_Openness_Haystaq`)
            * 1.0
            / nullif(count(*), 0) as haystaq_rate,
            {%- for f in stateleg_flags %}
                count_if(`{{ f }}`) * 1.0 / nullif(count(*), 0) as `{{ f }}`
                {%- if not loop.last %},{% endif %}
            {%- endfor %}
        from {{ ref("m_people_api__voter") }}
    )

select
    'affinity_true_rate_outside_band' as violation,
    cast(affinity_rate as string) as detail
from rates
where
    affinity_rate is null
    or affinity_rate not between {{ affinity_rate_min }}
    and {{ affinity_rate_max }}
union all
select
    'ideology_coverage_outside_band' as violation,
    cast(ideology_coverage as string) as detail
from rates
where
    ideology_coverage is null
    or ideology_coverage not between {{ ideology_coverage_min }}
    and {{ ideology_coverage_max }}
union all
select
    'registration_openness_rate_outside_band' as violation,
    cast(registration_rate as string) as detail
from rates
where
    registration_rate is null
    or registration_rate not between {{ registration_rate_min }}
    and {{ registration_rate_max }}
union all
select
    'haystaq_openness_rate_outside_band' as violation,
    cast(haystaq_rate as string) as detail
from rates
where
    haystaq_rate is null
    or haystaq_rate not between {{ haystaq_rate_min }}
    and {{ haystaq_rate_max }}
{%- for f in stateleg_flags %}
    union all
    select '{{ f }}_rate_outside_band' as violation, cast(`{{ f }}` as string) as detail
    from rates
    where
        `{{ f }}` is null
        or `{{ f }}` not between {{ stateleg_rate_min }}
        and {{ stateleg_rate_max }}
{%- endfor %}
