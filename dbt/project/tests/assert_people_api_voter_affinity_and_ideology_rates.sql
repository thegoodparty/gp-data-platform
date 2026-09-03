{{ config(severity="warn") }}

-- Distribution canary on the two independent-targeting columns. Both are
-- national aggregates over the whole voter file, so a monthly L2 vintage moves
-- them by a fraction of a point; a move of more than a point points at a
-- predicate input changing domain, or an edit dropping one of the coalesce
-- wrappers in the affinity union, rather than at genuine churn.
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
-- Single scan; both checks derive from the same one-row aggregate.
{% set affinity_rate_min = 0.690 %}
{% set affinity_rate_max = 0.710 %}
{% set ideology_coverage_min = 0.589 %}
{% set ideology_coverage_max = 0.609 %}

with
    rates as (
        select
            count_if(`Voter_Independent_Affinity`)
            * 1.0
            / nullif(count(*), 0) as affinity_rate,
            count(`hf_ideology_general`)
            * 1.0
            / nullif(count(*), 0) as ideology_coverage
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
