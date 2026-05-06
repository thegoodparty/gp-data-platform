{{
    config(
        materialized="table",
        tags=["mart", "election_api", "issue", "haystaq", "district_top_issues"],
    )
}}

{#-
    Top 5 Haystaq issue scores per L2 district, scoped to districts that have
    a 2026 BallotReady election (via the LLM-derived BR-position to L2-district
    match). One row per (district x issue) for the top 5 issues by average score.

    The (column, label) pairs below are the single source of truth for the 24
    Haystaq issue scores used by this model. The Jinja loops below expand them
    into the AVG aggregation, the UNPIVOT, and the issue-label CASE.
-#}
{%- set issues = [
    ("hs_charter_schools_support", "Support Charter Schools"),
    ("hs_school_choice_support", "Support School Choice"),
    ("hs_school_funding_more", "Increase School Funding"),
    ("hs_dei_support", "Support Diversity & Inclusion"),
    ("hs_civil_liberties_support", "Support Civil Liberties"),
    (
        "hs_affordable_housing_gov_has_role",
        "Government Should Address Affordable Housing",
    ),
    ("hs_public_transit_support", "Support Public Transit"),
    ("hs_min_wage_15_increase_support", "Support a $15 Minimum Wage"),
    ("hs_tax_cuts_support", "Support Tax Cuts"),
    ("hs_medicaid_expansion_support", "Support Medicaid Expansion"),
    ("hs_medicare_for_all_support", "Support Medicare For All"),
    ("hs_abortion_pro_choice", "Pro-Choice on Abortion"),
    ("hs_same_sex_marriage_support", "Support Same-Sex Marriage"),
    ("hs_climate_change_believer", "Believe in Climate Change"),
    ("hs_marijuana_legal_support", "Support Legalizing Marijuana"),
    ("hs_gun_control_support", "Support Gun Control"),
    ("hs_death_penalty_support", "Support the Death Penalty"),
    ("hs_police_trust_yes", "Trust the Police"),
    ("hs_violent_crime_very_worried", "Worried About Violent Crime"),
    (
        "hs_felon_voting_support",
        "Support Voting Rights for People with Felony Convictions",
    ),
    ("hs_rank_choice_voting_support", "Support Ranked-Choice Voting"),
    ("hs_pipeline_fracking_support", "Support Pipelines and Fracking"),
    ("hs_casino_support", "Support Legal Casinos"),
    ("hs_immigration_undesirable", "View Immigration Negatively"),
] -%}

{%- set issue_columns = issues | map(attribute=0) | list -%}

with
    target_districts as (
        select distinct m.state as l2_state, m.l2_district_type, m.l2_district_name
        from {{ ref("election") }} as e
        inner join
            {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as m
            on m.br_database_id = e.br_position_database_id
            and m.is_matched
        where e.election_year = 2026
    ),

    l2_voter_data as (
        select
            state_postal_code,
            {{ get_l2_district_columns(use_backticks=true, cast_to_string=true) }},
            {{ issue_columns | join(",\n            ") }}
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
    ),

    voter_district_scores as (
        select
            state_postal_code as l2_state,
            district_column_name as l2_district_type,
            district_value as l2_district_name,
            {{ issue_columns | join(",\n            ") }}
        from
            l2_voter_data unpivot (
                district_value for district_column_name
                in ({{ get_l2_district_columns(use_backticks=false) }})
            )
        where district_value is not null
        union all
        select
            state_postal_code as l2_state,
            'State' as l2_district_type,
            state_postal_code as l2_district_name,
            {{ issue_columns | join(",\n            ") }}
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
    ),

    district_avg_scores as (
        select
            v.l2_state,
            v.l2_district_type,
            v.l2_district_name,
            count(*) as l2_voter_count,
            {%- for column in issue_columns %}
                avg({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        from voter_district_scores as v
        inner join
            target_districts as t
            on t.l2_state = v.l2_state
            and t.l2_district_type = v.l2_district_type
            and t.l2_district_name = v.l2_district_name
        group by 1, 2, 3
    ),

    district_issue_long as (
        select
            l2_state, l2_district_type, l2_district_name, l2_voter_count, issue, score
        from
            district_avg_scores
            unpivot (score for issue in ({{ issue_columns | join(", ") }}))
    )

select
    {{
        generate_salted_uuid(
            fields=[
                "l2_state",
                "l2_district_type",
                "l2_district_name",
                "issue",
            ]
        )
    }} as id,
    {{
        generate_salted_uuid(
            fields=[
                "l2_state",
                "l2_district_type",
                "l2_district_name",
            ]
        )
    }} as district_id,
    current_timestamp() as created_at,
    current_timestamp() as updated_at,
    l2_state,
    l2_district_type,
    l2_district_name,
    l2_voter_count,
    issue,
    case
        issue
        {%- for column, label in issues %}
            when '{{ column }}' then '{{ label }}'
        {%- endfor %}
    end as issue_label,
    score,
    row_number() over (
        partition by l2_state, l2_district_type, l2_district_name order by score desc
    ) as issue_rank
from district_issue_long
qualify issue_rank <= 5
