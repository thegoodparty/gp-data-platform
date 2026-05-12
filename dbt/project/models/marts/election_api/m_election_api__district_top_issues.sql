{{
    config(
        materialized="table",
        tags=["mart", "election_api", "issue", "haystaq", "district_top_issues"],
    )
}}

{#-
    District-level Haystaq issue scores per L2 district. Covers every L2
    district with an `is_matched = true` row in the LLM L2-to-BallotReady
    district match (`stg_model_predictions__llm_l2_br_match_20260126`). Not
    scoped to a single election cycle — districts with off-cycle offices are
    included as well.

    Grain: up to one row per (district, issue) where the district has at
    least one voter with a non-null score for that issue. Spark UNPIVOT
    defaults to EXCLUDE NULLS, so districts with NULL average scores for an
    issue emit no row for that issue.

    Issue universe, labels, and jurisdictional flags come from the
    `haystaq_issue_tags` seed — the seed is the source of truth. Edit the
    seed to add, remove, or re-tag issues; rebuild the seed before rebuilding
    this model so `dbt_utils.get_column_values` reads the new column list.

    Downstream consumers (election-api) filter by jurisdictional flag and
    re-rank within the filtered set. The mart emits the overall `issue_rank`
    only.
-#}
{%- set issue_columns = dbt_utils.get_column_values(
    ref("haystaq_issue_tags"), "issue", order_by="issue"
) -%}

with
    target_districts as (
        select distinct m.state as l2_state, m.l2_district_type, m.l2_district_name
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as m
        where m.is_matched
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
    ),

    district_issue_tagged as (
        -- Join seed metadata in a dedicated CTE so the final SELECT has only one
        -- source in scope, avoiding ambiguity when `generate_salted_uuid` passes
        -- unqualified column names into the SQL.
        select
            d.l2_state,
            d.l2_district_type,
            d.l2_district_name,
            d.l2_voter_count,
            d.issue,
            d.score,
            t.issue_label,
            t.is_local,
            t.is_regional,
            t.is_state,
            t.is_federal
        from district_issue_long as d
        inner join {{ ref("haystaq_issue_tags") }} as t on t.issue = d.issue
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
    issue_label,
    score,
    is_local,
    is_regional,
    is_state,
    is_federal,
    row_number() over (
        partition by l2_state, l2_district_type, l2_district_name
        order by score desc, issue asc
    ) as issue_rank
from district_issue_tagged
