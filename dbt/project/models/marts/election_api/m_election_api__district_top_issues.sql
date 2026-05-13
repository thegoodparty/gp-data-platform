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

    Issue universe is the `issue_columns` list below. Issue labels and the
    four jurisdictional flags come from the `haystaq_issue_tags` seed, joined
    at runtime.

    Why two places: dbt cloud CI compiles the mart against a fresh PR schema
    where the seed isn't materialized yet, so a `dbt_utils.get_column_values`
    lookup against `ref('haystaq_issue_tags')` fails at compile time. The
    inline list is the compile-time source of truth for the column set; the
    seed is the runtime source of truth for labels and flags. The singular
    test `tests/assert_district_top_issues_seed_coverage.sql` asserts every
    seed issue appears in the mart, catching drift between the two if a new
    issue is added to one place but not the other.

    To add or remove an issue: edit both this list (keep it alphabetical) and
    `seeds/haystaq_issue_tags.csv`. The singular test will fail loudly if
    they drift apart.

    Column-name constraint: every value in this list must be a live column on
    int__l2_nationwide_uniform_w_haystaq; the Jinja loops below render
    `AVG(<issue>)` against those identifiers directly. Two known L2 typos are
    preserved verbatim (`hs_aliens_governenment_hiding_much`,
    `hs_mass_deporations_support`) — do not "fix" the spelling here without a
    matching upstream rename. See the haystaq_issue_tags seed description in
    seeds_schema.yaml for the full rule.

    Downstream consumers (election-api) filter by jurisdictional flag and
    re-rank within the filtered set. The mart emits the overall `issue_rank`
    only.
-#}
{%- set issue_columns = [
    "hs_abortion_pro_choice",
    "hs_affordable_housing_gov_has_role",
    "hs_age_limit_support",
    "hs_aliens_governenment_hiding_much",
    "hs_amazon_exploitative",
    "hs_artificial_intelligence_excited",
    "hs_autonomous_vehicles_allow",
    "hs_casino_support",
    "hs_charter_schools_support",
    "hs_china_foreign_policy_advesarial",
    "hs_civil_liberties_support",
    "hs_climate_change_believer",
    "hs_college_admissions_consider_race",
    "hs_community_college_free_support",
    "hs_critical_race_theory_books_ban",
    "hs_crypto_increase_restrictions",
    "hs_death_penalty_support",
    "hs_defense_spending_increase",
    "hs_dei_support",
    "hs_doge_support",
    "hs_econ_anxiety_very_worried",
    "hs_family_medical_leave_support",
    "hs_felon_voting_support",
    "hs_gas_tax_support",
    "hs_general_anti_vax_pro_vax",
    "hs_gentrification_oppose",
    "hs_gig_work_make_employees",
    "hs_green_new_deal_support",
    "hs_gun_control_support",
    "hs_immigration_undesirable",
    "hs_income_inequality_serious",
    "hs_infrastructure_funding_fund_more",
    "hs_insurance_of_last_resort_government_should_provide",
    "hs_israel_military_actions_support",
    "hs_jan_6th_pardons_support",
    "hs_jobs_guarantee_support",
    "hs_marijuana_legal_support",
    "hs_mass_deporations_support",
    "hs_medicaid_expansion_support",
    "hs_medicare_for_all_support",
    "hs_mexican_wall_support",
    "hs_min_wage_15_increase_support",
    "hs_obamacare_aca_protect",
    "hs_online_gambling_more_legal",
    "hs_opioid_crisis_treat",
    "hs_pipeline_fracking_support",
    "hs_police_trust_yes",
    "hs_public_transit_support",
    "hs_rank_choice_voting_support",
    "hs_redistricting_indep_com",
    "hs_regulations_too_harsh",
    "hs_same_sex_marriage_support",
    "hs_school_choice_support",
    "hs_school_funding_more",
    "hs_sell_federal_lands_support",
    "hs_social_media_truth_vs_speech_truth",
    "hs_social_security_tax_increase_support",
    "hs_stadium_public_financing_approve",
    "hs_state_level_fema_support",
    "hs_tax_cuts_support",
    "hs_teachers_union_positive",
    "hs_trans_athlete_yes",
    "hs_trump_tariffs_support",
    "hs_trump_ukraine_policy_support",
    "hs_unions_beneficial",
    "hs_united_healthcare_at_fault",
    "hs_violent_crime_very_worried",
    "hs_voting_fraud_concern_fraud",
] -%}

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
        --
        -- LEFT JOIN (not INNER) so issues in the inline `issue_columns` list
        -- above that are missing from the seed produce rows with NULL labels
        -- and flags, which the `not_null` and `relationships` tests on those
        -- columns surface as loud test failures. With an INNER JOIN the same
        -- drift would silently drop the issue's rows from the mart.
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
        left join {{ ref("haystaq_issue_tags") }} as t on t.issue = d.issue
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
