{#- Age has two band sets that never share an audience, so it gets its own rule
    below. The newer set adds an unknown option; the older one had none. -#}
{% set age_bands = {
    "old": ["age_18_25", "age_25_35", "age_35_50", "age_50_plus"],
    "new": [
        "age_18_24",
        "age_25_34",
        "age_35_49",
        "age_50_64",
        "age_65_plus",
    ],
} %}
{% set families = {
    "gender": ["gender_male", "gender_female", "gender_unknown"],
    "homeowner": [
        "homeowner_yes",
        "homeowner_likely",
        "homeowner_no",
        "homeowner_unknown",
    ],
    "education": [
        "education_none",
        "education_high_school_diploma",
        "education_technical_school",
        "education_some_college",
        "education_college_degree",
        "education_graduate_degree",
        "education_unknown",
    ],
    "children": [
        "has_children_yes",
        "has_children_no",
        "has_children_unknown",
    ],
    "marital": [
        "married",
        "single",
        "likely_married",
        "likely_single",
        "marital_unknown",
    ],
    "ethnicity": [
        "ethnicity_african_american",
        "ethnicity_asian",
        "ethnicity_european",
        "ethnicity_hispanic",
        "ethnicity_other",
        "ethnicity_unknown",
    ],
    "veteran": ["veteran_yes", "veteran_unknown"],
    "business_owner": ["business_owner_yes", "business_owner_unknown"],
} %}
-- The saved audience behind an outreach send, with the demographic choices that
-- tell a targeted audience from a whole-district one. A family narrows the
-- audience only when some but not all of its options are ticked: every age band
-- ticked is the same audience as none. Each family counts once, so
-- "homeowners, college degree" is two dimensions.
with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_voter_file_filter") }}
    ),

    ticked as (
        select
            id as voter_file_filter_id,
            name as filter_name,
            created_at,
            updated_at,
            first_used_for_outreach_at,
            recommended_intent,
            coalesce(recommended_modified, false) as is_recommended_modified,
            {%- for family, cols in families.items() %}
                {% for col in cols -%}
                    cast(coalesce({{ col }}, false) as int){{ " + " if not loop.last }}
                {%- endfor %} as {{ family }}_ticked,
            {%- endfor %}
            {%- for band_set, cols in age_bands.items() %}
                {% for col in cols -%}
                    cast(coalesce({{ col }}, false) as int){{ " + " if not loop.last }}
                {%- endfor %} as age_{{ band_set }}_ticked,
            {%- endfor %}
            cast(coalesce(age_unknown, false) as int) as age_unknown_ticked,
            coalesce(
                size(from_json(income_ranges, 'array<string>')), 0
            ) as income_ticked,
            coalesce(
                size(from_json(language_codes, 'array<string>')), 0
            ) as language_ticked
        from source
    ),

    flagged as (
        select
            * except (
                {%- for family in families %}{{ family }}_ticked, {% endfor -%}
                age_old_ticked,
                age_new_ticked,
                age_unknown_ticked,
                income_ticked,
                language_ticked
            ),
            case
                when age_new_ticked > 0
                then age_new_ticked + age_unknown_ticked between 1 and 5
                else age_old_ticked between 1 and 3
            end as has_age_filter,
            {%- for family, cols in families.items() %}
                {{ family }}_ticked
                between 1 and {{ cols | length - 1 }} as has_{{ family }}_filter,
            {%- endfor %}
            -- Nine income bands and four language options in the product.
            income_ticked between 1 and 8 as has_income_filter,
            language_ticked between 1 and 3 as has_language_filter
        from ticked
    )

select
    *,
    cast(has_age_filter as int)
    + {%- for family in families %} cast(has_{{ family }}_filter as int) + {%- endfor %}
    cast(has_income_filter as int)
    + cast(has_language_filter as int) as demographic_dimension_count
from flagged
