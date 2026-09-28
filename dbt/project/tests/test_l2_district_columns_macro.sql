-- Unit test for the L2 district column macros. Returns a row per failed
-- assertion, so an empty result is a pass. The expected counts come from the
-- lists themselves: the checks are about how the macro renders a list, not how
-- long the list is.
{% set all_types = get_l2_district_types() %}
{% set allocated_types = get_l2_district_types(scope="allocated") %}
{% set all_columns = get_l2_district_columns() | replace("\n", " ") %}
{% set unpivot_columns = get_l2_district_columns(use_backticks=false) | replace(
    "\n", " "
) %}
{% set cast_columns = get_l2_district_columns(
    scope="allocated", cast_to_string=true
) | replace("\n", " ") %}
{% set alias_columns = get_l2_district_columns(table_alias="v") | replace("\n", " ") %}
with
    checks as (
        -- every allocated type must be a real L2 column, i.e. a member of 'all'
        select
            'allocated_subset_of_all' as check_name,
            {{ allocated_types | reject("in", all_types) | list | length }} as actual,
            0 as expected

        union all

        select
            'allocated_is_distinct',
            {{ allocated_types | unique | list | length }},
            {{ allocated_types | length }}

        union all

        -- backticked SELECT form emits one entry per type
        select
            'columns_all_entry_count',
            size(split('{{ all_columns }}', ',')),
            {{ all_types | length }}

        union all

        -- UNPIVOT form is bare names: no backticks anywhere
        select
            'unpivot_form_has_no_backticks',
            length(regexp_replace('{{ unpivot_columns }}', '[^`]', '')),
            0

        union all

        -- cast form wraps every entry
        select
            'cast_form_casts_every_entry',
            size(split('{{ cast_columns }}', 'as string')) - 1,
            {{ allocated_types | length }}

        union all

        -- alias form qualifies every entry as v.`Col`
        select
            'alias_form_qualifies_every_entry',
            size(split('{{ alias_columns }}', 'v\\.`')) - 1,
            {{ all_types | length }}
    )

select *
from checks
where actual <> expected
