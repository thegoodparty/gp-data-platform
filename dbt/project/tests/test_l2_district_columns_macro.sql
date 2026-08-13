-- Unit test for the L2 district column macros. Returns a row per failed
-- assertion, so an empty result is a pass.
with
    checks as (
        select
            'all_list_size' as check_name,
            {{ get_l2_district_types() | length }} as actual,
            231 as expected

        union all

        select
            'allocated_list_size' as check_name,
            {{ get_l2_district_types(scope="allocated") | length }} as actual,
            62 as expected

        union all

        -- every allocated type must be a real L2 column, i.e. a member of 'all'
        select
            'allocated_subset_of_all' as check_name,
            {{
                get_l2_district_types(scope="allocated") | reject(
                    "in", get_l2_district_types()
                ) | list | length
            }} as actual,
            0 as expected

        union all

        -- the allocated list must not contain duplicates
        select
            'allocated_is_distinct' as check_name,
            {{ get_l2_district_types(scope="allocated") | unique | list | length }}
            as actual,
            {{ get_l2_district_types(scope="allocated") | length }} as expected

        union all

        -- backticked SELECT form emits one entry per type
        select
            'columns_all_entry_count' as check_name,
            size(
                split('{{ get_l2_district_columns() | replace("\n", " ") }}', ',')
            ) as actual,
            231 as expected

        union all

        -- UNPIVOT form is bare names: no backticks anywhere
        select
            'unpivot_form_has_no_backticks' as check_name,
            length(
                regexp_replace(
                    '{{ get_l2_district_columns(use_backticks=false) | replace("\n", " ") }}',
                    '[^`]',
                    ''
                )
            ) as actual,
            0 as expected

        union all

        -- cast form wraps every entry
        select
            'cast_form_casts_every_entry' as check_name,
            size(
                split(
                    '{{ get_l2_district_columns(scope="allocated", cast_to_string=true) | replace("\n", " ") }}',
                    'as string'
                )
            )
            - 1 as actual,
            62 as expected
    )

select *
from checks
where actual <> expected
