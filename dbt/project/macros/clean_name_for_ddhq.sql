{% macro clean_name_for_ddhq(name_column) %}
    {#
    Clean and standardize name fields for DDHQ matching

    Args:
        name_column: The column name to clean (e.g., 'candidate_name')

    Usage:
        select
            {{ clean_name_for_ddhq('candidate_name') }} as cleaned_candidate_name
        from my_table
    #}
    case
        -- Convert to string and strip whitespace, then handle nulls and empty strings
        when {{ name_column }} is null
        then null
        when trim(cast({{ name_column }} as string)) = ''
        then null
        when lower(trim(cast({{ name_column }} as string))) = 'nan'
        then null
        when regexp_like({{ name_column }}, '[0-9]')
        then null
        when length({{ name_column }}) > 60
        then null
        when length({{ name_column }}) < 3
        then null

        else
            initcap(
                trim(
                    regexp_replace(
                        regexp_replace({{ name_column }}, '\\s+', ' '),
                        "^[^\\w\\s\\-']+|([^\\w\\s\\-'\\s])$",
                        ''
                    )
                )
            )
    end
{% endmacro %}
