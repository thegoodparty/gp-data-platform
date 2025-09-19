{% macro clean_office_for_ddhq(office_column) %}
    {#
    Clean and standardize office/race name fields for DDHQ matching

    Args:
        office_column: The column name to clean (e.g., 'office_name')

    Usage:
        select
            {{ clean_office_for_ddhq('office_name') }} as cleaned_office_name
        from my_table
    #}
    case
        -- Convert to string and strip whitespace, then handle nulls and empty strings
        when {{ office_column }} is null
        then null
        when trim(cast({{ office_column }} as string)) = ''
        then null
        when lower(trim(cast({{ office_column }} as string))) in ('nan', 'none')
        then null
        when length(trim(cast({{ office_column }} as string))) > 150
        then null

        else
            -- Fix multiple spaces and standardize common office name patterns
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        trim(cast({{ office_column }} as string)),
                                        '\\s+',
                                        ' '
                                    ),
                                    '\\b(?i)board\\b',
                                    'Board'
                                ),
                                '\\b(?i)council\\b',
                                'Council'
                            ),
                            '\\b(?i)mayor\\b',
                            'Mayor'
                        ),
                        '\\b(?i)school\\b',
                        'School'
                    ),
                    '\\b(?i)district\\b',
                    'District'
                ),
                '^\\s+|\\s+$',
                ''
            )
    end
{% endmacro %}
