{% macro clean_party_affiliation_for_ddhq(party_column) %}
    {#
    Clean and standardize party affiliation fields for DDHQ matching

    Args:
        party_column: The column name to clean (e.g., 'party_affiliation')

    Usage:
        select
            {{ clean_party_affiliation_for_ddhq('party_affiliation') }} as cleaned_party_affiliation
        from my_table
    #}
    case
        -- Convert to string and strip whitespace, then handle nulls and empty strings
        when {{ party_column }} is null
        then null
        when trim(cast({{ party_column }} as string)) = ''
        then null
        when lower(trim(cast({{ party_column }} as string))) in ('nan', 'none')
        then null

        else
            -- Apply party name standardizations
            case
                when
                    lower(trim(cast({{ party_column }} as string)))
                    in ('nonpartisan', 'non partisan', 'nonparisan', 'nonpartisian')
                then 'Nonpartisan'
                when lower(trim(cast({{ party_column }} as string))) = 'independent'
                then 'Independent'
                when trim(cast({{ party_column }} as string)) = 'Libertarian Party'
                then 'Libertarian'
                else trim(cast({{ party_column }} as string))
            end
    end
{% endmacro %}
