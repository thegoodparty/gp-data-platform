{% macro parse_party_affiliation(column) %}
    case
        when nullif(trim({{ column }}), '') is null
        then null
        when {{ column }} ilike '%independent%'
        then 'Independent'
        when {{ column }} ilike '%nonpartisan%'
        then 'Nonpartisan'
        when {{ column }} ilike '%democrat%'
        then 'Democrat'
        when {{ column }} ilike '%republican%'
        then 'Republican'
        when {{ column }} ilike '%libertarian%'
        then 'Libertarian'
        when {{ column }} ilike '%green%'
        then 'Green'
        else 'Other'
    end
{% endmacro %}
