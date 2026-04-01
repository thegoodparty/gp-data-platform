{% macro cast_to_boolean(column_name, true_values=none, false_values=none) %}
    {#
    Converts a string column to a boolean value.

    - Matches are case-insensitive and whitespace-trimmed.
    - NULL and empty string inputs → NULL.
    - Unrecognised values → NULL (caught by accepted_values tests downstream).

    Args:
        column_name: The column expression to convert.
        true_values:  List of lowercase strings that map to TRUE.
                      Default: ['true', 'yes', '1', 'y']
        false_values: List of lowercase strings that map to FALSE.
                      Default: ['false', 'no', '0', 'n']

    Usage:
        {{ cast_to_boolean("properties_partisan_np", ['partisan'], ['nonpartisan']) }}
        {{ cast_to_boolean("is_judicial") }}
#}
    {% set tv = true_values if true_values is not none else ["true", "yes", "1", "y"] %}
    {% set fv = (
        false_values
        if false_values is not none
        else ["false", "no", "0", "n"]
    ) %}
    case
        when
            lower(trim(cast({{ column_name }} as string)))
            in ({{ tv | map("tojson") | join(", ") }})
        then true
        when
            lower(trim(cast({{ column_name }} as string)))
            in ({{ fv | map("tojson") | join(", ") }})
        then false
        when nullif(trim(cast({{ column_name }} as string)), '') is null
        then null
        else null
    end
{% endmacro %}
