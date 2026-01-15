{% macro get_icp_office_normalized_position_names() %}
    {#-
    Returns a SQL IN clause list of normalized position types for ICP offices.

    This macro centralizes the list of position types that are considered
    for ICP-Office-Win and ICP-Office-Serve flags.

    Returns:
        string: SQL IN clause with quoted position type names
    -#}
    {%- set normalized_names = [
        "State Representative",
        "State Senator",
        "City Legislature",
        "County Legislature//Executive Board",
        "Township Trustee//Township Council",
        "City Executive//Mayor",
        "Township Supervisor",
        "Township Mayor",
        "County Executive Head",
        "City Legislature Chair//President of Council",
        "County Legislature/Township Supervisor (Joint)//Executive Board/Township Supervisor (Joint)",
        "County Legislative Chair (non-executive)",
        "City Board of Selectmen",
        "Town Meeting Board",
        "City Ward Moderator",
        "City Vice-Mayor//Mayor Pro Tem",
        "County Vice Mayor",
        "Township Executive",
    ] -%}

    {%- set quoted_types = [] -%}
    {%- for normalized_name in normalized_names -%}
        {%- do quoted_types.append("'" ~ normalized_name ~ "'") -%}
    {%- endfor -%}

    {{ quoted_types | join(", ") }}
{% endmacro %}
