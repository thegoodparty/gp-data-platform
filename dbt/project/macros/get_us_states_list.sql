{% macro get_us_states_list(include_DC=true, include_US=false) %}
    {#-
    Returns a list of US state postal codes for use in dbt tests.

    Args:
        include_US (boolean): Whether to include 'US' in the list (default: false)
        include_DC (boolean): Whether to include 'DC' in the list (default: true)

    Returns:
        list: List of US state postal codes
    -#}
    {%- set base_states = [
        "AK",
        "AL",
        "AR",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "KY",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    ] -%}

    {%- set states_list = base_states.copy() -%}

    {%- if include_DC -%} {%- do states_list.append("DC") -%} {%- endif -%}

    {%- if include_US -%} {%- do states_list.append("US") -%} {%- endif -%}

    {{ return(states_list) }}
{% endmacro %}
