{% macro get_icp_office_normalized_position_names() %}
    select name from {{ ref("icp_normalized_position_names") }}
{% endmacro %}
