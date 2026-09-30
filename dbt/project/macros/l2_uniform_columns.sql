{% macro l2_uniform_columns(source_ref) %}
    {#-
        Column list for the per-state L2 uniform staging views: every source
        column, with the two full phone-number fields replaced in place by their
        sanitized versions. Other telephone columns (area code, 7-digit,
        unformatted, confidence, availability flags) are not full numbers and are
        left untouched.
    -#}
    {%- set phone_columns = [
        "VoterTelephones_LandlineFormatted",
        "VoterTelephones_CellPhoneFormatted",
    ] -%}
    {{ dbt_utils.star(from=source_ref, except=phone_columns) }},
    {%- for column in phone_columns %}
        {{ sanitize_l2_phone_number(column) }} as `{{ column }}`
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
{% endmacro %}
