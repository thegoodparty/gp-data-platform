{#- Columns are listed by name on both sides: a positional union would silently
    shift values if the two column orders ever drifted apart. -#}
{%- set voter_columns = adapter.get_columns_in_relation(ref("gp_api_voters")) -%}
{%- set extra_columns = ["Registered_Voter", "District_Source"] %}

select
    {%- for column in voter_columns %} `{{ column.name }}`,{% endfor %}
    true as `Registered_Voter`,
    'voter_file' as `District_Source`
from {{ ref("gp_api_voters") }}

union all

select
    {%- for column in voter_columns %} `{{ column.name }}`,{% endfor %}
    {%- for name in extra_columns %}
        `{{ name }}`{% if not loop.last %},{% endif %}
    {%- endfor %}
from {{ ref("gp_api_consumer_only_people") }}
