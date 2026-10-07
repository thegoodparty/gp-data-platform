{#- Columns are listed by name on both sides: a positional union would silently
    shift values if the two column orders ever drifted apart. -#}
{%- set voter_columns = adapter.get_columns_in_relation(ref("gp_api_voters")) -%}
{%- set voter_names = voter_columns | map(attribute="name") | map("lower") | list -%}
{%- set flag_names = ["registered_voter", "district_source"] -%}
{#- The commercial file's own columns, filled only on consumer-only rows. -#}
{%- set commercial_columns = [] -%}
{%- for column in adapter.get_columns_in_relation(
    ref("gp_api_consumer_only_people")
) -%}
    {%- if column.name | lower not in voter_names + flag_names -%}
        {%- do commercial_columns.append(column) -%}
    {%- endif -%}
{%- endfor %}

select
    {%- for column in voter_columns %} `{{ column.name }}`,{% endfor %}
    true as `Registered_Voter`,
    'voter_file' as `District_Source`,
    {%- for column in commercial_columns %}
        cast(null as {{ column.dtype }}) as `{{ column.name }}`
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ ref("gp_api_voters") }}

union all

select
    {%- for column in voter_columns %} `{{ column.name }}`,{% endfor %}
    `Registered_Voter`,
    `District_Source`,
    {%- for column in commercial_columns %}
        `{{ column.name }}`{% if not loop.last %},{% endif %}
    {%- endfor %}
from {{ ref("gp_api_consumer_only_people") }}
