{% set source_ref = source("dbt_source", "l2_s3_ri_uniform") %}
{% set phone_columns = [
    "VoterTelephones_CellPhoneFormatted",
    "VoterTelephones_LandlineFormatted",
] %}

select
    {{ dbt_utils.star(from=source_ref, except=phone_columns) }},
    {% for phone_column in phone_columns %}
        {{ clean_l2_phone_number(phone_column) }} as {{ phone_column }}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
from {{ source_ref }}
