{% set source_ref = source("airbyte_source", "hubspot_api_deals") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}  -- use `except` for any columns to transform individually
from {{ source_ref }}
