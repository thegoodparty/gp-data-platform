/*
    The columns in this data stream change frequently, so we use a macro to retrieve instead instead of hardcoding them. Alternatively we can pick up only the needed columns
    To avoid breaking changes, we use the `except` keyword to select all columns except the ones that are transformed individually.
*/
{% set source_ref = source("airbyte_source", "hubspot_api_owners") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}  -- use `except` for any columns to transform individually
from {{ source_ref }}
