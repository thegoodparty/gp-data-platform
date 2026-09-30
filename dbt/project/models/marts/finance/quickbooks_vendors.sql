-- `tax_identifier` is held back: for a 1099 sole proprietor it is an SSN, and
-- this mart inherits the catalog-level SELECT holders that
-- assert_finance_schema_access_restricted.sql allowlists as a known gap.
select
    * except (
        _airbyte_raw_id,
        _airbyte_meta,
        _airbyte_generation_id,
        airbyte_cursor,
        tax_identifier
    )
from {{ ref("stg_airbyte_source_finance__quickbooks_vendors") }}
