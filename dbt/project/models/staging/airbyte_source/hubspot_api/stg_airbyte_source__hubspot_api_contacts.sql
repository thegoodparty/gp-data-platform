select
    -- fmt: off
    * except (createdAt, updatedAt),
    createdAt as created_at,
    updatedAt as updated_at
    -- fmt: on
from {{ source("airbyte_source", "hubspot_api_contacts") }}
