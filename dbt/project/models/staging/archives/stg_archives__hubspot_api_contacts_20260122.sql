select
    -- fmt: off
    * except (createdAt, updatedAt),
    createdAt as created_at,
    updatedAt as updated_at
    -- fmt: on
from {{ source("archives", "airbyte_source__hubspot_api_contacts_20260122") }}
