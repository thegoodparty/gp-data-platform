select
    -- fmt: off
    * except (companies, createdAt, updatedAt),
    case
        when
            companies is null
            or trim(companies) = ''
            or trim(companies) = '[]'
        then null
        else from_json(companies, 'array<string>')
    end as companies,  -- type is array<string>
    createdAt as created_at,
    updatedAt as updated_at
    -- fmt: on
from {{ source("airbyte_source", "hubspot_api_contacts") }}
