-- Every logged engagement except the call properties, which have their own
-- source. The same call ids appear here without the disposition, so consumers
-- that need calls read the calls stream and exclude type CALL from this one.
select
    cast(id as string) as id,
    type as engagement_type,
    active as is_active,
    gdprdeleted as is_gdpr_deleted,
    -- When the activity happened, as against when HubSpot recorded it.
    timestamp_millis(`timestamp`) as occurred_at,
    timestamp_millis(createdat) as created_at,
    timestamp_millis(lastupdated) as updated_at,
    cast(ownerid as string) as hubspot_owner_id,
    associations_contactids as contact_ids,
    associations_companyids as company_ids,
    associations_dealids as deal_ids,
    metadata_subject as subject,
    metadata_status as status,
    _airbyte_extracted_at
from {{ source("airbyte_source", "hubspot_api_engagements") }}
