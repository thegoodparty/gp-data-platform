-- properties_win_number must be populated on the HubSpot companies archive.
--
-- The archive is a select * over snapshot__hubspot_api_companies, so it carries
-- whatever columns the snapshot happens to have, and it now rebuilds
-- properties_win_number from the properties JSON. That matters on both shapes of
-- snapshot: one built after the HubSpot companies stream stopped narrowing the
-- property has no column at all, and one built before it has the column but
-- leaves it NULL on every version written since. A dbt snapshot adds columns and
-- never drops them, so prod is the second shape and would otherwise keep a
-- nearly-empty column while the value sat in the JSON. Asserting on the archive
-- rather than on the snapshot's column list is what makes that visible.
--
-- The reference is the raw stream's own population of the same property, read
-- from the same catalog, so this fails on a lost column rather than on a
-- legitimately sparse property. The archive holds at least one row per raw row,
-- so half is a wide margin: a lost column reads zero against thousands.
with
    archive as (

        select count(properties_win_number) as populated
        from {{ ref("int__hubspot_companies_archive_2025") }}

    ),

    raw_stream as (

        select count(get_json_object(properties, '$.win_number')) as populated
        from {{ source("airbyte_source", "hubspot_api_companies") }}

    )

select archive.populated as archive_populated, raw_stream.populated as raw_populated
from archive
cross join raw_stream
where archive.populated < raw_stream.populated * 0.5
