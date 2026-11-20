-- properties_win_number must survive the HubSpot companies archive.
--
-- The archive is a select * over snapshot__hubspot_api_companies, so it carries
-- whatever columns the snapshot happens to have. The HubSpot companies stream
-- narrows its raw columns over time, and a snapshot built after
-- properties_win_number stopped being one of them had no such column, which
-- silently removed it from the archive and broke every model that reads
-- win_number from it. The archive now rebuilds the column from the properties
-- JSON when the snapshot lacks it.
--
-- The test asserts the column is populated, not merely present: a snapshot that
-- has the column but no values would pass a presence check and still leave the
-- downstream assessments null. The reference count is the raw stream's own
-- population of the same property, read from the same catalog, so this fails on
-- a lost column and not on a legitimately sparse property.
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
-- The archive keeps one row per company per election-date version, so it holds
-- at least as many populated values as the raw table has rows. Allow a small
-- shortfall for a version that predates the property without being a lost
-- column; a column that is gone reads zero against thousands.
where archive.populated < raw_stream.populated * 0.5
