-- properties_win_number must survive the HubSpot companies archive.
--
-- The archive is a select * over snapshot__hubspot_api_companies, so it carries
-- whatever columns the snapshot happens to have. The HubSpot companies stream
-- narrows its raw columns over time, and a snapshot built after
-- properties_win_number stopped being one of them has no such column, which
-- silently removed it from the archive and broke every model that reads
-- win_number from it. The archive now rebuilds the column from the properties
-- JSON in exactly that case.
--
-- So this asserts the recovery, not a population level: when the snapshot lacks
-- the column, the archive must still return values for it. When the snapshot
-- does have the column the archive carries it through and there is nothing to
-- check here -- prod's snapshot column stopped being written in early 2026 and is
-- nearly empty for reasons that predate this, so asserting a population there
-- would fail on data this model does not own.
{% set snapshot_columns = adapter.get_columns_in_relation(
    ref("snapshot__hubspot_api_companies")
) %}
{% set snapshot_column_names = (
    snapshot_columns | map(attribute="name") | map("lower") | list
) %}
{% if "properties_win_number" not in snapshot_column_names %}

    select count(properties_win_number) as populated
    from {{ ref("int__hubspot_companies_archive_2025") }}
    -- The raw stream still delivers this property, so a rebuild from it has to
    -- produce rows. The raw table is the reference rather than a constant because
    -- the property is only populated for companies that have a target: a genuine
    -- zero here would mean the stream changed shape again.
    having
        count(properties_win_number) < (
            select count(get_json_object(properties, '$.win_number')) * 0.5
            from {{ source("airbyte_source", "hubspot_api_companies") }}
        )

{% else %} select null as populated where false

{% endif %}
