-- The user journey table is read only: the scheduled build is its one writer
-- and record edits belong to product admin. Unity Catalog has no DENY and
-- inherits catalog and schema grants into every table, so a write grant added
-- anywhere above this table silently reaches it. This fails when any principal
-- outside the allowlist can modify it, by grant at any level or by ownership.
--
-- Reads the production relation by name, so it checks the table people query
-- rather than a CI copy. Service principals appear as UUIDs with no name
-- lookup in SQL; changing one is meant to break this test.
with
    allowed as (
        select
            explode(
                array(
                    'data-engineers',
                    'ed920c40-3b7d-42cb-ad32-0b4d5484c117',  -- dbt_cloud, the builder
                    '58aa66d2-1d9d-4b02-bda8-b001a8e6689e'  -- owns mart_analytics
                )
            ) as principal
    ),

    write_grants as (
        select 'catalog' as granted_on, grantee as principal, privilege_type
        from goodparty_data_catalog.information_schema.catalog_privileges
        where catalog_name = 'goodparty_data_catalog'

        union all

        select 'schema', grantee, privilege_type
        from goodparty_data_catalog.information_schema.schema_privileges
        where schema_name = 'mart_analytics'

        union all

        select 'table', grantee, privilege_type
        from goodparty_data_catalog.information_schema.table_privileges
        where table_schema = 'mart_analytics' and table_name = 'user_journey'
    ),

    owners as (
        select 'schema owner' as granted_on, schema_owner as principal
        from goodparty_data_catalog.information_schema.schemata
        where schema_name = 'mart_analytics'

        union all

        select 'table owner', table_owner
        from goodparty_data_catalog.information_schema.tables
        where table_schema = 'mart_analytics' and table_name = 'user_journey'
    ),

    writers as (
        select granted_on, principal
        from write_grants
        where privilege_type in ('MODIFY', 'ALL_PRIVILEGES')

        union all

        select granted_on, principal
        from owners
    )

select granted_on, principal
from writers
where principal not in (select principal from allowed where principal is not null)
