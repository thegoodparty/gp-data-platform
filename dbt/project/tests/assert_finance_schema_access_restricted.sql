-- The finance schemas hold the general ledger plus vendor and customer contact
-- PII, and are meant to be readable only by data engineering and business and
-- finance ops. Unity Catalog inherits catalog-level grants into every schema and
-- has no DENY, so nothing structurally keeps a new principal out: a grant added
-- to the catalog for an unrelated reason silently reaches finance too. This
-- fails when the set of principals that can reach these schemas changes.
--
-- Named grantees are allowlisted individually. Service principals appear as
-- UUIDs with no way to resolve a name in SQL, so they are allowlisted by id;
-- rotating one is meant to break this test, because that is a deliberate act.
with
    allowed_names as (
        select
            explode(
                array(
                    'data-engineers',  -- data engineering
                    'mart_finance_readers'  -- business and finance ops
                )
            ) as grantee
    ),

    allowed_principals as (
        select
            explode(
                array(
                    -- The six holders of catalog-level SELECT, which inherits here.
                    -- Tracked as a known gap; the fix is a separate finance catalog.
                    '0f2fe566-8f33-4395-87dc-2b0c5063a7eb',  -- airflow
                    '24f016e2-6569-4b63-a2bf-9e7d34f7e7f1',
                    '55a382da-4185-4d9c-bf59-c3283c6ef319',
                    'a04e9e24-aac3-4308-9a65-d88fa34e444f',  -- github_action, owns mart_finance
                    'a2538681-3f1d-49d4-a0bc-6c7b55c54674',
                    'ed920c40-3b7d-42cb-ad32-0b4d5484c117',  -- dbt_cloud
                    -- Airbyte owns airbyte_source_finance and writes it.
                    'd64bbff3-a0fb-42b8-bc0b-b0155274a61f'
                )
            ) as grantee
    ),

    granted as (
        select schema_name, grantee, privilege_type
        from goodparty_data_catalog.information_schema.schema_privileges
        where schema_name in ('airbyte_source_finance', 'mart_finance')

        union all

        select table_schema as schema_name, grantee, privilege_type
        from goodparty_data_catalog.information_schema.table_privileges
        where table_schema in ('airbyte_source_finance', 'mart_finance')
    )

select schema_name, grantee, collect_set(privilege_type) as privileges
from granted
where
    grantee not in (select grantee from allowed_names)
    and grantee not in (select grantee from allowed_principals)
group by schema_name, grantee
