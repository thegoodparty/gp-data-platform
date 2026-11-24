-- The finance tables are meant to be reachable only through mart_finance, whose
-- read access is scoped to a readers group. Nothing structural puts them there:
-- it is the marts.finance config, and dropping it lands them in the default dbt
-- schema that every data user reads, with no other test failing. This checks
-- where they actually are. Personal dev schemas are out of scope.
--
-- The ledger pass-throughs share a prefix; the AI spend marts do not, so they
-- are listed by name and a new finance mart belongs in this list.
select tables.table_schema, tables.table_name
from goodparty_data_catalog.information_schema.tables as tables
where
    (
        startswith(tables.table_name, 'quickbooks_')
        or startswith(tables.table_name, 'stripe_')
        or tables.table_name
        in ('bifrost_daily_usage', 'anthropic_daily_usage', 'ai_usage_daily')
    )
    and (
        tables.table_schema = 'dbt'
        or (
            startswith(tables.table_schema, 'mart_')
            and tables.table_schema != 'mart_finance'
        )
    )
