-- The finance tables are meant to be reachable only through mart_finance, whose
-- read access is scoped to a readers group. Nothing structural puts them there:
-- it is the marts.finance config, and dropping it lands them in the default dbt
-- schema that every data user reads, with no other test failing. This checks
-- where they actually are. Personal dev schemas are out of scope.
select table_schema, table_name
from goodparty_data_catalog.information_schema.tables
where
    (startswith(table_name, 'quickbooks_') or startswith(table_name, 'stripe_'))
    and (
        table_schema = 'dbt'
        or (startswith(table_schema, 'mart_') and table_schema != 'mart_finance')
    )
