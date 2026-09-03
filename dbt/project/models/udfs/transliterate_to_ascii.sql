{{ config(materialized="sql") }}

-- Databricks caps a query at 5 Python UDF references, so a model cannot call
-- this more than five times; apply it once over a column rather than per case.
create or replace function {{ this }} (value string)
returns string
language
    python environment(dependencies = '["Unidecode"]', environment_version = 'None')
as $$
if not value:
    return value
from unidecode import unidecode
return unidecode(value)
$$
