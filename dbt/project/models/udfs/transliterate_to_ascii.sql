{{ config(materialized="sql") }}

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
