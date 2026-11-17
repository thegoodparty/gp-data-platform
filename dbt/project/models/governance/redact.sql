{{ config(materialized="sql") }}

-- The confidential mask: a null of the same type, so a masked reader sees an
-- empty value rather than an error on any date, number or string column.
create or replace function {{ this }} (value string)
returns string deterministic
return cast(null as string)
;
