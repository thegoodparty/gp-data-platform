{{ config(materialized="sql") }}

-- Row filter body for a table nobody has classified: false means no rows reach
-- a masked reader, so a new table lands closed and opens when it is tagged.
create or replace function {{ this }} ()
returns boolean deterministic
return false
;
