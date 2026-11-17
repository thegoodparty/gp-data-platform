{{ config(materialized="sql") }}

-- Salted hash of an identifier, so a masked reader still joins and can still
-- look a row up. The salt lives in a table rather than here so the body can be
-- read with DESCRIBE FUNCTION without giving the salt away; bump the version in
-- the subquery to rotate, which invalidates every earlier hash at once.
--
-- Lower and trim first so the same address hashes alike across sources.
create or replace function {{ this }} (value string)
returns string deterministic
return
    sha2(
        concat(
            (
                select salt from goodparty_data_catalog.governance.pseudonym_salt
                where version = 1
            ),
            ':',
            lower(trim(value))
        ),
        256
    )
;
