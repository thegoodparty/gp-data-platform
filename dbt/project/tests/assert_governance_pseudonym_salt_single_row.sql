-- The identifier mask reads a single salt row for a fixed version, so a
-- missing, duplicated or null-salt row for that version has to fail loudly.
-- Without the salt every pseudonym is NULL, which would null every masked
-- identifier with no error anywhere.
--
-- Anchored on the version the function reads rather than grouped over whatever
-- rows exist: grouping alone passes vacuously on an empty table, which is the
-- same silent failure in a different shape.
--
-- Counts only: the salt value is never selected.
with
    expected as (select 1 as version),

    salt as (select version, salt from {{ source("governance", "pseudonym_salt") }}),

    checked as (
        select
            expected.version,
            count(salt.version) as rows_for_version,
            count(salt.salt) as non_null_salts
        from expected
        left join salt on salt.version = expected.version
        group by expected.version
    )

select version, rows_for_version, non_null_salts
from checked
where rows_for_version <> 1 or non_null_salts <> 1
