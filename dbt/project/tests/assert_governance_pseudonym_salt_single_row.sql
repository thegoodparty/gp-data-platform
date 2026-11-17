-- The identifier mask reads a single salt row, so more than one row for a
-- version (or none) has to fail loudly. Without the salt every pseudonym is
-- NULL, which would null every masked identifier with no error anywhere.
--
-- Counts only: the salt value is never selected.
select version, count(*) as rows_for_version, count(salt) as non_null_salts
from {{ source("governance", "pseudonym_salt") }}
group by version
having count(*) <> 1 or count(salt) <> 1
