{{
    config(
        severity="warn",
        warn_if=">0",
        error_if=">20",
        tags=["feed_invariant", "sales_reverse_etl"],
    )
}}
-- DATA-1523 / DATA-2011 guard (usable contact).
-- Every candidacy_hubspot row should be reachable: a real email OR a 10+digit phone.
-- The feed's
-- eligibility currently accepts ANY non-empty phone (the >=10-digit check only gates
-- the anti-join),
-- so a sub-10-digit phone with no email is an unusable lead. Known small edge today;
-- warn on any,
-- error on a material regression. Tightening feed eligibility would let this go to
-- strict error.
select gp_candidacy_id
from {{ ref("candidacy_hubspot") }}
where
    nullif(trim(`Email`), '') is null
    and (
        nullif(regexp_replace(`Phone Number`, '[^0-9]', ''), '') is null
        or length(regexp_replace(`Phone Number`, '[^0-9]', '')) < 10
    )
