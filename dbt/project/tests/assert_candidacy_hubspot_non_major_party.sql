{{ config(tags=["feed_invariant", "sales_reverse_etl"]) }}
-- DATA-1523 / DATA-2011 cutover guard (non-major-party).
-- candidacy_hubspot must not include major-party candidates (inherited verbatim from
-- the legacy
-- feeds). Zero tolerance.
select gp_candidacy_id
from {{ ref("candidacy_hubspot") }}
where `Party Affiliation` ilike '%democrat%' or `Party Affiliation` ilike '%republican%'
