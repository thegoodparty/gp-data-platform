{{ config(tags=["feed_invariant", "sales_reverse_etl"]) }}
-- DATA-1523 / DATA-2011 cutover guard (not-already-in-HubSpot anti-join).
-- candidacy_hubspot must exclude anyone already in HubSpot by email, 10+digit phone, or
-- br_candidacy_id. This guards the NOT EXISTS anti-join in candidacy_hubspot.sql
-- against
-- int__hubspot_contacts. Zero tolerance: any row is a duplicate lead re-uploaded to
-- HubSpot.
select h.gp_candidacy_id
from {{ ref("candidacy_hubspot") }} as h
where
    exists (
        select 1
        from {{ ref("int__hubspot_contacts") }} as hs
        where
            (
                nullif(trim(h.`Email`), '') is not null
                and lower(trim(hs.email)) = lower(trim(h.`Email`))
            )
            or (
                length(regexp_replace(h.`Phone Number`, '[^0-9]', '')) >= 10
                and regexp_replace(hs.phone_number, '[^0-9]', '')
                = regexp_replace(h.`Phone Number`, '[^0-9]', '')
            )
            or (
                nullif(h.candidacy_id, '') is not null
                and hs.br_candidacy_id = try_cast(h.candidacy_id as int)
            )
    )
