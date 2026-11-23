-- Pins the scalar-nulling contract on people: each per-source identifier
-- column is populated iff the person's group holds exactly one distinct value
-- for that source (for HubSpot, one contact not merged away; for gp_api, one
-- user not attached to a BallotReady person on a similarity score alone).
with
    counts as (
        select
            pi.gp_person_id,
            count(
                distinct case when pi.source_name = 'ballotready' then pi.source_id end
            ) as n_br,
            count(
                distinct case
                    when pi.source_name = 'gp_api' and not ci.is_score_only_merge
                    then pi.source_id
                end
            ) as n_gp,
            count(
                distinct case
                    when pi.source_name = 'hubspot' and m.merged_contact_id is null
                    then pi.source_id
                end
            ) as n_hs,
            count(
                distinct case when pi.source_name = 'techspeed' then pi.source_id end
            ) as n_ts
        from {{ ref("person_identifiers") }} as pi
        inner join
            {{ ref("int__civics_person_canonical_ids") }} as ci
            on ci.record_key = pi.record_key
        left join
            {{ ref("int__hubspot_contact_merges") }} as m
            on pi.record_key = 'hubspot|' || m.merged_contact_id
        group by pi.gp_person_id
    )

select p.gp_person_id
from {{ ref("people") }} as p
inner join counts as c using (gp_person_id)
where
    (p.br_person_id is not null) != (c.n_br = 1)
    or (p.gp_api_user_id is not null) != (c.n_gp = 1)
    or (p.hs_contact_id is not null) != (c.n_hs = 1)
    or (p.ts_candidate_code is not null) != (c.n_ts = 1)
