-- Pins the scalar-nulling contract on people: each per-source identifier
-- column is populated iff the person's group holds exactly one distinct value
-- for that source (after the same DDHQ/TS key normalization).
with
    counts as (
        select
            gp_person_id,
            count(
                distinct case when source_name = 'ballotready' then source_id end
            ) as n_br,
            count(distinct case when source_name = 'gp_api' then source_id end) as n_gp,
            count(
                distinct case when source_name = 'hubspot' then source_id end
            ) as n_hs,
            count(
                distinct case
                    when source_name = 'ddhq' then substring_index(source_id, '_', 1)
                end
            ) as n_dq,
            count(
                distinct case
                    when source_name = 'techspeed'
                    then {{ strip_ts_stage_suffix("source_id") }}
                end
            ) as n_ts
        from {{ ref("person_identifiers") }}
        group by gp_person_id
    )

select p.gp_person_id
from {{ ref("people") }} as p
inner join counts as c using (gp_person_id)
where
    (p.br_person_id is not null) != (c.n_br = 1)
    or (p.gp_api_user_id is not null) != (c.n_gp = 1)
    or (p.hs_contact_id is not null) != (c.n_hs = 1)
    or (p.ddhq_candidate_id is not null) != (c.n_dq = 1)
    or (p.ts_candidate_code is not null) != (c.n_ts = 1)
