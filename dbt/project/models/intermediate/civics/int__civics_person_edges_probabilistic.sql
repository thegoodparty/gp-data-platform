-- Probabilistic person edges from the Splink person matcher, kept in their own
-- model so the deterministic pregroups can never read them. The pregroup source
-- (int__civics_person_groups_deterministic) sits upstream of the matcher, so by
-- DAG topology a published probabilistic merge cannot feed the next run's
-- pregroups. That separation is the whole reason this is not folded into
-- int__civics_person_edges.
{% set merge_threshold = 0.95 %}
with
    scored as (
        select
            unique_id_l,
            unique_id_r,
            match_probability,
            first_name_l,
            first_name_r,
            email_l,
            email_r,
            phone_l,
            phone_r
        from {{ ref("stg_er_source__pairwise_people") }}
        where match_probability >= {{ merge_threshold }}
    ),

    -- Precision gate. A pair carrying no shared contact key, whose first names
    -- agree only because the nickname alias arrays intersect, is the measured
    -- false-positive class: antonio/antoinette, dennis/denise, nancy/hannah.
    -- Six of fifty sampled were wrong there against none elsewhere, and a
    -- wrong person merge is destructive downstream, so the class is dropped
    -- rather than scored. Abbreviations are kept, since one name containing
    -- the other (ben/benjamin) was correct in every pair read.
    kept as (
        select unique_id_l, unique_id_r, match_probability
        from scored
        where
            (email_l is not null and email_l = email_r)
            or (phone_l is not null and phone_l = phone_r)
            or first_name_l = first_name_r
            or contains(first_name_l, first_name_r)
            or contains(first_name_r, first_name_l)
    ),

    -- The prematch keys techspeed on the person-grain candidate code while the
    -- person graph keys it per candidacy stage, so one prematch key fans out to
    -- every stage record key it covers. Every other source is 1:1.
    key_map as (
        select
            record_key,
            case
                when source_name = 'techspeed'
                then {{ strip_ts_stage_suffix("record_key") }}
                else record_key
            end as prematch_key
        from {{ ref("int__civics_person_nodes") }}
    ),

    expanded as (
        select ka.record_key as rk_a, kb.record_key as rk_b, k.match_probability
        from kept as k
        inner join key_map as ka on ka.prematch_key = k.unique_id_l
        inner join key_map as kb on kb.prematch_key = k.unique_id_r
    )

select
    least(rk_a, rk_b) as record_key_1,
    greatest(rk_a, rk_b) as record_key_2,
    'e10_splink_person' as edge_type,
    max(match_probability) as match_probability
from expanded
where rk_a <> rk_b
group by 1, 2, 3
