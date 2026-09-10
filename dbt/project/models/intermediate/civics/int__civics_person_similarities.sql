-- Person similarities. One row per undirected pair of distinct identities that
-- a Splink person edge at or above the merge threshold says are the same
-- person.
--
-- Not closed over. int__civics_person_groups admits a set of identities only
-- when every pair inside it appears here, which is what keeps a pairwise
-- judgement meaningful: under transitive closure, chaining routes around any
-- gate you build, which is how 6,219 groups came to fuse two BallotReady
-- people from an edge set containing no BR-to-BR pair.
{% set merge_threshold = 0.95 %}
with
    identities as (
        select record_key, identity_key from {{ ref("int__civics_person_identities") }}
    ),

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
    -- Six of fifty sampled were wrong there against none elsewhere. Belongs in
    -- the matcher's own post-prediction filter, but the published vintage does
    -- not carry it, so removing it here would regress precision until a new
    -- run is published.
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

    splink_pairs as (
        select ka.record_key as rk_a, kb.record_key as rk_b, k.match_probability
        from kept as k
        inner join key_map as ka on ka.prematch_key = k.unique_id_l
        inner join key_map as kb on kb.prematch_key = k.unique_id_r
    ),

    -- Lifted to the identity grain: two identities are similar when any record
    -- in one carries evidence against any record in the other. Sameness inside
    -- an identity is already asserted, so one member vouching for it is enough.
    lifted as (
        select
            least(ia.identity_key, ib.identity_key) as identity_key_1,
            greatest(ia.identity_key, ib.identity_key) as identity_key_2,
            p.match_probability
        from splink_pairs as p
        inner join identities as ia on ia.record_key = p.rk_a
        inner join identities as ib on ib.record_key = p.rk_b
        where ia.identity_key <> ib.identity_key
    )

select identity_key_1, identity_key_2, max(match_probability) as match_probability
from lifted
group by 1, 2
