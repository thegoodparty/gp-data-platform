-- Deterministic person edges. One row per typed edge between two record keys
-- (record_key = source_name || '|' || source_id). Edges are direction-agnostic
-- (record_key_1 <= record_key_2). No probabilistic matching: every edge
-- derives from native ids, candidacy-stage Splink cluster co-membership, the
-- elected-official bridge, or exact contact-info equality with name agreement
-- (E8/E9). See canonical-person-plan.md decision 1.
{% set contact_key_max_records = 25 %}
with
    -- br_candidacy_id -> br_candidate_id (person grain).
    candidacies as (
        select distinct
            cast(br_candidacy_id as string) as br_candidacy_id,
            cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null and br_candidate_id is not null
    ),

    users as (
        select id, hubspot_contact_id
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    contacts as (
        select
            id,
            cast(id as string) as id_string,
            goodparty_user_id,
            cast(br_candidacy_id as string) as br_candidacy_id
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    campaigns as (
        select cast(campaign_id as string) as campaign_id, user_id
        from {{ ref("campaigns") }}
        where is_latest_version and user_id is not null
    ),

    clustered as (
        select
            cluster_id,
            source_id,
            source_name,
            br_candidacy_id,
            split(source_id, '__')[0] as gp_api_campaign_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
    ),

    -- gp_api user <-> BR person via the elected-official bridge (E6 and the
    -- E7 conflict pre-filter both consume this).
    bridge as (
        select distinct
            gp_api_user_id, cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("int__civics_elected_official_gp_api_bridge") }}
        where gp_api_user_id is not null and br_candidate_id is not null
    ),

    -- E1/E2: HubSpot contact <-> gp_api user via bidirectional native ids.
    -- Both directions collapse to one pair after normalization.
    e1 as (
        select
            'hubspot|' || cast(c.id as string) as rk_a,
            'gp_api|' || cast(c.goodparty_user_id as string) as rk_b
        from contacts as c
        inner join users as u on u.id = c.goodparty_user_id
        union
        select
            'gp_api|' || cast(u.id as string),
            'hubspot|' || cast(u.hubspot_contact_id as string)
        from users as u
        inner join contacts as c on c.id_string = u.hubspot_contact_id
    ),

    -- E3: HubSpot contact br_candidacy_id -> BR candidacy -> br_candidate_id.
    e3 as (
        select
            'hubspot|' || c.id_string as rk_a,
            'ballotready|' || cand.br_candidate_id as rk_b
        from contacts as c
        inner join candidacies as cand on cand.br_candidacy_id = c.br_candidacy_id
    ),

    -- E4: ts_officeholder_id == br_office_holder_id -> br_candidate_id.
    -- Reused ts_officeholder_ids suppressed (they conflate distinct people).
    e4 as (
        select
            'techspeed_officeholder|' || cast(ts_officeholder_id as string) as rk_a,
            'ballotready|' || cast(br_candidate_id as string) as rk_b
        from {{ ref("int__civics_elected_official_canonical_ids") }}
        where not ts_officeholder_id_is_reused and br_candidate_id is not null
    ),

    -- E5: candidacy-stage cluster co-membership. Map each member to its record
    -- key, then hub every member to the cluster's min record key (avoids a
    -- pairwise cross join). BR members map via candidacy; gp_api members map
    -- campaign -> user; TS/DDHQ members are their own keys.
    cluster_members as (
        select cc.cluster_id, 'ballotready|' || cand.br_candidate_id as record_key
        from clustered as cc
        inner join candidacies as cand on cand.br_candidacy_id = cc.br_candidacy_id
        where cc.source_name = 'ballotready'
        union
        select cc.cluster_id, 'gp_api|' || cast(camp.user_id as string)
        from clustered as cc
        inner join campaigns as camp on camp.campaign_id = cc.gp_api_campaign_id
        where cc.source_name = 'gp_api'
        union
        select cluster_id, 'techspeed|' || source_id
        from clustered
        where source_name = 'techspeed'
        union
        select cluster_id, 'ddhq|' || source_id
        from clustered
        where source_name = 'ddhq'
    ),

    cluster_hub as (
        select cluster_id, min(record_key) as hub_key
        from cluster_members
        group by cluster_id
    ),

    e5 as (
        select cm.record_key as rk_a, h.hub_key as rk_b
        from cluster_members as cm
        inner join cluster_hub as h using (cluster_id)
        where cm.record_key <> h.hub_key
    ),

    -- E6: elected-official bridge. gp_api user <-> BR person.
    e6 as (
        select
            'gp_api|' || cast(gp_api_user_id as string) as rk_a,
            'ballotready|' || br_candidate_id as rk_b
        from bridge
    ),

    -- E7: within-source vendor keys. TS records sharing a stage-stripped
    -- candidate_code; DDHQ records sharing candidate_id. Guards a vendor-only
    -- person's primary/general split. DDHQ candidate_id is reused across
    -- people ~1.5% of the time, so pre-filter: if a key's records already
    -- resolve (via clusters) to >1 distinct br_candidate_id, its E7 edges are
    -- flagged is_conflict and excluded from propagation downstream.
    e7_members as (
        select
            'techspeed' as source_name,
            source_id,
            {{ strip_ts_stage_suffix("source_id") }} as e7_key
        from clustered
        where source_name = 'techspeed'
        union all
        select 'ddhq', source_id, split(source_id, '_')[0]
        from clustered
        where source_name = 'ddhq'
    ),

    -- Distinct br_candidate_ids each vendor record reaches through its
    -- cluster: directly via a BR co-member, or via a gp_api co-member that
    -- resolves to a BR person through the elected-official bridge.
    vendor_cluster_br as (
        select cc.source_name, cc.source_id, cand.br_candidate_id
        from clustered as cc
        inner join
            clustered as br
            on br.cluster_id = cc.cluster_id
            and br.source_name = 'ballotready'
        inner join candidacies as cand on cand.br_candidacy_id = br.br_candidacy_id
        where cc.source_name in ('techspeed', 'ddhq')
        union
        select cc.source_name, cc.source_id, b.br_candidate_id
        from clustered as cc
        inner join
            clustered as gp
            on gp.cluster_id = cc.cluster_id
            and gp.source_name = 'gp_api'
        inner join campaigns as camp on camp.campaign_id = gp.gp_api_campaign_id
        inner join bridge as b on b.gp_api_user_id = camp.user_id
        where cc.source_name in ('techspeed', 'ddhq')
    ),

    e7_key_stats as (
        select
            m.source_name,
            m.e7_key,
            count(distinct v.br_candidate_id) as distinct_br,
            count(distinct m.source_id) as distinct_records
        from e7_members as m
        left join
            vendor_cluster_br as v
            on v.source_name = m.source_name
            and v.source_id = m.source_id
        group by m.source_name, m.e7_key
    ),

    e7_hub as (
        select source_name, e7_key, min(source_name || '|' || source_id) as hub_key
        from e7_members
        group by source_name, e7_key
    ),

    e7 as (
        select
            m.source_name || '|' || m.source_id as rk_a,
            h.hub_key as rk_b,
            s.distinct_br > 1 as is_conflict
        from e7_members as m
        inner join e7_key_stats as s using (source_name, e7_key)
        inner join e7_hub as h using (source_name, e7_key)
        where
            s.distinct_records > 1 and m.source_name || '|' || m.source_id <> h.hub_key
    ),

    -- E8/E9: shared normalized email (E8) or phone (E9) plus name agreement.
    -- A bare contact-key match over-merges: spouses share campaign inboxes and
    -- family phones, and institutional keys (school-board inboxes, city
    -- switchboards, placeholder numbers) span dozens of people. Audited
    -- 2026-08: with the name gate below, 0 false positives in ~130 sampled
    -- pairs; without it, the excluded bucket is dominated by real
    -- two-person households.
    nickname_pairs as (
        select
            {{ first_name_normalized("name1") }} as name_a,
            {{ first_name_normalized("name2") }} as name_b
        from {{ ref("nicknames") }}
        union
        select
            {{ first_name_normalized("name2") }}, {{ first_name_normalized("name1") }}
        from {{ ref("nicknames") }}
    ),

    -- Symmetric alias classes: the seed is directional (robert -> bob), but a
    -- pair can arrive in either order, so close it both ways plus identity.
    nickname_aliases as (
        select
            name_a as name,
            array_distinct(array_append(collect_list(name_b), name_a)) as aliases
        from nickname_pairs
        group by name_a
    ),

    -- Generational suffix per BR person from the explicit suffix columns
    -- (BR last names rarely embed the suffix as text).
    br_suffixes as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            max(nullif(trim(suffix), '')) as suffix_raw
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        group by br_candidate_id
    ),

    br_officeholder_contacts as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            first_name,
            last_name,
            nullif(trim(suffix), '') as suffix_raw,
            email,
            phone
        from {{ ref("int__civics_elected_official_ballotready_person") }}
        where br_candidate_id is not null
        qualify
            row_number() over (
                partition by br_candidate_id order by term_start_date desc nulls last
            )
            = 1
    ),

    -- TechSpeed contact rows keyed to their clustered candidacy-stage record
    -- keys: recompute the prematch's candidate code and join it to the
    -- stage-stripped clustered source_id, so E8/E9 endpoints are node keys.
    ts_contact_codes as (
        select
            {{
                generate_candidate_code(
                    "first_name",
                    "last_name",
                    "state",
                    "office_type",
                    "city",
                )
            }} as candidate_code,
            first_name,
            last_name,
            email,
            coalesce(nullif(phone_clean, ''), nullif(phone, '')) as phone
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
    ),

    contact_sources as (
        select
            'hubspot|' || cast(id as string) as record_key,
            first_name,
            last_name,
            cast(null as string) as suffix_raw,
            email,
            phone
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        union all
        select
            'gp_api|' || cast(id as string),
            first_name,
            last_name,
            cast(null as string),
            email,
            phone
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
        union all
        select
            'ballotready|' || cast(i.br_candidate_id as string),
            i.id_first_name,
            i.id_last_name,
            s.suffix_raw,
            i.id_email,
            i.id_phone
        from {{ ref("int__ballotready_candidate_identity") }} as i
        left join
            br_suffixes as s on s.br_candidate_id = cast(i.br_candidate_id as string)
        union all
        select
            'ballotready|' || br_candidate_id,
            first_name,
            last_name,
            suffix_raw,
            email,
            phone
        from br_officeholder_contacts
        union all
        select
            'techspeed|' || cc.source_id,
            tc.first_name,
            tc.last_name,
            cast(null as string),
            tc.email,
            tc.phone
        from clustered as cc
        inner join
            ts_contact_codes as tc
            on tc.candidate_code = {{ strip_ts_stage_suffix("cc.source_id") }}
        where cc.source_name = 'techspeed'
        union all
        select
            'techspeed_officeholder|' || cast(ts.ts_officeholder_id as string),
            ts.first_name,
            ts.last_name,
            cast(null as string),
            ts.email,
            coalesce(nullif(ts.phone_clean, ''), nullif(ts.phone, ''))
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }} as ts
        inner join
            {{ ref("int__civics_elected_official_canonical_ids") }} as eo
            on eo.ts_officeholder_id = ts.ts_officeholder_id
            and not eo.ts_officeholder_id_is_reused
    ),

    contact_normalized as (
        select
            record_key,
            substring_index(record_key, '|', 1) as source_name,
            {{ first_name_normalized("first_name") }} as fn,
            regexp_replace(
                lower({{ remove_name_suffixes("last_name") }}), '[^a-z]', ''
            ) as ln,
            -- Suffix mismatch is a cannot-link: verified live father/son pairs
            -- (both county officials) share a family phone and differ only by
            -- Jr/Sr. '' means no suffix; both sides must agree exactly.
            coalesce(
                nullif(
                    regexp_replace(
                        upper(
                            coalesce(
                                suffix_raw,
                                regexp_extract(
                                    last_name,
                                    '(?i)(?:^|[ ,])(jr|sr|ii|iii|iv|v)\\.?\\s*$',
                                    1
                                )
                            )
                        ),
                        '[^A-Z]',
                        ''
                    ),
                    ''
                ),
                ''
            ) as suffix_token,
            lower(trim(email)) as email_lower,
            case
                when
                    email_lower like '%@%'
                    -- Internal/test addresses sit on dozens of unrelated
                    -- records (staff-created accounts).
                    and email_lower not like '%@goodparty.org'
                    and email_lower not like '%@goodparty.com'
                    and email_lower not like '%@mailinator.com'
                then
                    concat(
                        regexp_replace(split_part(email_lower, '@', 1), '\\+.*$', ''),
                        '@',
                        split_part(email_lower, '@', 2)
                    )
            end as email_key,
            right(regexp_replace(phone, '[^0-9]', ''), 10) as phone_digits,
            case
                when
                    length(regexp_replace(phone, '[^0-9]', '')) >= 10
                    and phone_digits not in (
                        '5555555555',
                        '1234567890',
                        '1111111111',
                        '0000000000',
                        '9999999999'
                    )
                then phone_digits
            end as phone_key
        from contact_sources
        where
            nullif(trim(first_name), '') is not null
            and nullif(trim(last_name), '') is not null
    ),

    contact_records as (
        select
            n.record_key,
            n.source_name,
            n.fn,
            n.ln,
            n.suffix_token,
            n.email_key,
            n.phone_key,
            coalesce(a.aliases, array(n.fn)) as fn_aliases
        from contact_normalized as n
        left join nickname_aliases as a on a.name = n.fn
        where n.fn <> '' and n.ln <> ''
    ),

    contact_keyed as (
        select distinct
            'email' as key_type,
            email_key as contact_key,
            record_key,
            source_name,
            fn,
            ln,
            suffix_token,
            fn_aliases
        from contact_records
        where email_key is not null
        union all
        select distinct
            'phone',
            phone_key,
            record_key,
            source_name,
            fn,
            ln,
            suffix_token,
            fn_aliases
        from contact_records
        where phone_key is not null
    ),

    -- Institutional inboxes and shared office lines span dozens of people;
    -- cap how many records one key may connect.
    contact_key_sizes as (
        select key_type, contact_key, count(distinct record_key) as n_records
        from contact_keyed
        group by key_type, contact_key
    ),

    -- A key whose same-last-name members include >1 BR person is ambiguous
    -- identity evidence (mirrors the E7 pre-filter): flag, don't merge.
    contact_key_br as (
        select key_type, contact_key, ln, count(distinct record_key) as n_br
        from contact_keyed
        where source_name = 'ballotready'
        group by key_type, contact_key, ln
    ),

    e8_e9 as (
        select
            a.record_key as rk_a,
            b.record_key as rk_b,
            a.key_type,
            (a.source_name = 'ballotready' and b.source_name = 'ballotready')
            or coalesce(br.n_br, 0) > 1 as is_conflict
        from contact_keyed as a
        inner join
            contact_keyed as b
            on a.key_type = b.key_type
            and a.contact_key = b.contact_key
            and a.record_key < b.record_key
        inner join
            contact_key_sizes as s
            on s.key_type = a.key_type
            and s.contact_key = a.contact_key
            and s.n_records <= {{ contact_key_max_records }}
        left join
            contact_key_br as br
            on br.key_type = a.key_type
            and br.contact_key = a.contact_key
            and br.ln = a.ln
        where
            a.ln = b.ln
            and a.suffix_token = b.suffix_token
            and (
                a.fn = b.fn
                or arrays_overlap(a.fn_aliases, b.fn_aliases)
                or (
                    substr(a.fn, 1, 1) = substr(b.fn, 1, 1)
                    and levenshtein(a.fn, b.fn) <= 2
                )
            )
    ),

    all_edges as (
        select rk_a, rk_b, 'e1_hubspot_user' as edge_type, false as is_conflict
        from e1
        union all
        select rk_a, rk_b, 'e3_hubspot_br_candidacy', false
        from e3
        union all
        select rk_a, rk_b, 'e4_ts_officeholder', false
        from e4
        union all
        select rk_a, rk_b, 'e5_cluster', false
        from e5
        union all
        select rk_a, rk_b, 'e6_eo_bridge', false
        from e6
        union all
        select rk_a, rk_b, 'e7_within_source', is_conflict
        from e7
        union all
        select
            rk_a,
            rk_b,
            case key_type when 'email' then 'e8_email_name' else 'e9_phone_name' end,
            is_conflict
        from e8_e9
    )

select
    least(rk_a, rk_b) as record_key_1,
    greatest(rk_a, rk_b) as record_key_2,
    edge_type,
    bool_or(is_conflict) as is_conflict
from all_edges
where rk_a is not null and rk_b is not null and rk_a <> rk_b
group by 1, 2, 3
