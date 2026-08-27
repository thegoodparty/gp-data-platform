-- Entity Resolution prematch for the person entity: one row per person record
-- across HubSpot contacts, gp_api users, BallotReady people, TechSpeed
-- candidates (person grain), and TechSpeed officeholders. DDHQ carries no
-- person id or contact fields and attaches via candidacy clusters; voter
-- records are out of scope. All matching happens in Splink — this model only
-- normalizes fields and applies contact hygiene, so shared institutional
-- inboxes, placeholder phones, and internal accounts cannot chain unrelated
-- people through blocking.
-- pregroup_id carries the deterministic person group: matcha blocks on it and
-- injects same-group edges at p=1.0, so deterministic identity always survives
-- probabilistic matching.
{% set contact_key_max_records = 25 %}
with
    -- Nickname aliases per canonical name (same construction as the candidacy
    -- prematch) so Splink's ArrayIntersectLevel detects nickname matches.
    nickname_aliases as (
        select
            {{ first_name_normalized("name1") }} as name,
            array_distinct(
                array_append(
                    collect_list({{ first_name_normalized("name2") }}),
                    {{ first_name_normalized("name1") }}
                )
            ) as aliases
        from {{ ref("nicknames") }}
        group by {{ first_name_normalized("name1") }}
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

    det_groups as (
        select record_key, source_name, deterministic_group_key
        from {{ ref("int__civics_person_groups_deterministic") }}
    ),

    hubspot_raw as (
        select
            'hubspot' as source_name,
            cast(id as string) as source_id,
            first_name,
            last_name,
            cast(null as string) as suffix_raw,
            email,
            phone,
            state as state_raw,
            city,
            cast(null as string) as zip_raw,
            coalesce(
                try_cast(birth_date as date),
                try_to_date(replace(birth_date, '/', '-'), 'M-d-yyyy')
            ) as birth_date,
            party_affiliation as party_raw,
            cast(null as string) as br_candidate_id,
            coalesce(contact_created_at, created_at) as first_seen_at
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    gp_api_raw as (
        select
            'gp_api' as source_name,
            cast(id as string) as source_id,
            first_name,
            last_name,
            cast(null as string) as suffix_raw,
            email,
            phone,
            cast(null as string) as state_raw,
            cast(null as string) as city,
            zip as zip_raw,
            cast(null as date) as birth_date,
            cast(null as string) as party_raw,
            cast(null as string) as br_candidate_id,
            created_at as first_seen_at
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    -- Earliest creation across both BR feeds, keyed on the person grain (same
    -- construction as the person mint).
    br_created as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            candidacy_created_at as created_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        union all
        select cast(br_candidate_id as string), office_holder_created_at
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
    ),

    br_first_seen as (
        select br_candidate_id, min(created_at) as first_seen_at
        from br_created
        group by br_candidate_id
    ),

    -- Explicit suffix from the candidacy feed (BR surnames rarely embed it).
    br_suffixes as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            max(nullif(trim(suffix), '')) as suffix_raw
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        group by br_candidate_id
    ),

    -- Officeholder-only people carry contact fields the identity model lacks;
    -- already one row per br_candidate_id.
    br_officeholder as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            first_name,
            last_name,
            nullif(trim(suffix), '') as suffix_raw,
            email,
            phone,
            state,
            coalesce(city, mailing_city) as city,
            mailing_zip,
            party_affiliation
        from {{ ref("int__civics_elected_official_ballotready_person") }}
        where br_candidate_id is not null
    ),

    ballotready_raw as (
        select
            'ballotready' as source_name,
            coalesce(cast(i.br_candidate_id as string), o.br_candidate_id) as source_id,
            coalesce(i.id_first_name, o.first_name) as first_name,
            coalesce(i.id_last_name, o.last_name) as last_name,
            coalesce(o.suffix_raw, s.suffix_raw) as suffix_raw,
            coalesce(i.id_email, o.email) as email,
            coalesce(i.id_phone, o.phone) as phone,
            coalesce(i.id_state, o.state) as state_raw,
            o.city,
            o.mailing_zip as zip_raw,
            cast(null as date) as birth_date,
            coalesce(i.id_party, o.party_affiliation) as party_raw,
            coalesce(
                cast(i.br_candidate_id as string), o.br_candidate_id
            ) as br_candidate_id,
            f.first_seen_at
        from {{ ref("int__ballotready_candidate_identity") }} as i
        full outer join
            br_officeholder as o
            on o.br_candidate_id = cast(i.br_candidate_id as string)
        left join
            br_suffixes as s
            on s.br_candidate_id
            = coalesce(cast(i.br_candidate_id as string), o.br_candidate_id)
        left join
            br_first_seen as f
            on f.br_candidate_id
            = coalesce(cast(i.br_candidate_id as string), o.br_candidate_id)
    ),

    -- TechSpeed candidates at person grain: the candidate code without the
    -- stage suffix. One representative delivery row per code; first_seen_at is
    -- the min over all delivery rows (mirrors the candidacy prematch).
    ts_coded as (
        select
            *,
            {{
                generate_candidate_code(
                    "first_name",
                    "last_name",
                    "state",
                    "office_type",
                    "city",
                )
            }} as candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
    ),

    techspeed_raw as (
        select
            'techspeed' as source_name,
            candidate_code as source_id,
            first_name,
            last_name,
            name_suffix as suffix_raw,
            email,
            coalesce(nullif(phone_clean, ''), nullif(phone, '')) as phone,
            state_postal_code as state_raw,
            city,
            postal_code as zip_raw,
            birth_date_parsed as birth_date,
            party as party_raw,
            cast(null as string) as br_candidate_id,
            min(
                coalesce(cast(date_processed_date as timestamp), _airbyte_extracted_at)
            ) over (partition by candidate_code) as first_seen_at
        from ts_coded
        where candidate_code is not null
        qualify
            row_number() over (
                partition by candidate_code
                order by date_processed_date asc nulls last, _ab_source_file_url asc
            )
            = 1
    ),

    ts_officeholder_raw as (
        select
            'techspeed_officeholder' as source_name,
            cast(ts.ts_officeholder_id as string) as source_id,
            ts.first_name,
            ts.last_name,
            cast(null as string) as suffix_raw,
            ts.email,
            coalesce(nullif(ts.phone_clean, ''), nullif(ts.phone, '')) as phone,
            ts.state as state_raw,
            ts.city,
            ts.postal_code as zip_raw,
            cast(null as date) as birth_date,
            ts.party as party_raw,
            cast(null as string) as br_candidate_id,
            min(
                coalesce(
                    cast(ts.date_processed_date as timestamp), ts._airbyte_extracted_at
                )
            ) over (partition by ts.ts_officeholder_id) as first_seen_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }} as ts
        inner join
            {{ ref("int__civics_elected_official_canonical_ids") }} as eo
            on eo.ts_officeholder_id = ts.ts_officeholder_id
            and not eo.ts_officeholder_id_is_reused
        qualify
            row_number() over (
                partition by ts.ts_officeholder_id
                order by ts.date_processed_date asc nulls last, ts._airbyte_raw_id asc
            )
            = 1
    ),

    unioned as (
        select *
        from hubspot_raw
        union all
        select *
        from gp_api_raw
        union all
        select *
        from ballotready_raw
        union all
        select *
        from techspeed_raw
        union all
        select *
        from ts_officeholder_raw
    ),

    named as (
        select *
        from unioned
        where
            nullif(trim(first_name), '') is not null
            and nullif(trim(last_name), '') is not null
    ),

    normalized as (
        select
            u.source_name,
            u.source_id,
            u.source_name || '|' || u.source_id as unique_id,
            {{ first_name_normalized("u.first_name") }} as first_name,
            {{ first_name_tokens("u.first_name") }} as first_name_tokens,
            -- Comma-attached suffixes leave a trailing comma behind
            -- ("Bartels, Jr." -> "Bartels,"); strip it so the surname agrees
            -- across sources (same rule as TechSpeed staging).
            trim(
                regexp_replace(
                    lower({{ remove_name_suffixes("trim(u.last_name)") }}), ',$', ''
                )
            ) as last_name,
            -- '' means no suffix; a mismatch is a cannot-link downstream
            -- (father/son share family phones and differ only by Jr/Sr).
            coalesce(
                nullif(
                    regexp_replace(
                        upper(
                            coalesce(
                                u.suffix_raw,
                                regexp_extract(
                                    u.last_name,
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
            lower(trim(u.email)) as email_lower,
            case
                when
                    email_lower like '%@%'
                    -- Internal/test addresses sit on dozens of unrelated
                    -- records (staff-created accounts) and would chain them.
                    and email_lower not like '%@goodparty.org'
                    and email_lower not like '%@goodparty.com'
                    and email_lower not like '%@mailinator.com'
                then
                    concat(
                        regexp_replace(split_part(email_lower, '@', 1), '\\+.*$', ''),
                        '@',
                        split_part(email_lower, '@', 2)
                    )
            end as email,
            regexp_replace(u.phone, '[^0-9]', '') as phone_digits,
            -- Exactly 10 digits, or 11 with a country code. Longer strings
            -- carry extensions (shared switchboards), whose trailing 10 digits
            -- are not the line's number.
            case
                when
                    (
                        length(phone_digits) = 10
                        or (length(phone_digits) = 11 and phone_digits like '1%')
                    )
                    and right(phone_digits, 10) not in (
                        '5555555555',
                        '1234567890',
                        '1111111111',
                        '0000000000',
                        '9999999999'
                    )
                then right(phone_digits, 10)
            end as phone,
            cs.state_cleaned_postal_code as state,
            nullif(lower(trim(u.city)), '') as city,
            case
                when length(regexp_replace(u.zip_raw, '[^0-9]', '')) >= 5
                then left(regexp_replace(u.zip_raw, '[^0-9]', ''), 5)
            end as zip5,
            u.birth_date,
            {{ parse_party_affiliation("u.party_raw") }} as party,
            u.br_candidate_id,
            u.first_seen_at
        from named as u
        left join clean_states as cs on trim(upper(u.state_raw)) = cs.state_raw
    ),

    -- Shared institutional keys (school-board inboxes, city switchboards) span
    -- dozens of people; blocking on them would score every pair they touch.
    -- Null any contact key above the cap.
    email_counts as (
        select email, count(*) as n_records
        from normalized
        where email is not null
        group by email
    ),

    phone_counts as (
        select phone, count(*) as n_records
        from normalized
        where phone is not null
        group by phone
    ),

    -- TechSpeed person pregroup: min deterministic group across the code's
    -- candidacy-stage record keys. A code whose stage keys span >1 group is
    -- already conflict-implicated (its E7 edges resolve to >1 BR person) and
    -- is excluded; its records still attach to people via candidacy clusters.
    ts_pregroups as (
        select
            {{ strip_ts_stage_suffix("substring_index(record_key, '|', -1)") }}
            as candidate_code,
            min(deterministic_group_key) as deterministic_group_key,
            count(distinct deterministic_group_key) as n_groups
        from det_groups
        where source_name = 'techspeed'
        group by 1
    )

select
    n.unique_id,
    n.source_id,
    n.source_name,
    n.first_name,
    coalesce(a.aliases, array(n.first_name)) as first_name_aliases,
    n.first_name_tokens,
    n.last_name,
    n.suffix_token,
    case when ec.n_records <= {{ contact_key_max_records }} then n.email end as email,
    case when pc.n_records <= {{ contact_key_max_records }} then n.phone end as phone,
    n.state,
    n.city,
    n.zip5,
    n.birth_date,
    n.party,
    n.br_candidate_id,
    n.first_seen_at,
    coalesce(
        tsg.deterministic_group_key, dg.deterministic_group_key, n.unique_id
    ) as pregroup_id
from normalized as n
left join nickname_aliases as a on a.name = n.first_name
left join email_counts as ec on ec.email = n.email
left join phone_counts as pc on pc.phone = n.phone
left join
    det_groups as dg on dg.record_key = n.unique_id and n.source_name <> 'techspeed'
left join
    ts_pregroups as tsg
    on tsg.candidate_code = n.source_id
    and n.source_name = 'techspeed'
where
    -- Names that normalize to empty (punctuation-only) cannot be matched.
    n.first_name <> ''
    and n.last_name <> ''
    and (n.source_name <> 'techspeed' or coalesce(tsg.n_groups, 1) = 1)
