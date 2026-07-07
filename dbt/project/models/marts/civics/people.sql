-- Canonical person mart. One row per gp_person_id. Identifier columns are
-- scalar only when the group carries exactly one value for that source; the
-- full multi-valued sets live in person_identifiers. Attribute precedence:
-- gp_api > HubSpot > BR > TS > TS officeholder > DDHQ for contact fields,
-- BR > others for civic fields. Role flags derive from the member records,
-- not from a stored status.
-- See canonical-person-plan.md decision 5.
with
    records as (
        select
            ci.gp_person_id,
            ci.record_key,
            ci.source_name,
            substring_index(ci.record_key, '|', -1) as source_id,
            ci.first_seen_at,
            ci.group_size,
            pg.had_conflict,
            -- Per-source native identifiers, stage/race suffixes stripped so a
            -- vendor person's primary+general rows collapse to one value.
            case when ci.source_name = 'ballotready' then source_id end as br_id_val,
            case when ci.source_name = 'gp_api' then source_id end as gp_api_id_val,
            case when ci.source_name = 'hubspot' then source_id end as hs_id_val,
            case
                when ci.source_name = 'ddhq' then substring_index(source_id, '_', 1)
            end as ddhq_id_val,
            case
                when ci.source_name = 'techspeed'
                then regexp_replace(source_id, '__(primary|general|runoff)$', '')
            end as ts_code_val
        from {{ ref("int__civics_person_canonical_ids") }} as ci
        left join {{ ref("int__civics_person_groups") }} as pg using (record_key)
    ),

    -- Scalar where unambiguous: null when the group holds 0 or >1 distinct
    -- values (count(distinct) ignores nulls, so 0 -> null via max()).
    identifiers as (
        select
            gp_person_id,
            case
                when count(distinct br_id_val) = 1 then max(br_id_val)
            end as br_person_id,
            case
                when count(distinct gp_api_id_val) = 1 then max(gp_api_id_val)
            end as gp_api_user_id,
            case
                when count(distinct hs_id_val) = 1 then max(hs_id_val)
            end as hs_contact_id,
            case
                when count(distinct ddhq_id_val) = 1 then max(ddhq_id_val)
            end as ddhq_candidate_id,
            case
                when count(distinct ts_code_val) = 1 then max(ts_code_val)
            end as ts_candidate_code
        from records
        group by gp_person_id
    ),

    person_base as (
        select
            gp_person_id,
            min(first_seen_at) as first_seen_at,
            max(group_size) as group_size,
            coalesce(bool_or(had_conflict), false) as had_conflict
        from records
        group by gp_person_id
    ),

    -- Attribute reps: one representative record per (person, source), earliest
    -- first, so a source's fields stay internally consistent rather than being
    -- stitched column-wise across rows.
    gp_api_attrs as (
        select
            r.gp_person_id,
            nullif(trim(u.first_name), '') as first_name,
            nullif(trim(u.last_name), '') as last_name,
            nullif(trim(u.email), '') as email,
            nullif(trim(u.phone), '') as phone
        from records as r
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_user") }} as u
            on cast(u.id as string) = r.source_id
        where r.source_name = 'gp_api'
        qualify
            row_number() over (
                partition by r.gp_person_id
                order by r.first_seen_at asc nulls last, r.source_id
            )
            = 1
    ),

    hubspot_attrs as (
        select
            r.gp_person_id,
            nullif(trim(c.first_name), '') as first_name,
            nullif(trim(c.last_name), '') as last_name,
            nullif(trim(c.email), '') as email,
            nullif(trim(c.phone), '') as phone,
            nullif(trim(c.state), '') as state,
            nullif(trim(c.party_affiliation), '') as party
        from records as r
        inner join
            {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as c
            on cast(c.id as string) = r.source_id
        where r.source_name = 'hubspot'
        qualify
            row_number() over (
                partition by r.gp_person_id
                order by r.first_seen_at asc nulls last, r.source_id
            )
            = 1
    ),

    -- BR party is not on the identity model; pull one representative party per
    -- br_candidate_id from the candidacy feed.
    br_party as (
        select
            cast(br_candidate_id as string) as br_candidate_id,
            {{ parse_party_affiliation("parties") }} as party
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null and parties is not null
        qualify
            row_number() over (
                partition by br_candidate_id
                order by
                    coalesce(candidacy_updated_at, _airbyte_extracted_at) desc,
                    parties asc
            )
            = 1
    ),

    br_attrs as (
        select
            r.gp_person_id,
            nullif(trim(bi.id_first_name), '') as first_name,
            nullif(trim(bi.id_last_name), '') as last_name,
            nullif(trim(bi.id_email), '') as email,
            nullif(trim(bi.id_phone), '') as phone,
            nullif(trim(bi.id_state), '') as state,
            bp.party
        from records as r
        inner join
            {{ ref("int__ballotready_candidate_identity") }} as bi
            on cast(bi.br_candidate_id as string) = r.source_id
        left join br_party as bp on bp.br_candidate_id = r.source_id
        where r.source_name = 'ballotready'
        qualify
            row_number() over (
                partition by r.gp_person_id
                order by r.first_seen_at asc nulls last, r.source_id
            )
            = 1
    ),

    -- TS/DDHQ attributes ride the clustered candidacy-stage rows, keyed on
    -- unique_id == record_key (DDHQ carries no email/phone).
    clustered as (
        select unique_id, first_name, last_name, state, party, email, phone
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
    ),

    ts_attrs as (
        select
            r.gp_person_id,
            nullif(trim(cl.first_name), '') as first_name,
            nullif(trim(cl.last_name), '') as last_name,
            nullif(trim(cl.email), '') as email,
            nullif(trim(cl.phone), '') as phone,
            nullif(trim(cl.state), '') as state,
            nullif(trim(cl.party), '') as party
        from records as r
        inner join clustered as cl on cl.unique_id = r.record_key
        where r.source_name = 'techspeed'
        qualify
            row_number() over (
                partition by r.gp_person_id
                order by r.first_seen_at asc nulls last, r.source_id
            )
            = 1
    ),

    ddhq_attrs as (
        select
            r.gp_person_id,
            nullif(trim(cl.first_name), '') as first_name,
            nullif(trim(cl.last_name), '') as last_name,
            nullif(trim(cl.state), '') as state,
            nullif(trim(cl.party), '') as party
        from records as r
        inner join clustered as cl on cl.unique_id = r.record_key
        where r.source_name = 'ddhq'
        qualify
            row_number() over (
                partition by r.gp_person_id
                order by r.first_seen_at asc nulls last, r.source_id
            )
            = 1
    ),

    -- TS officeholder attributes: without this tier, officeholder-only groups
    -- (which the mart flags is_elected_official) would carry no attributes at
    -- all. Extra tie-breaks dedupe multiple staging rows per officeholder id.
    ts_officeholder_attrs as (
        select
            r.gp_person_id,
            nullif(trim(o.first_name), '') as first_name,
            nullif(trim(o.last_name), '') as last_name,
            nullif(trim(o.email), '') as email,
            nullif(trim(o.phone_clean), '') as phone,
            nullif(trim(o.state), '') as state,
            nullif(trim(o.party), '') as party
        from records as r
        inner join
            {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }} as o
            on cast(o.ts_officeholder_id as string) = r.source_id
        where r.source_name = 'techspeed_officeholder'
        qualify
            row_number() over (
                partition by r.gp_person_id
                order by
                    r.first_seen_at asc nulls last,
                    r.source_id,
                    o.date_processed_date desc nulls last,
                    o.email asc nulls last,
                    o.phone_clean asc nulls last,
                    o.last_name asc nulls last
            )
            = 1
    ),

    -- Role signals. is_candidate: any candidacy-context member (TS/DDHQ record,
    -- a BR person with a candidacy row, or a gp_api user with a campaign).
    -- is_elected_official: a techspeed_officeholder record, a BR person with an
    -- officeholder-feed row, or a gp_api user with an elected-office record.
    br_candidacy_persons as (
        select distinct cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
    ),

    br_officeholder_persons as (
        select distinct cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
    ),

    gp_campaign_users as (
        select distinct cast(user_id as string) as user_id
        from {{ ref("campaigns") }}
        where is_latest_version and user_id is not null
    ),

    gp_elected_users as (
        select distinct cast(gp_api_user_id as string) as gp_api_user_id
        from {{ ref("int__civics_elected_official_gp_api") }}
        where gp_api_user_id is not null
    ),

    roles as (
        select
            r.gp_person_id,
            bool_or(
                r.source_name in ('techspeed', 'ddhq')
                or (r.source_name = 'ballotready' and bc.br_candidate_id is not null)
                or (r.source_name = 'gp_api' and gc.user_id is not null)
            ) as is_candidate,
            bool_or(
                r.source_name = 'techspeed_officeholder'
                or (r.source_name = 'ballotready' and bo.br_candidate_id is not null)
                or (r.source_name = 'gp_api' and ge.gp_api_user_id is not null)
            ) as is_elected_official
        from records as r
        left join
            br_candidacy_persons as bc
            on r.source_name = 'ballotready'
            and bc.br_candidate_id = r.source_id
        left join
            br_officeholder_persons as bo
            on r.source_name = 'ballotready'
            and bo.br_candidate_id = r.source_id
        left join
            gp_campaign_users as gc
            on r.source_name = 'gp_api'
            and gc.user_id = r.source_id
        left join
            gp_elected_users as ge
            on r.source_name = 'gp_api'
            and ge.gp_api_user_id = r.source_id
        group by r.gp_person_id
    )

select
    pb.gp_person_id,

    -- Identifiers (scalar where unambiguous; nulled on 0 or >1 group values).
    ids.br_person_id,
    ids.gp_api_user_id,
    ids.hs_contact_id,
    ids.ddhq_candidate_id,
    ids.ts_candidate_code,

    -- Contact attributes: gp_api > HubSpot > BR > TS > TS officeholder > DDHQ.
    coalesce(
        ga.first_name,
        ha.first_name,
        ba.first_name,
        ta.first_name,
        toa.first_name,
        da.first_name
    ) as first_name,
    coalesce(
        ga.last_name,
        ha.last_name,
        ba.last_name,
        ta.last_name,
        toa.last_name,
        da.last_name
    ) as last_name,
    coalesce(ga.email, ha.email, ba.email, ta.email, toa.email) as email,
    coalesce(ga.phone, ha.phone, ba.phone, ta.phone, toa.phone) as phone,

    -- Civic attributes: BR > others.
    coalesce(ba.state, ha.state, ta.state, toa.state, da.state) as state,
    coalesce(ba.party, ha.party, ta.party, toa.party, da.party) as party,

    pb.first_seen_at,
    pb.group_size,
    pb.had_conflict,

    coalesce(roles.is_candidate, false) as is_candidate,
    coalesce(roles.is_elected_official, false) as is_elected_official
from person_base as pb
left join identifiers as ids using (gp_person_id)
left join roles using (gp_person_id)
left join gp_api_attrs as ga using (gp_person_id)
left join hubspot_attrs as ha using (gp_person_id)
left join br_attrs as ba using (gp_person_id)
left join ts_attrs as ta using (gp_person_id)
left join ts_officeholder_attrs as toa using (gp_person_id)
left join ddhq_attrs as da using (gp_person_id)
