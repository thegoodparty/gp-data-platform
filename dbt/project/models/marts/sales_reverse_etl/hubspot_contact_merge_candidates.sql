-- HubSpot contacts the person graph says are the same person, ranked so each person
-- has one primary to merge the others into. One row per live contact in a person
-- holding two or more. A merge keeps the primary's value for every property it has
-- set, so the ranking puts first the contact whose values should win; the
-- conflicting_properties column lists what each secondary would lose.
with
    contacts as (select * from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}),

    -- A contact merged away in HubSpot keeps its last extract in the source, and
    -- the survivor names it in merged_object_ids. There is nothing left to merge.
    merged_away as (
        select distinct merged_id
        from contacts
        lateral view explode(split(merged_object_ids, ';')) t as merged_id
        where merged_object_ids is not null and merged_id != cast(id as string)
    ),

    person_contacts as (
        select
            ci.gp_person_id,
            substring_index(ci.record_key, '|', -1) as hs_contact_id,
            ci.identity_key
        from {{ ref("int__civics_person_canonical_ids") }} as ci
        -- Before the count: staging applies DSAR suppression on every read, so a
        -- contact suppressed since the person graph last built is absent here.
        inner join
            contacts as c
            on cast(c.id as string) = substring_index(ci.record_key, '|', -1)
        left join
            merged_away as ma on ma.merged_id = substring_index(ci.record_key, '|', -1)
        -- Staff and test accounts are left alone.
        left join
            {{ ref("int__civics_internal_persons") }} as ip
            on ip.gp_person_id = ci.gp_person_id
        where
            ci.source_name = 'hubspot'
            and ma.merged_id is null
            and ip.gp_person_id is null
        qualify count(*) over (partition by ci.gp_person_id) > 1
    ),

    -- The app writes to the contact a user's meta_data points at, and that
    -- contact's email is the login email other syncs match on.
    app_links as (
        select
            u.hubspot_contact_id,
            bool_or(
                coalesce(mu.pro_campaign_count, 0) > 0
            ) as app_user_has_pro_campaign,
            -- Not users.updated_at, which a backfill stamped on most rows at once.
            max(
                coalesce(mu.last_campaign_created_at, mu.created_at)
            ) as app_user_last_active_at
        from {{ ref("stg_airbyte_source__gp_api_db_user") }} as u
        left join {{ ref("users") }} as mu on mu.user_id = u.id
        where u.hubspot_contact_id is not null
        group by u.hubspot_contact_id
    ),

    app_accounts as (
        select gp_person_id, count(*) as app_account_count
        from {{ ref("int__civics_person_canonical_ids") }}
        where source_name = 'gp_api'
        group by gp_person_id
    ),

    -- How each contact joined its person, for reviewing the match itself.
    edges as (
        select unique_id_l as record_key, link_type
        from {{ ref("int__civics_person_links") }}
        where unique_id_l like 'hubspot|%'
        union
        select unique_id_r, link_type
        from {{ ref("int__civics_person_links") }}
        where unique_id_r like 'hubspot|%'
    ),

    linked_via as (
        select
            substring_index(record_key, '|', -1) as hs_contact_id,
            array_join(array_sort(collect_set(link_type)), ', ') as linked_via
        from edges
        group by record_key
    ),

    enriched as (
        select
            pc.gp_person_id,
            pc.hs_contact_id,
            pc.identity_key,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            -- Free text in HubSpot (Ohio, OH); normalized so contacts compare.
            coalesce(cs.state_cleaned_postal_code, c.state) as state,
            c.candidate_office,
            -- Comparison keys for match_evidence.
            nullif(right(regexp_replace(c.phone, '[^0-9]', ''), 10), '') as phone_key,
            lower(trim(c.first_name)) || ' ' || lower(trim(c.last_name)) as name_key,
            nullif(lower(trim(c.candidate_office)), '') as office_key,
            c.goodparty_user_id,
            c.hubspot_owner_id,
            c.lifecycle_stage,
            c.lead_status,
            c.type,
            c.product_user,
            c.win_stage,
            c.serve_stage,
            c.pledge_status,
            coalesce(c.is_pro_candidate, false) as is_pro_candidate,
            coalesce(c.has_ever_been_pro, false) as has_ever_been_pro,
            coalesce(c.num_notes, 0) as num_notes,
            c.last_contacted_at,
            c.hs_object_source_label as object_source,
            coalesce(c.contact_created_at, c.created_at) as contact_created_at,
            c.updated_at as contact_updated_at,
            al.hubspot_contact_id is not null as is_app_linked,
            coalesce(al.app_user_has_pro_campaign, false) as app_user_has_pro_campaign,
            al.app_user_last_active_at,
            lv.linked_via,
            -- Rep-driven progress. A closed disposition was worked, so it outranks
            -- an untouched intake.
            case
                when c.win_stage like 'Stage 6%'
                then 9
                when c.win_stage like 'Stage 5%'
                then 8
                when c.win_stage = 'Previously Pro'
                then 7
                when c.win_stage like 'Stage 4%'
                then 6
                when c.win_stage like 'Stage 3%'
                then 5
                when c.win_stage like 'Stage 2%'
                then 4
                when c.win_stage like 'Stage 1%'
                then 3
                when c.win_stage like 'Closed%'
                then 2
                when c.win_stage like 'Stage 0%'
                then 1
                else 0
            end as win_stage_rank,
            coalesce(c.num_notes, 0) > 0
            or c.last_contacted_at is not null as has_sales_activity,
            -- Not updated_at: imports and syncs touch every contact.
            greatest(
                c.last_contacted_at,
                c.notes_last_updated,
                c.hs_analytics_last_timestamp,
                c.email_last_open_at,
                c.email_last_click_at
            ) as last_engagement_at
        from person_contacts as pc
        inner join contacts as c on cast(c.id as string) = pc.hs_contact_id
        left join
            {{ ref("clean_states") }} as cs
            on upper(trim(c.state)) = upper(trim(cs.state_raw))
        left join app_links as al on al.hubspot_contact_id = pc.hs_contact_id
        left join linked_via as lv on lv.hs_contact_id = pc.hs_contact_id
    ),

    ranked as (
        select
            *,
            row_number() over (
                partition by gp_person_id
                order by
                    is_pro_candidate desc,
                    is_app_linked desc,
                    app_user_has_pro_campaign desc,
                    app_user_last_active_at desc nulls last,
                    win_stage_rank desc,
                    has_sales_activity desc,
                    last_engagement_at desc nulls last,
                    contact_created_at asc nulls last,
                    cast(hs_contact_id as bigint) asc
            ) as contact_rank
        from enriched
    ),

    primaries as (select * from ranked where contact_rank = 1),

    runners_up as (select * from ranked where contact_rank = 2),

    persons as (
        select
            p.gp_person_id,
            p.hs_contact_id as primary_hs_contact_id,
            -- The first ranking key on which the primary beat the runner-up.
            case
                when p.is_pro_candidate != r.is_pro_candidate
                then 'is_pro_candidate'
                when p.is_app_linked != r.is_app_linked
                then 'is_app_linked'
                when p.app_user_has_pro_campaign != r.app_user_has_pro_campaign
                then 'app_user_has_pro_campaign'
                when not p.app_user_last_active_at <=> r.app_user_last_active_at
                then 'app_user_last_active_at'
                when p.win_stage_rank != r.win_stage_rank
                then 'win_stage'
                when p.has_sales_activity != r.has_sales_activity
                then 'has_sales_activity'
                when not p.last_engagement_at <=> r.last_engagement_at
                then 'last_engagement_at'
                when not p.contact_created_at <=> r.contact_created_at
                then 'contact_created_at'
                else 'hs_contact_id'
            end as primary_decided_by
        from primaries as p
        inner join runners_up as r using (gp_person_id)
    ),

    person_flags as (
        select
            gp_person_id,
            count(*) as live_contact_count,
            count(distinct lower(trim(last_name))) > 1 as has_last_name_mismatch,
            -- HubSpot refuses a second live contact with the same email, so one of
            -- these was likely deleted or merged without the survivor showing it.
            count(lower(trim(email)))
            > count(distinct lower(trim(email))) as has_shared_email,
            count_if(is_pro_candidate) > 1 as has_multiple_pro_contacts,
            count(distinct hubspot_owner_id) > 1 as has_multiple_owners
        from ranked
        group by gp_person_id
    )

select
    r.gp_person_id,
    r.hs_contact_id,
    case when r.contact_rank = 1 then 'primary' else 'secondary' end as merge_role,
    p.primary_hs_contact_id,
    r.contact_rank,
    p.primary_decided_by,
    f.live_contact_count,

    -- Properties set on both where this secondary's value would be overwritten.
    -- Lifecycle stage and email are absent: HubSpot keeps the furthest stage, and
    -- adds the secondary's email to the primary as an additional address.
    case
        when r.contact_rank > 1
        then
            nullif(
                concat_ws(
                    ', ',
                    case when r.win_stage != pr.win_stage then 'win_stage' end,
                    case
                        when r.hubspot_owner_id != pr.hubspot_owner_id
                        then 'hubspot_owner_id'
                    end,
                    case when r.lead_status != pr.lead_status then 'lead_status' end,
                    case when r.serve_stage != pr.serve_stage then 'serve_stage' end,
                    case
                        when r.pledge_status != pr.pledge_status then 'pledge_status'
                    end,
                    case
                        when r.candidate_office != pr.candidate_office
                        then 'candidate_office'
                    end,
                    case
                        when r.goodparty_user_id != pr.goodparty_user_id
                        then 'goodparty_user_id'
                    end
                ),
                ''
            )
    end as conflicting_properties,

    -- Review flags, person-level.
    coalesce(aa.app_account_count, 0) > 1 as has_multiple_app_accounts,
    f.has_last_name_mismatch,
    f.has_shared_email,
    f.has_multiple_pro_contacts,
    f.has_multiple_owners,

    -- Native ids when the secondary shares the primary's deterministic identity;
    -- otherwise only the Splink person matcher joined them.
    case
        when r.contact_rank = 1
        then null
        when r.identity_key = pr.identity_key
        then 'native_ids'
        else 'splink'
    end as matched_on,
    -- What the secondary shares with the primary, strongest first, so a merge run
    -- can go in batches and leave the weakest for review. Null on the primary.
    case
        when r.contact_rank = 1
        then null
        when r.phone_key = pr.phone_key
        then 'same_phone'
        when r.name_key = pr.name_key and r.office_key = pr.office_key
        then 'same_name_and_office'
        when
            r.name_key = pr.name_key
            and r.state = pr.state
            and (r.office_key is null or pr.office_key is null)
        then 'same_name_and_state_office_unknown'
        when r.name_key = pr.name_key and r.state = pr.state
        then 'same_name_and_state_office_differs'
        when r.name_key = pr.name_key
        then 'same_name_state_unknown_or_differs'
        else 'name_differs'
    end as match_evidence,
    r.linked_via,

    -- Ranking inputs.
    r.is_pro_candidate,
    r.is_app_linked,
    r.app_user_has_pro_campaign,
    r.app_user_last_active_at,
    r.win_stage,
    r.win_stage_rank,
    r.has_sales_activity,
    r.last_engagement_at,
    r.contact_created_at,
    r.contact_updated_at,

    -- For side-by-side review.
    r.first_name,
    r.last_name,
    r.email,
    r.phone,
    r.state,
    r.candidate_office,
    r.goodparty_user_id,
    r.hubspot_owner_id,
    r.lifecycle_stage,
    r.lead_status,
    r.serve_stage,
    r.pledge_status,
    r.has_ever_been_pro,
    r.type,
    r.product_user,
    r.num_notes,
    r.last_contacted_at,
    r.object_source
from ranked as r
inner join persons as p using (gp_person_id)
inner join primaries as pr using (gp_person_id)
inner join person_flags as f using (gp_person_id)
left join app_accounts as aa using (gp_person_id)
