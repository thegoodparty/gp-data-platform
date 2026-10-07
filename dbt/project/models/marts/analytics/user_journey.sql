-- The end-to-end user view: one row per gp-api user account, across
-- acquisition, sales contact, the current candidacy, product use, outreach,
-- revenue and outcome. Internal only.
--
-- Assembly only. Every fact is defined in the user-grain model that owns it;
-- this table joins them on user_id and derives nothing but durations and
-- data-quality flags. A definition change belongs upstream, never here.
--
-- Read only by design: the scheduled build is the only writer and there is no
-- write-back path. Record edits belong to product admin.
--
-- Columns that measure against today (days_to_*, is_still_running, the anchor
-- choice itself, the rolling month flags) move with the build date, so a
-- rebuild is idempotent for a given day, not across days.
with
    users as (
        select
            user_id,
            email,
            first_name,
            last_name,
            phone,
            zip,
            created_at as registered_at,
            date_trunc('month', created_at) as registration_month,
            case
                when is_win_user and is_serve_user
                then 'both'
                when is_win_user
                then 'win'
                when is_serve_user
                then 'serve'
                -- Signed up and never joined an organization in either product.
                else 'none'
            end as product,
            campaign_count,
            non_demo_campaign_count,
            first_campaign_created_at,
            last_campaign_created_at,
            campaign_count > 0 and non_demo_campaign_count = 0 as is_demo_only,
            is_serve_user,
            eo_activated_at
        from {{ ref("users") }}
    ),

    keys as (
        select
            user_id,
            gp_person_id,
            hubspot_contact_id,
            hubspot_key_source,
            stripe_customer_id,
            account_count,
            is_primary_account,
            had_conflict
        from {{ ref("int__user_resolved_keys") }}
    ),

    milestones as (
        select user_id, registration_country
        from {{ ref("int__amplitude_user_milestones") }}
    ),

    active_eos as (select user_id from {{ ref("users_serve_active") }}),

    -- Read from the model the Activated Candidates OKR reads, so this row and
    -- the reported figure cannot drift. Win accounts only; null elsewhere.
    win_activation as (
        select user_id, is_activated, first_campaign_sent_at as activated_at
        from {{ ref("users_win_base") }}
    ),

    -- Staff and test accounts, from the one definition public feeds use. Keyed
    -- on the person, so a signup newer than the person graph reads false until
    -- the next rebuild; the gp_person_id test warns when any such row exists.
    internal as (select gp_person_id from {{ ref("int__civics_internal_persons") }})

select
    u.user_id,
    k.gp_person_id,
    k.hubspot_contact_id,
    k.hubspot_key_source,
    k.stripe_customer_id,
    k.account_count,
    k.is_primary_account,
    k.had_conflict,

    u.email,
    u.first_name,
    u.last_name,
    u.phone,
    u.zip,
    u.registered_at,
    u.registration_month,
    u.product,
    m.registration_country,

    {{
        dbt_utils.star(
            from=ref("int__user_candidacy_profile"),
            except=["user_id"],
            relation_alias="cand",
        )
    }},
    u.campaign_count,
    u.non_demo_campaign_count,
    u.first_campaign_created_at,
    u.last_campaign_created_at,

    {{
        dbt_utils.star(
            from=ref("int__user_hubspot_profile"),
            except=["user_id"],
            relation_alias="hs",
        )
    }},
    utm.utm_source_first,
    utm.utm_medium_first,
    utm.utm_campaign_first,
    utm.utm_source_first_normalized,
    utm.utm_medium_first_normalized,

    {{
        dbt_utils.star(
            from=ref("int__user_product_activity"),
            except=["user_id"],
            relation_alias="act",
        )
    }},

    -- The anchor date already ships as candidacy_election_date.
    {{
        dbt_utils.star(
            from=ref("int__user_outreach_intensity"),
            except=["user_id", "election_date"],
            relation_alias="outr",
        )
    }},

    {{
        dbt_utils.star(
            from=ref("int__user_revenue_profile"),
            except=["user_id"],
            relation_alias="rev",
        )
    }},

    wa.is_activated,
    wa.activated_at,

    {{
        dbt_utils.star(
            from=ref("int__user_power_user"),
            except=["user_id"],
            relation_alias="pu",
        )
    }},

    u.is_serve_user,
    u.eo_activated_at,
    ae.user_id is not null as is_active_eo,

    datediff(
        cand.candidacy_election_date, current_date()
    ) as days_to_candidacy_election,
    datediff(cand.candidacy_filing_deadline, current_date()) as days_to_filing_deadline,
    datediff(u.registered_at, hs.first_touch_at) as first_touch_to_signup_days,
    datediff(
        act.onboarding_started_at, u.registered_at
    ) as signup_to_onboarding_started_days,
    datediff(
        act.onboarding_completed_at, act.onboarding_started_at
    ) as onboarding_started_to_completed_days,
    datediff(
        act.onboarding_completed_at, u.registered_at
    ) as signup_to_onboarding_completed_days,
    datediff(
        wa.activated_at, act.onboarding_completed_at
    ) as onboarding_completed_to_activated_days,
    datediff(
        rev.pro_since, act.onboarding_completed_at
    ) as onboarding_completed_to_pro_days,
    datediff(act.product_output_at, u.registered_at) as signup_to_product_output_days,
    datediff(
        cand.candidacy_election_date, hs.first_touch_at
    ) as first_touch_to_election_days,
    datediff(
        cand.candidacy_filing_deadline, hs.first_touch_at
    ) as first_touch_to_filing_days,

    u.is_demo_only,
    i.gp_person_id is not null as is_internal,
    current_timestamp() as record_refreshed_at
from users as u
left join keys as k using (user_id)
left join milestones as m using (user_id)
left join {{ ref("int__user_candidacy_profile") }} as cand using (user_id)
left join {{ ref("int__user_hubspot_profile") }} as hs using (user_id)
left join {{ ref("int__user_first_touch_utm") }} as utm using (user_id)
left join {{ ref("int__user_product_activity") }} as act using (user_id)
left join {{ ref("int__user_outreach_intensity") }} as outr using (user_id)
left join {{ ref("int__user_revenue_profile") }} as rev using (user_id)
left join active_eos as ae using (user_id)
left join win_activation as wa using (user_id)
left join {{ ref("int__user_power_user") }} as pu using (user_id)
left join internal as i on i.gp_person_id = k.gp_person_id
