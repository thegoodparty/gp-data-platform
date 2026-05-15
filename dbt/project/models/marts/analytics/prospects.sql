{{ config(materialized="view") }}

/*
    mart_analytics.prospects

    Wide base table at HubSpot-contact grain for prospect-funnel metrics.
    One row per sales-touched HubSpot contact (called OR in any Serve/Win
    funnel signal OR pledged OR serve-opted-in).

    CAVEAT: Win activation rates derived from this mart are
    current-snapshot ratios (e.g., contacts currently in Stage 6 vs
    contacts currently in Stage 0), not true cohort transition rates.
    The HubSpot Win lifecycle entered_at workflow is not enabled today;
    win_stage string snapshots are used instead. Serve rates ARE true
    transition rates (Serve lifecycle workflow is enabled).
*/
with

    contacts as (select * from {{ ref("int__hubspot_prospect_contacts") }}),

    contact_calls as (select * from {{ ref("int__hubspot_contact_calls") }}),

    -- Multi-user-contact resolution: prefer a candidate row with a
    -- prod_db_user_id populated; tiebreak by most-recently-updated. Keeps
    -- gp_user_id unique per contact (the unique-where-not-null test on
    -- the mart depends on this).
    candidate_picked as (
        select hubspot_contact_id, gp_candidate_id, prod_db_user_id
        from {{ ref("candidate") }}
        where hubspot_contact_id is not null
        qualify
            row_number() over (
                partition by hubspot_contact_id
                order by
                    (prod_db_user_id is not null) desc nulls last,
                    updated_at desc nulls last
            )
            = 1
    ),

    -- Canonical Win/Serve user classification. organizations.user_id is
    -- what the civics mart names the product-DB user_id — we alias it to
    -- gp_user_id for consistency throughout the prospects mart.
    user_org_types as (
        select
            user_id as gp_user_id,
            max(
                case when organization_type = 'win' then true else false end
            ) as has_win_org,
            max(
                case when organization_type = 'serve' then true else false end
            ) as has_serve_org
        from {{ ref("organizations") }}
        where user_id is not null
        group by user_id
    ),

    -- General-election winners (fallback signal if "winner" needs to mean
    -- literal election winners rather than Win-org users).
    user_won_general as (
        select
            cd.prod_db_user_id as gp_user_id,
            max(cy.general_election_result = 'Won') as has_won_general_election
        from {{ ref("candidate") }} cd
        join {{ ref("candidacy") }} cy on cd.gp_candidate_id = cy.gp_candidate_id
        where cd.prod_db_user_id is not null
        group by cd.prod_db_user_id
    ),

    sales_touched as (
        select
            c.*,
            cp.gp_candidate_id,
            cp.prod_db_user_id as gp_user_id,
            cc.total_calls,
            cc.connected_pledge_calls,
            cc.connected_reject_calls,
            cc.connected_other_calls,
            cc.not_connected_calls,
            cc.unmapped_calls,
            cc.null_disposition_calls,
            cc.connected_calls,
            coalesce(cc.ever_connected, false) as ever_connected,
            cc.first_call_at,
            cc.last_call_at,
            cc.last_connected_call_at,
            cc.distinct_owners,
            cc.last_owner_id,
            (cc.contact_id is not null) as was_called
        from contacts c
        left join contact_calls cc on c.id = cc.contact_id
        left join candidate_picked cp on c.id = cp.hubspot_contact_id
        -- Sales-touched gate: any Serve/Win/pledge/opt-in signal OR called
        where
            cc.contact_id is not null
            or c.serve_lifecycle_lead_entered_at is not null
            or c.serve_lifecycle_sal_entered_at is not null
            or c.serve_lifecycle_scheduled_sql_entered_at is not null
            or c.serve_lifecycle_sql_entered_at is not null
            or c.serve_lifecycle_opportunity_entered_at is not null
            or c.serve_lifecycle_converted_entered_at is not null
            or c.serve_lifecycle_activated_entered_at is not null
            or c.serve_lifecycle_retained_entered_at is not null
            or c.serve_lifecycle_churned_entered_at is not null
            or c.serve_lifecycle_not_qualified_entered_at is not null
            or c.serve_stage is not null
            or c.serve_activated_at is not null
            or nullif(trim(c.win_stage), '') is not null
            or c.win_activated_user ilike 'true'
            or c.is_pledged
            or c.has_verbal_pledge
            or c.has_email_pledge
            or c.is_serve_opted_in
    ),

    final as (
        select
            -- Identity
            st.id as contact_id,
            st.email,
            st.first_name,
            st.last_name,
            st.full_name,
            st.state,
            st.city,
            st.phone_number,

            -- User bridge
            st.gp_candidate_id,
            st.gp_user_id,

            -- Sales attribution — person-based. in_serve_funnel is broad:
            -- ANY Serve signal (lifecycle, stage, or activated_at). Matches
            -- the sales-touched gate exactly so the YAML test stays in sync
            -- with the WHERE clause.
            (
                st.serve_lifecycle_lead_entered_at is not null
                or st.serve_lifecycle_sal_entered_at is not null
                or st.serve_lifecycle_scheduled_sql_entered_at is not null
                or st.serve_lifecycle_sql_entered_at is not null
                or st.serve_lifecycle_opportunity_entered_at is not null
                or st.serve_lifecycle_converted_entered_at is not null
                or st.serve_lifecycle_activated_entered_at is not null
                or st.serve_lifecycle_retained_entered_at is not null
                or st.serve_lifecycle_churned_entered_at is not null
                or st.serve_lifecycle_not_qualified_entered_at is not null
                or st.serve_stage is not null
                or st.serve_activated_at is not null
            ) as in_serve_funnel,
            (
                nullif(trim(st.win_stage), '') is not null
                or coalesce(st.win_activated_user ilike 'true', false)
            ) as in_win_funnel,
            (
                (
                    st.serve_lifecycle_lead_entered_at is not null
                    or st.serve_lifecycle_sal_entered_at is not null
                    or st.serve_lifecycle_scheduled_sql_entered_at is not null
                    or st.serve_lifecycle_sql_entered_at is not null
                    or st.serve_lifecycle_opportunity_entered_at is not null
                    or st.serve_lifecycle_converted_entered_at is not null
                    or st.serve_lifecycle_activated_entered_at is not null
                    or st.serve_lifecycle_retained_entered_at is not null
                    or st.serve_lifecycle_churned_entered_at is not null
                    or st.serve_lifecycle_not_qualified_entered_at is not null
                    or st.serve_stage is not null
                    or st.serve_activated_at is not null
                )
                and (
                    nullif(trim(st.win_stage), '') is not null
                    or coalesce(st.win_activated_user ilike 'true', false)
                )
            ) as in_both_funnels,
            st.was_called,
            least(
                st.serve_lifecycle_lead_entered_at,
                st.serve_lifecycle_activated_entered_at,
                st.first_call_at
            ) as first_sales_touch_at,
            date_trunc(
                'month',
                least(
                    st.serve_lifecycle_lead_entered_at,
                    st.serve_lifecycle_activated_entered_at,
                    st.first_call_at
                )
            ) as first_sales_touch_month,

            -- Position-based ICP (secondary dimensions; NOT person attribution)
            st.position_icp_serve,
            st.position_icp_win,

            -- Winner / EO classification — canonical (from organizations)
            coalesce(uo.has_win_org, false) as has_win_org,
            coalesce(uo.has_serve_org, false) as has_serve_org,
            -- "Win or Serve product user" — NOT literal election winner; see
            -- has_won_general_election below.
            (
                coalesce(uo.has_win_org, false) or coalesce(uo.has_serve_org, false)
            ) as is_winner_or_eo,

            -- Secondary HubSpot-intent signals (Sales-pursuit, not user-confirmed)
            (
                nullif(trim(st.win_stage), '') is not null
                or coalesce(st.win_activated_user ilike 'true', false)
            ) as is_winner_pursuit,
            (
                st.serve_lifecycle_activated_entered_at is not null
            ) as is_eo_sales_attributed,

            -- HubSpot Sales pursuit taxonomy. Derived from contact_type_raw
            -- (semicolon-separated multi-value). Distinguishes pure Win
            -- pursuit from pure Serve pursuit from dual pursuit (e.g. EO
            -- running for re-election). Dual-pursuit contacts legitimately
            -- appear in both Win and Serve metric denominators because
            -- Sales is pursuing them for both products.
            (st.contact_type_raw like '%Self-Filer Lead%') as is_self_filer_lead,
            (
                st.contact_type_raw like '%Historical Incumbent%'
            ) as is_historical_incumbent,
            (st.contact_type_raw like '%Campaign%') as is_campaign_type,
            (
                st.contact_type_raw like '%Historical Incumbent%'
                and (
                    st.contact_type_raw like '%Self-Filer Lead%'
                    or st.contact_type_raw like '%Campaign%'
                )
            ) as is_dual_pursuit,
            st.contact_type_raw,
            st.candidate_type,

            -- General-election-winner sub-segment
            coalesce(ug.has_won_general_election, false) as has_won_general_election,

            -- Serve funnel state (true transition rates — Serve workflow enabled)
            st.serve_lifecycle_lead_entered_at,
            st.serve_lifecycle_sal_entered_at,
            st.serve_lifecycle_scheduled_sql_entered_at,
            st.serve_lifecycle_sql_entered_at,
            st.serve_lifecycle_opportunity_entered_at,
            st.serve_lifecycle_converted_entered_at,
            st.serve_lifecycle_activated_entered_at,
            st.serve_lifecycle_retained_entered_at,
            st.serve_lifecycle_churned_entered_at,
            st.serve_lifecycle_not_qualified_entered_at,
            st.serve_stage,
            (
                st.serve_lifecycle_activated_entered_at is not null
            ) as is_serve_activated_prospect,
            st.is_serve_opted_in,
            st.serve_activated_at,
            st.number_of_serve_calls,

            -- Win funnel state (snapshot ratios only)
            st.win_stage,
            coalesce(
                st.win_stage = 'Stage 0 – Intake', false
            ) as is_win_intake_prospect,
            coalesce(
                st.win_stage = 'Stage 3 – Pro Presented', false
            ) as is_win_pro_presented_prospect,
            coalesce(
                st.win_stage = 'Stage 5 – Closed Won (Pro)', false
            ) as is_win_pro_won_prospect,
            coalesce(
                st.win_stage = 'Stage 6 - Activated Pro User', false
            ) as is_win_activated_prospect,
            coalesce(
                st.win_activated_user ilike 'true', false
            ) as win_activated_user_hs_flag,

            -- Quality / verification
            st.is_pledged,
            st.has_verbal_pledge,
            st.has_email_pledge,
            st.pledge_status,
            st.verified_candidate_status,
            st.lead_status,

            -- Call activity (NULL when never called)
            st.total_calls,
            st.connected_pledge_calls,
            st.connected_reject_calls,
            st.connected_other_calls,
            st.not_connected_calls,
            st.unmapped_calls,
            st.null_disposition_calls,
            st.connected_calls,
            st.ever_connected,
            st.first_call_at,
            st.last_call_at,
            st.last_connected_call_at,
            st.distinct_owners,
            st.last_owner_id,

            -- Metadata
            st.created_at,
            st.updated_at
        from sales_touched st
        left join user_org_types uo on st.gp_user_id = uo.gp_user_id
        left join user_won_general ug on st.gp_user_id = ug.gp_user_id
    )

select *
from final
