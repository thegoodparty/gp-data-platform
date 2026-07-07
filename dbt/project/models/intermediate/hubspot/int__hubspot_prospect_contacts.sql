{{
    config(
        auto_liquid_cluster=true,
    )
}}

-- One row per HubSpot contact. Surfaces lifecycle/stage/activation/pledge
-- fields plus ICP enrichment from int__hubspot_contacts. Does NOT filter
-- by name (unlike int__hubspot_contacts) so prospects without a recorded
-- name are still included.
with

    contacts as (select * from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}),

    -- Resolved ICP at the position level. Renamed here to make the
    -- position-basis explicit in column names (these flags describe the
    -- office, not the person's pursuit type).
    contact_icp as (
        select
            id,
            icp_win as position_icp_win,
            icp_serve as position_icp_serve,
            icp_win_supersize as position_icp_win_supersize
        from {{ ref("int__hubspot_contacts") }}
    ),

    final as (
        select
            -- Identity (no name filter)
            c.id,
            -- HubSpot contact's direct link to the product DB user. Distinct
            -- from the candidate-mediated path: a contact may have this set
            -- even when no candidate row exists for the user. Used by the
            -- prospects mart as the preferred gp_user_id source.
            c.goodparty_user_id,
            c.email,
            c.first_name,
            c.last_name,
            c.full_name,
            c.state,
            c.city,
            c.phone as phone_number,

            -- Resolved position-based ICP (left join — NULL where no
            -- resolution; do NOT coalesce to false because "not ICP" and
            -- "unknown" are distinct states for analytics consumers).
            i.position_icp_win,
            i.position_icp_serve,
            i.position_icp_win_supersize,

            -- HubSpot lifecycle current snapshot strings
            c.lifecycle_stage,
            c.serve_stage,
            c.win_stage,

            -- Serve lifecycle transition timestamps
            c.serve_lifecycle_lead_entered_at,
            c.serve_lifecycle_sal_entered_at,
            c.serve_lifecycle_scheduled_sql_entered_at,
            c.serve_lifecycle_sql_entered_at,
            c.serve_lifecycle_opportunity_entered_at,
            c.serve_lifecycle_converted_entered_at,
            c.serve_lifecycle_activated_entered_at,
            c.serve_lifecycle_retained_entered_at,
            c.serve_lifecycle_churned_entered_at,
            c.serve_lifecycle_not_qualified_entered_at,

            -- Win stage transition timestamps. Surfaced for futureproofing
            -- once the HubSpot Win lifecycle workflow is enabled (today the
            -- win_stage string snapshot is the primary signal).
            c.win_stage_0_intake_entered_at,
            c.win_stage_1_new_assigned_entered_at,
            c.win_stage_2_discovery_complete_entered_at,
            c.win_stage_3_pro_presented_entered_at,
            c.win_stage_4_pro_consideration_entered_at,
            c.win_stage_5_closed_won_pro_entered_at,
            c.win_stage_6_activated_pro_user_entered_at,
            c.win_stage_closed_too_early_entered_at,

            -- Activation / quality / pledge signals
            c.serve_activated_user,
            c.serve_activated_at,
            c.serve_opt_in_at,
            c.is_serve_opted_in,
            c.serve_onboarding_stage,
            c.win_activated_user,
            c.is_pledged,
            c.has_verbal_pledge,
            c.has_email_pledge,
            c.has_verbal_serve,
            c.pledge_status,
            c.verified_candidate_status,
            c.lead_status,
            c.number_of_serve_calls,

            -- HubSpot Sales pursuit taxonomy. `type` is semicolon-separated
            -- multi-value: Self-Filer Lead / Campaign → Win pursuit;
            -- Historical Incumbent → Serve pursuit; combinations flag dual
            -- pursuits (e.g. EOs running for re-election). `candidate_type`
            -- is separate (Incumbent / Challenger) for current-race status.
            c.type as contact_type_raw,
            c.candidate_type,

            -- Metadata
            c.created_at,
            c.updated_at
        from contacts c
        left join contact_icp i on c.id = i.id
    )

select *
from final
