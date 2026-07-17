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
                    updated_at desc nulls last,
                    gp_candidate_id asc
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

    -- Seat-winner signal (fallback if "winner" must mean a literal election
    -- winner rather than a Win-org user), from candidacy.candidacy_result where a
    -- bare 'Won' = won the race's final stage. Computed once per candidate, then
    -- rolled up to the product user; the two are OR-merged in the final select so
    -- a win is caught whether the candidacy links to the contact via the
    -- candidate identity (gp_candidate_id — covers BR-sourced candidacies) or the
    -- product user (prod_db_user_id).
    -- Three-valued: TRUE = won a seat; FALSE = has a decided non-win and no win;
    -- NULL = no decided outcome at all. A decided loss deliberately takes
    -- precedence over a still-pending candidacy in a different race: via
    -- max(), TRUE > FALSE > NULL, so max(false, null) = false. (A candidate who
    -- lost one race and has another still pending reads FALSE, not NULL — the
    -- known non-win wins; has_candidacy still flags them as an active candidate.)
    -- A candidate with only pending candidacies and no decided result stays NULL.
    candidate_won as (
        select
            gp_candidate_id,
            max(
                case
                    when candidacy_result = 'Won'
                    then true
                    when candidacy_result in ('Lost', 'Withdrew', 'Not on Ballot')
                    then false
                    -- 'Runoff' (and any NULL/pending result) is not yet decided.
                    -- Explicit NULL so it reads as pending: on its own it leaves
                    -- has_won_election NULL, but a decided loss overrides it via
                    -- max() above. Kept explicit so it isn't mistaken for a gap.
                    when candidacy_result = 'Runoff'
                    then null
                end
            ) as has_won_election
        from {{ ref("candidacy") }}
        where gp_candidate_id is not null
        group by gp_candidate_id
    ),

    -- Same signal at the product-user grain, derived from candidate_won so the
    -- win/loss classification lives in one place (max of the boolean preserves
    -- the TRUE > FALSE > NULL precedence).
    user_won as (
        select
            cd.prod_db_user_id as gp_user_id,
            max(cw.has_won_election) as has_won_election
        from candidate_won cw
        join {{ ref("candidate") }} cd on cd.gp_candidate_id = cw.gp_candidate_id
        where cd.prod_db_user_id is not null
        group by cd.prod_db_user_id
    ),

    -- Same signal at the HubSpot-contact grain, across ALL candidate records for
    -- the contact. candidate_picked resolves only ONE candidate per contact (to
    -- keep gp_user_id unique), so a win on another candidate record for the same
    -- contact would be missed by the candidate-identity path alone. This rolls up
    -- every linked candidate so the win is caught regardless of which one was
    -- picked. Same TRUE > FALSE > NULL precedence via max().
    contact_won as (
        select cd.hubspot_contact_id, max(cw.has_won_election) as has_won_election
        from candidate_won cw
        join {{ ref("candidate") }} cd on cd.gp_candidate_id = cw.gp_candidate_id
        where cd.hubspot_contact_id is not null
        group by cd.hubspot_contact_id
    ),

    sales_touched as (
        select
            c.*,
            cp.gp_candidate_id,
            -- gp_user_id resolution: prefer HubSpot's direct contact->user link
            -- (goodparty_user_id from staging); fall back to candidate-mediated
            -- prod_db_user_id when the direct link is missing. The direct link is
            -- contact-specific rather than going through candidacy mapping. About
            -- 1,650 contacts with Win/Serve orgs are only discoverable via the
            -- direct link.
            coalesce(c.goodparty_user_id, cp.prod_db_user_id) as gp_user_id,
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
            (cc.contact_id is not null) as was_called,
            (
                c.serve_lifecycle_lead_entered_at is not null
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
            ) as in_serve_funnel,
            (
                nullif(trim(c.win_stage), '') is not null
                or coalesce(c.win_activated_user ilike 'true', false)
            ) as in_win_funnel,
            -- Earliest available timestamp signaling Sales touched this
            -- contact. Considers all Serve lifecycle transitions, the
            -- HubSpot serve_activated_at and serve_opt_in_at timestamps,
            -- and the first call. NULL when none are populated (e.g.,
            -- contacts gated in only by Win-stage snapshot strings or
            -- pledge/opt-in boolean flags that carry no timestamp).
            nullif(
                least(
                    coalesce(c.serve_lifecycle_lead_entered_at, timestamp '9999-01-01'),
                    coalesce(c.serve_lifecycle_sal_entered_at, timestamp '9999-01-01'),
                    coalesce(
                        c.serve_lifecycle_scheduled_sql_entered_at,
                        timestamp '9999-01-01'
                    ),
                    coalesce(c.serve_lifecycle_sql_entered_at, timestamp '9999-01-01'),
                    coalesce(
                        c.serve_lifecycle_opportunity_entered_at, timestamp '9999-01-01'
                    ),
                    coalesce(
                        c.serve_lifecycle_converted_entered_at, timestamp '9999-01-01'
                    ),
                    coalesce(
                        c.serve_lifecycle_activated_entered_at, timestamp '9999-01-01'
                    ),
                    coalesce(
                        c.serve_lifecycle_retained_entered_at, timestamp '9999-01-01'
                    ),
                    coalesce(
                        c.serve_lifecycle_churned_entered_at, timestamp '9999-01-01'
                    ),
                    coalesce(
                        c.serve_lifecycle_not_qualified_entered_at,
                        timestamp '9999-01-01'
                    ),
                    coalesce(c.serve_activated_at, timestamp '9999-01-01'),
                    coalesce(c.serve_opt_in_at, timestamp '9999-01-01'),
                    coalesce(cc.first_call_at, timestamp '9999-01-01')
                ),
                timestamp '9999-01-01'
            ) as first_sales_touch_at
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

            -- Sales attribution — person-based. in_serve_funnel and
            -- in_win_funnel are computed once in sales_touched and
            -- referenced here directly.
            st.in_serve_funnel,
            st.in_win_funnel,
            (st.in_serve_funnel and st.in_win_funnel) as in_both_funnels,
            st.was_called,
            st.first_sales_touch_at,
            date_trunc('month', st.first_sales_touch_at) as first_sales_touch_month,

            -- Position-based ICP (secondary dimensions; NOT person attribution)
            st.position_icp_serve,
            st.position_icp_win,

            -- Winner / EO classification — canonical (from organizations)
            coalesce(uo.has_win_org, false) as has_win_org,
            coalesce(uo.has_serve_org, false) as has_serve_org,
            -- "Win or Serve product user" — NOT literal election winner; see
            -- has_won_election below.
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
            coalesce(
                st.contact_type_raw like '%Self-Filer Lead%', false
            ) as is_self_filer_lead,
            coalesce(
                st.contact_type_raw like '%Historical Incumbent%', false
            ) as is_historical_incumbent,
            coalesce(st.contact_type_raw like '%Campaign%', false) as is_campaign_type,
            coalesce(
                st.contact_type_raw like '%Historical Incumbent%'
                and (
                    st.contact_type_raw like '%Self-Filer Lead%'
                    or st.contact_type_raw like '%Campaign%'
                ),
                false
            ) as is_dual_pursuit,
            st.contact_type_raw,
            st.candidate_type,

            -- Seat-winner sub-segment. TRUE = won a seat; FALSE = a candidate
            -- with a decided non-win and no win (a decided loss takes precedence
            -- over any still-pending candidacy in another race); NULL = no decided
            -- outcome at all (all pending / Cannot Determine) or not a candidate.
            -- OR-merged across three links so a win is caught however the winning
            -- candidacy attaches: the picked candidate identity (cw), the product
            -- user (uw), and a rollup across ALL candidate records for the HubSpot
            -- contact (con) — the last covers wins on a candidate row other than
            -- the one candidate_picked chose. Use has_candidacy to tell a pending
            -- candidate apart from a non-candidate.
            -- OR (not AND) is deliberate: it lets a FALSE on a single populated
            -- link classify contacts that have only one link type; an AND would
            -- leave those unclassifiable. The trade-off is a rare cross-identity
            -- contact whose links disagree (one all-lost, the other pending)
            -- reading FALSE instead of NULL. A fully three-valued answer would
            -- need a separate has_pending signal, since max(bool) already drops
            -- pending under a decided loss; not worth it for the volume.
            case
                when cw.has_won_election or uw.has_won_election or con.has_won_election
                then true
                when
                    cw.has_won_election is false
                    or uw.has_won_election is false
                    or con.has_won_election is false
                then false
            end as has_won_election,
            -- Whether the contact is a candidate at all, via the candidate
            -- identity or the product user — most prospects are. Tests the raw
            -- st.gp_candidate_id (not cw.gp_candidate_id) so a candidate identity
            -- with no candidacy row yet in the mart (e.g. a newly onboarded
            -- gp_api candidate not matched to any BR/TS/DDHQ race) still counts.
            -- The user side stays keyed on user_won (uw) — a bare product user
            -- without a candidacy is not a candidate. has_won_election's NULL
            -- alone can't separate a pending candidate from a non-candidate
            -- sales contact; this can.
            (
                st.gp_candidate_id is not null or uw.gp_user_id is not null
            ) as has_candidacy,

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
            -- Broad activation flag: any signal that this contact is an
            -- activated Serve user (lifecycle stage, HubSpot timestamp,
            -- or HubSpot boolean flag). Wider than is_serve_activated_prospect,
            -- which is stage-specific.
            (
                st.serve_lifecycle_activated_entered_at is not null
                or st.serve_activated_at is not null
                or coalesce(st.serve_activated_user ilike 'true', false)
            ) as is_serve_activated_any,
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
            -- Broad activation flag: stage-6 OR the HubSpot
            -- win_activated_user boolean. Wider than is_win_activated_prospect,
            -- which is stage-specific.
            (
                coalesce(st.win_stage = 'Stage 6 - Activated Pro User', false)
                or coalesce(st.win_activated_user ilike 'true', false)
            ) as is_win_activated_any,
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
        left join user_won uw on st.gp_user_id = uw.gp_user_id
        left join candidate_won cw on st.gp_candidate_id = cw.gp_candidate_id
        left join contact_won con on st.id = con.hubspot_contact_id
    )

-- One row per HubSpot contact, BUT collapsed to one row per gp_user_id
-- where gp_user_id is resolved. Two HubSpot contacts pointing at the
-- same product user (a known dup-data-entry pattern in HubSpot) are
-- deduplicated by picking the contact carrying the strongest funnel
-- signal first, so activation/conversion counts don't lose information
-- to dedup. Rows where gp_user_id is NULL are kept as-is (contact grain
-- preserved for unconverted prospects).
select *
from final
qualify
    case
        when gp_user_id is null
        then 1
        else
            row_number() over (
                partition by gp_user_id
                order by
                    -- Prefer the contact carrying the strongest funnel signal
                    -- so activation/conversion counts don't lose information
                    -- to dedup. A user with one activated contact and one
                    -- in-pipeline contact is still an activated user.
                    case
                        when win_stage = 'Stage 6 - Activated Pro User'
                        then 6
                        when is_serve_activated_prospect
                        then 6
                        when win_stage = 'Stage 5 – Closed Won (Pro)'
                        then 5
                        when win_stage = 'Stage 4 – Pro Consideration'
                        then 4
                        when win_stage = 'Stage 3 – Pro Presented'
                        then 3
                        when win_stage = 'Stage 2 – Discovery Complete'
                        then 2
                        when win_stage = 'Stage 1 – New / Assigned'
                        then 1
                        when win_stage = 'Stage 0 – Intake'
                        then 0
                        when in_win_funnel
                        then -1
                        when is_eo_sales_attributed
                        then -1
                        when in_serve_funnel
                        then -2
                        else -3
                    end desc,
                    -- Tiebreak within same funnel rank: prefer the contact with
                    -- the most activity (recency-weighted) and a deterministic
                    -- final tiebreak.
                    was_called desc,
                    last_call_at desc nulls last,
                    updated_at desc nulls last,
                    contact_id asc
            )
    end
    = 1
