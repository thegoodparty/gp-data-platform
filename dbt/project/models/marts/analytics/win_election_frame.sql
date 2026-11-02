-- One row per Win candidate with a 2026 race: who they are, how the race went,
-- and what they did in the product before it. The post-election analysis reads
-- only this table, so every count it prints traces to a flag here.
--
-- Everything behavioral is cut off at candidacy_race_date, the primary date for
-- a primary loser. Feature and audience definitions come from the shared
-- models; this table only picks the population and applies the cutoff.
--
-- The result columns stay null until a result is matched, so the same rows
-- serve before, during and after results come in. has_result is the coverage
-- measure.
with
    candidates as (
        select
            user_id,
            candidacy_campaign_id as campaign_id,
            candidacy_state as state,
            candidacy_office as office,
            candidacy_office_level_normalized as office_level,
            candidacy_position_id as position_id,
            candidacy_election_date as election_date,
            candidacy_primary_election_date as primary_election_date,
            candidacy_latest_stage_reached as latest_stage_reached,
            candidacy_result,
            candidacy_legitimacy_label as legitimacy_label,
            candidacy_is_incumbent as is_incumbent,
            candidacy_is_uncontested as is_uncontested,
            candidacy_is_open_seat as is_open_seat,
            candidacy_seats_available as seats_available,
            candidacy_number_of_opponents as number_of_opponents,
            candidacy_is_partisan as is_partisan,
            candidacy_voter_count as voter_count,
            candidacy_viability_score as viability_score,
            candidacy_is_verified as is_verified,
            registered_at,
            acquisition_motion,
            hs_analytics_source,
            is_activated,
            activated_at,
            is_pro,
            pro_since,
            is_power_user,
            onboarding_completed_at,
            coalesce(is_internal, false)
            or coalesce(is_demo_only, false) as is_staff_or_demo,
            candidacy_race_date as race_date
        from {{ ref("user_journey") }}
        where has_candidacy and year(candidacy_election_date) = 2026
    ),

    -- Feature definitions are shared; this table only applies the race cutoff.
    -- The race's actual candidate list, where civics has it. The journey's
    -- opponent count misses real opponents in some races.
    race as (
        select
            c.user_id,
            bool_or(f.is_contested) as is_contested,
            max(f.baseline_win_chance) as baseline_win_chance
        from candidates as c
        inner join
            {{ ref("win_race_field") }} as f
            on f.user_id = c.user_id
            and f.election_date = c.race_date
        group by c.user_id
    ),

    product_use as (
        select
            c.user_id,
            coalesce(
                bool_or(
                    f.feature = 'onboarding_path_to_victory'
                    and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_onboarding_path_to_victory,
            coalesce(
                bool_or(
                    f.feature = 'onboarding_v2' and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_onboarding_v2,
            coalesce(
                bool_or(
                    f.feature = 'plan_task' and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_plan_task,
            coalesce(
                bool_or(
                    f.feature = 'plan_opposition_research'
                    and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_plan_opposition_research,
            coalesce(
                bool_or(
                    f.feature = 'story_rewrite' and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_story_rewrite,
            coalesce(
                bool_or(
                    f.feature = 'opponent_lookup'
                    and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_opponent_lookup,
            coalesce(
                bool_or(f.feature = 'ai_help' and date(f.first_used_at) < c.race_date),
                false
            ) as used_ai_help,
            coalesce(
                bool_or(
                    f.feature = 'content_builder'
                    and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_content_builder,
            coalesce(
                bool_or(
                    f.feature = 'segment_build' and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_segment_build,
            coalesce(
                bool_or(
                    f.feature = 'voter_file_download'
                    and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_voter_file_download,
            coalesce(
                bool_or(
                    f.feature = 'team_invite' and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_team_invite,
            coalesce(
                bool_or(
                    f.feature = 'website_publish'
                    and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_website_publish,
            coalesce(
                bool_or(
                    f.feature = 'profile_complete'
                    and date(f.first_used_at) < c.race_date
                ),
                false
            ) as used_profile_complete,
            min(
                case when f.feature = 'ai_help' then date(f.first_used_at) end
            ) as first_ai_help_date,
            min(
                case when f.feature = 'opponent_lookup' then date(f.first_used_at) end
            ) as first_opponent_lookup_date
        from candidates as c
        left join {{ ref("user_feature_use") }} as f on f.user_id = c.user_id
        group by c.user_id
    ),

    sends as (
        select s.user_id, s.channel, s.send_at, s.recipient_count
        from {{ ref("int__user_outreach_sends") }} as s
        where s.product = 'win'
    ),

    outreach as (
        select
            c.user_id,
            count(s.send_at) as sends_before_race,
            coalesce(sum(s.recipient_count), 0) as recipients_before_race,
            count(
                distinct case when s.channel != 'unattributed' then s.channel end
            ) as channels_used,
            count_if(s.channel = 'door_knocking') > 0 as did_door_knocking,
            -- Weeks with a send in the final eight weeks.
            count(
                distinct case
                    when date(s.send_at) >= date_sub(c.race_date, 56)
                    then date_trunc('week', s.send_at)
                end
            ) as send_weeks_last_8
        from candidates as c
        left join sends as s on s.user_id = c.user_id and date(s.send_at) < c.race_date
        group by c.user_id
    ),

    audience_sends as (
        select
            a.user_id,
            a.voter_file_filter_id,
            a.message_text,
            a.demographic_dimension_count,
            a.is_own_audience
        from {{ ref("int__user_outreach_audiences") }} as a
        inner join candidates as c on c.user_id = a.user_id
        where a.status in ('completed', 'in_progress') and date(a.send_at) < c.race_date
    ),

    targeting as (
        select
            user_id,
            count_if(is_own_audience) > 0 as did_send_to_own_segment,
            count(
                distinct case
                    when demographic_dimension_count > 0 then voter_file_filter_id
                end
            ) as demographic_audiences,
            count(
                distinct case when demographic_dimension_count > 0 then message_text end
            ) as demographic_messages,
            max(demographic_dimension_count) as max_demographic_dimensions
        from audience_sends
        group by user_id
    ),

    activity as (
        select
            c.user_id,
            coalesce(sum(w.win_activity_days), 0) as active_days_last_12_weeks
        from candidates as c
        left join
            {{ ref("user_win_activity_weekly") }} as w
            on w.user_id = c.user_id
            and w.week_start_date >= date_sub(c.race_date, 84)
            and w.week_start_date < c.race_date
        group by c.user_id
    ),

    team_contact as (
        select
            c.user_id,
            count_if(t.is_outbound_touch) as team_touches_before_race,
            count_if(t.channel = 'meeting') > 0 as had_team_meeting,
            min(
                case when t.is_outbound_touch then date(t.touch_at) end
            ) as first_team_touch_date
        from candidates as c
        left join
            {{ ref("user_hubspot_touches") }} as t
            on t.user_id = c.user_id
            and date(t.touch_at) < c.race_date
        group by c.user_id
    )

select
    -- Keys and the race
    c.user_id,
    c.campaign_id,
    c.state,
    c.office,
    c.office_level,
    c.position_id,
    c.election_date,
    c.race_date,
    c.latest_stage_reached,
    c.seats_available,
    c.number_of_opponents,
    c.is_partisan,
    c.voter_count,
    c.viability_score,

    -- Who is counted, in the order the report applies them
    not c.is_staff_or_demo as is_real_user,
    c.legitimacy_label = '6. confirmed' as is_confirmed,
    c.candidacy_result in ('Won', 'Lost') as has_result,
    case
        when coalesce(r.is_contested, not c.is_uncontested)
        then 'contested'
        when not coalesce(r.is_contested, not c.is_uncontested)
        then 'uncontested'
        else 'unknown'
    end as contest_status,
    case
        when r.is_contested is not null
        then 'race'
        when c.is_uncontested is not null
        then 'journey'
    end as contest_source,
    r.baseline_win_chance,
    case
        when c.is_incumbent
        then 'incumbent'
        when not c.is_incumbent
        then 'challenger'
        else 'unknown'
    end as incumbency_status,
    c.race_date <= current_date() as is_race_past,

    -- Outcome, null until matched
    case
        c.candidacy_result when 'Won' then true when 'Lost' then false
    end as is_winner,
    c.candidacy_result = 'Lost'
    and c.latest_stage_reached like 'primary%' as is_primary_loss,

    -- How they came and what they bought
    c.registered_at,
    c.acquisition_motion = 'marketing_sourced' as is_self_signup,
    c.hs_analytics_source in ('PAID_SEARCH', 'PAID_SOCIAL') as is_paid_source,
    months_between(c.race_date, date(c.registered_at)) as months_signup_to_race,
    c.is_open_seat,
    c.is_verified,
    c.is_pro and date(c.pro_since) < c.race_date as is_pro_before_race,
    coalesce(
        date(c.pro_since) <= date_sub(c.race_date, 90), false
    ) as is_pro_3_months_out,
    c.is_activated and date(c.activated_at) < c.race_date as is_activated_before_race,
    c.is_power_user,
    date(c.onboarding_completed_at) < c.race_date as is_onboarded_before_race,

    -- Product use before the race
    p.used_onboarding_path_to_victory,
    p.used_onboarding_v2,
    p.used_plan_task,
    p.used_plan_opposition_research,
    p.used_story_rewrite,
    p.used_opponent_lookup,
    p.used_ai_help,
    p.used_content_builder,
    p.used_segment_build,
    p.used_voter_file_download,
    p.used_team_invite,
    p.used_website_publish,
    p.used_profile_complete,
    p.first_ai_help_date,
    p.first_opponent_lookup_date,
    a.active_days_last_12_weeks,

    -- Outreach before the race
    o.sends_before_race,
    o.recipients_before_race,
    1000.0
    * o.recipients_before_race
    / nullif(c.voter_count, 0) as reach_per_1000_voters,
    o.send_weeks_last_8,
    o.channels_used,
    o.did_door_knocking,
    coalesce(t.did_send_to_own_segment, false) as did_send_to_own_segment,
    coalesce(t.demographic_audiences, 0) as demographic_audiences,
    coalesce(t.demographic_messages, 0) as demographic_messages,
    coalesce(
        t.demographic_audiences >= 2 and t.demographic_messages >= 2, false
    ) as is_targeted_messaging,

    -- Our team
    tc.team_touches_before_race,
    tc.had_team_meeting,
    tc.first_team_touch_date,

    current_timestamp() as built_at
from candidates as c
left join race as r on r.user_id = c.user_id
left join product_use as p on p.user_id = c.user_id
left join outreach as o on o.user_id = c.user_id
left join targeting as t on t.user_id = c.user_id
left join activity as a on a.user_id = c.user_id
left join team_contact as tc on tc.user_id = c.user_id
