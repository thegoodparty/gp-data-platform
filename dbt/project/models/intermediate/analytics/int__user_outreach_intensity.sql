-- How hard a candidate campaigned in the six months running up to their own
-- election. The outreach slice of the end-to-end user view.
--
-- Every count here is anchored on the user's election date, never on the
-- calendar. Two candidates who each sent four campaigns are not comparable if
-- one was six weeks out and the other eighteen months out, and a calendar
-- quarter mixes those together.
--
-- Counts are null, not zero, wherever the window itself is unknown. About 29k
-- of 72k users have no usable election date, and a zero there would report a
-- missing office match as a candidate who did nothing.
-- election_date_status says which case a row is.
--
-- Two names deliberately differ from the columns already in circulation.
-- users_win_base.total_campaigns_sent is a lifetime count on the Amplitude
-- union basis with no window; outreach_sent_count is windowed and routes each
-- channel to the source that knows it. They will not match and are not meant
-- to. Likewise outreach_first_sent_at is not users_win_base
-- .first_campaign_sent_at.
--
-- Table, not view: two passes over the send model plus the anchor join, read by
-- the wide journey table and by its tests.
{{ config(materialized="table") }}

with
    anchor as (select * from {{ ref("int__user_election_anchor") }}),

    sends as (select * from {{ ref("int__user_outreach_sends") }}),

    -- A window only exists for these two statuses. Everything downstream keys
    -- its nulls off this flag rather than re-testing the status string.
    -- Inner join, not left. A user with no sends must not reach the position
    -- classifier: date(null) fails the `between` as unknown and would fall
    -- through to the final branch, filing every one of the 42k users who never
    -- sent anything as having sent after their election. The counts below are
    -- zero-filled at the end instead.
    windowed as (
        select
            a.user_id,
            a.outreach_window_start,
            a.outreach_window_end,
            s.channel,
            s.basis,
            s.send_at,
            s.recipient_count,
            case
                when
                    date(s.send_at)
                    between a.outreach_window_start and a.outreach_window_end
                then 'in_window'
                when date(s.send_at) < a.outreach_window_start
                then 'before_window'
                else 'after_election'
            end as window_position
        from anchor as a
        join sends as s using (user_id)
        where a.election_date_status in ('upcoming', 'past')
    ),

    in_window as (
        select
            user_id,
            count_if(basis = 'committed') as outreach_sent_count,
            count_if(basis = 'requested') as outreach_requested_count,
            sum(
                case when basis = 'committed' then recipient_count end
            ) as outreach_recipient_count,
            count(
                distinct case when basis = 'committed' then channel end
            ) as outreach_channel_count,
            min(
                case when basis = 'committed' then send_at end
            ) as outreach_first_sent_at,
            max(
                case when basis = 'committed' then send_at end
            ) as outreach_last_sent_at,

            count_if(
                basis = 'committed' and channel = 'texting'
            ) as outreach_texting_count,
            count_if(
                basis = 'committed' and channel = 'robocall'
            ) as outreach_robocall_count,
            count_if(
                basis = 'committed' and channel = 'social'
            ) as outreach_social_count,
            count_if(
                basis = 'committed' and channel = 'door_knocking'
            ) as outreach_door_knocking_count,
            count_if(
                basis = 'committed' and channel = 'phone_banking'
            ) as outreach_phone_banking_count,
            -- The legacy product-executed send, which fired without naming its
            -- channel. Most of the 2025 history lands here, so it is published
            -- rather than folded into a channel it might not belong to.
            count_if(
                basis = 'committed' and channel = 'unattributed'
            ) as outreach_unattributed_count
        from windowed
        where window_position = 'in_window'
        group by user_id
    ),

    -- Sends the window excludes, kept visible. A send after the election is
    -- usually the anchor having picked the cycle the candidate has already
    -- moved past, so a non-zero count here is a signal about the anchor, not
    -- about the candidate.
    outside_window as (
        select
            user_id,
            count_if(window_position = 'before_window') as outreach_before_window_count,
            count_if(
                window_position = 'after_election'
            ) as outreach_after_election_count
        from windowed
        where window_position in ('before_window', 'after_election')
        group by user_id
    )

select
    a.user_id,

    a.election_date,
    a.election_date_status,
    a.outreach_window_start,
    a.outreach_window_end,
    a.outreach_window_is_open,
    a.outreach_window_days_total,
    a.outreach_window_days_elapsed,

    -- Zero where we can see the window and nothing happened in it, null where
    -- there is no window to look at.
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_sent_count, 0)
    end as outreach_sent_count,
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_requested_count, 0)
    end as outreach_requested_count,

    -- Left null rather than zeroed even inside a window: only the p2p texting
    -- flow and some Amplitude legs count their recipients, so a zero would
    -- claim a send reached nobody when we simply do not know how many.
    w.outreach_recipient_count,

    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_channel_count, 0)
    end as outreach_channel_count,

    w.outreach_first_sent_at,
    w.outreach_last_sent_at,

    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_texting_count, 0)
    end as outreach_texting_count,
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_robocall_count, 0)
    end as outreach_robocall_count,
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_social_count, 0)
    end as outreach_social_count,
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_door_knocking_count, 0)
    end as outreach_door_knocking_count,
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_phone_banking_count, 0)
    end as outreach_phone_banking_count,
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(w.outreach_unattributed_count, 0)
    end as outreach_unattributed_count,

    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(o.outreach_before_window_count, 0)
    end as outreach_before_window_count,
    case
        when a.election_date_status in ('upcoming', 'past')
        then coalesce(o.outreach_after_election_count, 0)
    end as outreach_after_election_count
from anchor as a
left join in_window as w using (user_id)
left join outside_window as o using (user_id)
