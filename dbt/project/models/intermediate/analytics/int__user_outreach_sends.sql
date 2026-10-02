-- Every voter-outreach send we can see, one row each, tagged with the channel
-- it went out on and the source that knows about it.
--
-- Read from two systems on purpose, because each is blind where the other
-- sees. The gp-api outreach table holds the texting volume: half a million
-- billable texts on the p2p flow, none of which reached Amplitude until the
-- terminal event landed in late September 2026. Amplitude holds the legacy
-- product-executed sends and the phone-banking sessions, whose gp-api rows
-- never had a terminal status written back and so sit at 'pending' forever.
-- Counted from either source alone the number is short by roughly the size of
-- the other: about 1,280 users against 340, overlapping on 216.
--
-- basis separates a send we know committed from one we only know was asked
-- for. The 2025-era text and robocall rows carry a script, a message and a
-- date but never a terminal status, because ops fulfilled them through an
-- external vendor that did not write back. They are real scheduled campaigns,
-- so dropping them loses most of the pre-2026 history, and calling them sends
-- asserts a delivery nobody confirmed. They get their own basis and their own
-- column downstream.
--
-- Grain: one row per send. Not unique on user_id.
{{ config(materialized="table") }}

with
    keys as (select user_id from {{ ref("int__user_resolved_keys") }}),

    -- Current-state campaign rows, one per campaign. Deliberately not the
    -- version-grain mart: an outreach row belongs to a campaign, and joining it
    -- to versions would multiply it by the version count.
    campaign_users as (
        select id as campaign_id, user_id
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}
        where not coalesce(is_demo, false)
    ),

    outreach_rows as (
        select
            cu.user_id,
            o.id as outreach_id,
            o.outreach_type,
            o.status,
            -- `date` is the scheduled send date and is what the candidate
            -- chose; createdAt is when the row appeared. Prefer the former and
            -- fall back, because the native flows carry no `date`.
            coalesce(o.date, o.createdat) as send_at,
            try_cast(o.billable_text_count as bigint) as billable_text_count
        from {{ ref("stg_airbyte_source__gp_api_db_outreach") }} as o
        join campaign_users as cu on cu.campaign_id = o.campaignid
    ),

    gp_api_sends as (
        select
            user_id,
            case
                when outreach_type in ('p2p', 'text')
                then 'texting'
                when outreach_type = 'robocall'
                then 'robocall'
                when outreach_type = 'socialMedia'
                then 'social'
                when outreach_type = 'nativeDoorKnocking'
                then 'door_knocking'
                when outreach_type = 'nativePhoneBanking'
                then 'phone_banking'
            end as channel,
            send_at,
            'gp_api_outreach' as source,
            case
                when status in ('completed', 'in_progress')
                then 'committed'
                else 'requested'
            end as basis,
            -- Only the p2p flow counts its own recipients. Null, never zero,
            -- everywhere else: a robocall to an unknown number of people is not
            -- a robocall to nobody.
            case
                when outreach_type = 'p2p' then billable_text_count
            end as recipient_count,
            outreach_id,
            'gp_api_outreach|' || cast(outreach_id as string) as send_key
        from outreach_rows
        where
            (
                outreach_type in (
                    'p2p',
                    'robocall',
                    'socialMedia',
                    'nativeDoorKnocking',
                    'nativePhoneBanking'
                )
                and status in ('completed', 'in_progress')
            )
            -- The 2025 request era. A dated row is a campaign the candidate
            -- scheduled; an undated one is a campaign-plan checklist item and
            -- is not an outreach attempt at all.
            or (
                outreach_type in ('text', 'robocall')
                and status = 'pending'
                and send_at is not null
            )
    ),

    amplitude_events as (
        select
            try_cast(user_id as bigint) as user_id,
            event_type,
            event_time,
            event_properties:method::string as outreach_method,
            event_properties:channel::string as channel_property,
            -- `medium` is the channel property going forward; `channel` is kept
            -- beside it on the send terminal until nothing reads it.
            event_properties:medium::string as medium_property,
            -- The completion event names the same id outreachCampaignId, and on
            -- a one-to-many send it is the same send as the terminal. Not on
            -- door knocking, where every turf shares its campaign's anchor id.
            coalesce(
                event_properties:outreachid::string,
                case
                    when event_properties:fanout::string = 'one-to-many'
                    then event_properties:outreachcampaignid::string
                end
            ) as outreach_id,
            -- recipientCount only. The sibling voterContacts property is the
            -- size of the audience the candidate selected, not the number of
            -- people a send reached: it is the only count the phone-banking
            -- event carries and its median there is over two thousand, which
            -- would report completing one call session as contacting a whole
            -- call list. The two properties never co-occur, so coalescing them
            -- silently mixes the two meanings into one column.
            try_cast(event_properties:recipientcount as bigint) as raw_recipient_count
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            try_cast(user_id as bigint) is not null
            and event_type in (
                'Voter Outreach - Campaign Completed',
                -- The same event under its name from 2026-09-29 (omni #2215).
                'Outreach - Campaign Completed',
                'Voter Outreach - Campaign Scheduled',
                'Outreach - Phone Banking: Complete'
            )
            -- Self-report is outreach the candidate did somewhere else.
            -- Excluded by the same qualifier the activation OKR uses, so the
            -- two can never drift apart on this question.
            and coalesce(event_properties:method::string, '') <> 'manual'
    ),

    -- One row per committed send, not per event. The v2 flows elect a single
    -- committer per outreach id, but a payment-webhook retry can still
    -- re-emit, so the first event for an id is the send and the rest are
    -- echoes.
    amplitude_deduped as (
        select *
        from amplitude_events
        qualify
            outreach_id is null
            or row_number() over (
                partition by user_id, outreach_id order by event_time asc
            )
            = 1
    ),

    amplitude_sends as (
        select
            user_id,
            case
                -- The v2 terminal names its own channel.
                when event_type = 'Voter Outreach - Campaign Scheduled'
                then
                    case
                        when
                            coalesce(medium_property, channel_property)
                            in ('text', 'sms')
                        then 'texting'
                        when coalesce(medium_property, channel_property) = 'robocall'
                        then 'robocall'
                        when
                            coalesce(medium_property, channel_property)
                            in ('socialMedia', 'social')
                        then 'social'
                        else 'unattributed'
                    end
                -- Every channel on one event since 2026-09-29, named by `medium`.
                when event_type = 'Outreach - Campaign Completed'
                then
                    case
                        when medium_property = 'text'
                        then 'texting'
                        when medium_property = 'robocall'
                        then 'robocall'
                        when medium_property = 'socialMedia'
                        then 'social'
                        when medium_property = 'doorKnocking'
                        then 'door_knocking'
                        when medium_property = 'phoneBanking'
                        then 'phone_banking'
                        else 'unattributed'
                    end
                when event_type = 'Outreach - Phone Banking: Complete'
                then 'phone_banking'
                when event_type = 'Voter Outreach - Campaign Completed'
                then
                    case
                        -- 'native' is a completed door-knocking walk.
                        when outreach_method = 'native'
                        then 'door_knocking'
                        -- A null method is the legacy product-executed send.
                        -- 'unknown' is legacy self-report, counted on the
                        -- metric owner's 2026-10-02 ruling to match the OKR
                        -- (see the leg in sem_analytics__users_win.yml).
                        -- Neither names a channel this model reads.
                        else 'unattributed'
                    end
            end as channel,
            event_time as send_at,
            'amplitude' as source,
            'committed' as basis,
            case
                when raw_recipient_count between 0 and 100000 then raw_recipient_count
            end as recipient_count,
            outreach_id,
            -- The legacy leg carries no id and fires several times in the same
            -- millisecond when a candidate commits a batch. Those are distinct
            -- sends, not echoes: the copies carry different recipient counts,
            -- 1,200 against 55,000 in one case. So the fallback key needs a
            -- discriminator, or 107 real sends collapse into 50.
            'amplitude|' || coalesce(
                outreach_id,
                cast(user_id as string)
                || '|'
                || cast(event_time as string)
                || '|'
                || cast(
                    row_number() over (
                        partition by user_id, event_time, event_type
                        order by raw_recipient_count asc nulls last
                    ) as string
                )
            ) as send_key
        from amplitude_deduped
    ),

    -- Both systems describe the same send wherever their coverage overlaps, so
    -- the product database wins: it is the one that knows the channel and the
    -- volume. Two anti-joins, because only the v2 event carries an id to match
    -- on.
    --
    -- The v2 terminal does carry one, and every id it has ever emitted matches
    -- a gp-api outreach row, so this collapses exactly.
    gp_api_ids as (
        select distinct cast(outreach_id as string) as outreach_id
        from gp_api_sends
        where outreach_id is not null
    ),

    -- The legacy events carry no id, so they fall back to same user, same day.
    -- Matched per channel where the event names one, and on user and day alone
    -- for the unattributed leg, which could have been any channel.
    gp_api_send_days as (
        select distinct user_id, date(send_at) as send_date, channel
        from gp_api_sends
        where basis = 'committed'
    ),

    amplitude_kept as (
        select a.*
        from amplitude_sends as a
        left join gp_api_ids as i on i.outreach_id = a.outreach_id
        left join
            gp_api_send_days as d
            on d.user_id = a.user_id
            and d.send_date = date(a.send_at)
            and (d.channel = a.channel or a.channel = 'unattributed')
        where i.outreach_id is null and d.user_id is null
    ),

    -- The reverse direction, for the one case the anti-join above cannot see.
    -- A 2025 request and a legacy send event on the same day are the same
    -- send: the row records what the candidate asked for and the event records
    -- the product doing it. That is 165 pairs over 72 users.
    --
    -- Here Amplitude wins, which is the opposite of the rule above, because
    -- the question is different. gp-api wins on channel and volume because it
    -- records them and Amplitude does not. Amplitude wins on whether a send
    -- actually went out, because a request whose status was never written back
    -- is silent on that and the event is not. Dropping the event instead would
    -- keep the weaker evidence and discard the confirmation.
    amplitude_confirmations as (
        select distinct user_id, date(send_at) as send_date
        from amplitude_kept
        where basis = 'committed' and channel = 'unattributed'
    ),

    gp_api_kept as (
        select g.*
        from gp_api_sends as g
        left join
            amplitude_confirmations as c
            on c.user_id = g.user_id
            and c.send_date = date(g.send_at)
        where g.basis = 'committed' or c.user_id is null
    ),

    unioned as (
        select user_id, channel, send_at, source, basis, recipient_count, send_key
        from gp_api_kept
        union all
        select user_id, channel, send_at, source, basis, recipient_count, send_key
        from amplitude_kept
    )

select u.user_id, u.channel, u.send_at, u.source, u.basis, u.recipient_count, u.send_key
from unioned as u
join keys as k using (user_id)
where u.channel is not null and u.send_at is not null
