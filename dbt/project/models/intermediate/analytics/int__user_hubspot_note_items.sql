-- One row per user per HubSpot call, note or meeting that carries text a
-- reader would want: the input to the per-user note summary, and the source
-- items shown beside it.
--
-- Table, not view: it joins the touch history to both engagement streams and
-- is read twice by the summary model plus its deletion hook.
{{ config(materialized="table") }}

with
    touches as (
        select user_id, touch_id, channel, touch_at
        from {{ ref("int__user_hubspot_touches") }}
        where channel in ('call', 'note', 'meeting')
    ),

    -- Dialer boilerplate carries no content of its own: the sequence header,
    -- the "[NOOKS] -" prefix, the "recorded by <rep> on <date>" stamp and the
    -- AI-summary banner. A row that is only these is a stub.
    calls as (
        select
            cast(id as string) as touch_id,
            startswith(outcome_family, 'connected_') as is_connected,
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(coalesce(hs_call_body, ''), '<[^>]+>', ' '),
                            '(?i)\\[nooks\\] -|\\[(nooks|sequence)[^\\]]*\\]',
                            ''
                        ),
                        '(?i)\\[[^\\]]*\\]\\s*(\'[^\']*\'\\s*)?recorded by .*? on \\d{4}-\\d{2}-\\d{2}\\S*',
                        ''
                    ),
                    '(?i)call notes:|-+nooks ai summary.*?-+',
                    ''
                )
            ) as rep_text,
            trim(
                regexp_replace(coalesce(hs_call_summary, ''), '<[^>]+>', ' ')
            ) as summary_text
        from {{ ref("int__hubspot_calls") }}
    ),

    -- A meeting's body is the calendar invite (nine in ten match an invite
    -- template), so only the rep's internal notes say what happened. Grain
    -- write-ups arrive as notes, not meetings.
    engagements as (
        select
            id as touch_id,
            trim(
                regexp_replace(
                    case
                        engagement_type
                        when 'MEETING'
                        then internal_meeting_notes
                        else body
                    end,
                    '<[^>]+>',
                    ' '
                )
            ) as item_text
        from {{ ref("stg_airbyte_source__hubspot_api_engagements") }}
        where engagement_type in ('NOTE', 'MEETING')
    ),

    joined as (
        select
            t.user_id,
            t.touch_id,
            t.channel,
            t.touch_at,
            case
                when t.channel = 'call'
                -- On a call that never connected, HubSpot's summary describes
                -- the voicemail, so only what the rep typed is kept.
                then
                    case
                        when c.is_connected
                        then
                            concat_ws(
                                '\n', nullif(c.rep_text, ''), nullif(c.summary_text, '')
                            )
                        else c.rep_text
                    end
                else e.item_text
            end as item_text
        from touches as t
        left join calls as c on t.channel = 'call' and c.touch_id = t.touch_id
        left join engagements as e on t.channel <> 'call' and e.touch_id = t.touch_id
    ),

    -- One long Grain write-up should not crowd out every other item.
    capped as (
        select
            user_id,
            touch_id,
            channel,
            touch_at,
            left(
                trim(
                    regexp_replace(
                        regexp_replace(item_text, '&[a-zA-Z]+;|&#[0-9]+;', ' '),
                        '\\s+',
                        ' '
                    )
                ),
                4000
            ) as item_text
        from joined
    ),

    final as (
        select
            *,
            row_number() over (
                partition by user_id order by touch_at desc, touch_id
            ) as recency_rank,
            sum(length(item_text)) over (
                partition by user_id
                order by touch_at desc, touch_id
                rows between unbounded preceding and current row
            ) as cumulative_chars
        from capped
        -- Under 40 characters is a disposition word or "Left VM", not a note.
        where length(item_text) >= 40
    )

select
    user_id,
    touch_id,
    channel,
    touch_at,
    item_text,
    recency_rank,
    -- Bounds one user's prompt to about 10k tokens; the newest item always goes in.
    recency_rank = 1
    or (recency_rank <= 20 and cumulative_chars <= 40000) as is_summary_input
from final
