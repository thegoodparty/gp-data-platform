{{ config(severity="warn", store_failures=true, schema="mart_finance") }}

-- Each row is a group of paid, live P2P sends of one script and message to one
-- phone list on one send date, so the candidate pays per copy and every voter
-- on the list gets the text per copy. gp-api refuses these at finalize; this
-- is the backstop for what slips past it, such as two drafts finalizing at the
-- same instant. Ids ascend, so the last is the newest copy, the one to cancel.
--
-- Warn, not error: rows clear only when CS acts, and an error would skip every
-- model built on outreach until then. Failures go to mart_finance for weekly
-- triage, since each row is a double charge.
--
-- The window keeps a week of already-sent groups so the weekly read sees each
-- duplicate once, to cancel or to refund, and so a group stranded by a hard
-- delete in gp-api (the stream does not capture deletes) ages out.
with
    live_p2p as (
        select id, campaignid, phone_list_id, date, script, message, status
        from {{ ref("stg_airbyte_source__gp_api_db_outreach") }}
        where
            outreach_type = 'p2p'
            and status in ('pending', 'in_progress', 'completed')
            and phone_list_id is not null
            and date >= date_sub(current_date(), 7)
    ),

    -- Script and message are grouping keys only; their text never leaves here.
    duplicate_groups as (
        select
            campaignid as campaign_id,
            phone_list_id,
            date as send_at,
            array_sort(collect_list(struct(id, status))) as outreaches
        from live_p2p
        group by campaignid, phone_list_id, date, script, message
        having count(*) > 1
    )

select
    campaign_id,
    phone_list_id,
    send_at,
    transform(outreaches, o -> o.id) as outreach_ids,
    transform(outreaches, o -> o.status) as outreach_statuses
from duplicate_groups
