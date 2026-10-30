-- The audience behind each Win outreach send that has one: how narrowly it was
-- cut, whether the candidate built it, and the message sent to it. One row per
-- gp-api outreach row with a saved audience.
--
-- Social posts carry no audience, so they never appear here.
with
    campaign_users as (
        select id as campaign_id, user_id
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}
        where not coalesce(is_demo, false)
    )

select
    o.id as outreach_id,
    cu.user_id,
    o.campaignid as campaign_id,
    o.outreach_type,
    o.status,
    coalesce(o.date, o.createdat) as send_at,
    o.voter_file_filter_id,
    coalesce(o.message, o.script) as message_text,
    f.demographic_dimension_count,
    -- An untouched recommended list is ours; the candidate built the rest.
    f.recommended_intent is null or f.is_recommended_modified as is_own_audience
from {{ ref("stg_airbyte_source__gp_api_db_outreach") }} as o
inner join
    {{ ref("stg_airbyte_source__gp_api_db_voter_file_filter") }} as f
    on f.voter_file_filter_id = o.voter_file_filter_id
inner join campaign_users as cu on cu.campaign_id = o.campaignid
