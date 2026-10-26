{{ config(materialized="view") }}

/*
    The nightly AI summary of each user's HubSpot calls, notes and meetings,
    for the User lookup page in Sigma. Only the summary text and its
    bookkeeping are exposed; the notes themselves stay in the intermediate
    layer.
*/
with
    items as (
        select user_id, touch_id
        from {{ ref("int__user_hubspot_note_items") }}
        where is_summary_input
    ),

    -- Notes the summary has not read yet; it catches up on a later night.
    added_since as (
        select s.user_id, count(i.touch_id) as notes_added_since
        from {{ ref("int__user_hubspot_note_summary") }} as s
        join items as i on i.user_id = s.user_id
        where not array_contains(s.source_touch_ids, i.touch_id)
        group by s.user_id
    )

select
    s.user_id,
    s.summary,
    s.item_count,
    s.last_item_at,
    s.summarized_at,
    coalesce(a.notes_added_since, 0) as notes_added_since
from {{ ref("int__user_hubspot_note_summary") }} as s
left join added_since as a using (user_id)
