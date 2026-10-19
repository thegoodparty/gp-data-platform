-- One row per user: a short model-written summary of their HubSpot calls,
-- notes and meetings, for the user lookup page.
--
-- Each run summarizes only users who are new or whose input changed, newest
-- activity first, up to a cap; the rest carry to the next run. Several jobs
-- build this model through the person lineage, so a run writes only once 20
-- hours have passed since the last write, which makes it about daily. Exempt from
-- --full-refresh because the on-merge and CI jobs pass it, and a rebuild would
-- pay to re-summarize every user. To backfill, raise the cap for one watched
-- run: --vars '{hubspot_note_summary_user_cap: 25000}'. Passing the cap
-- explicitly also skips the 20-hour wait.
--
-- The post-hook deletes a summary once any item it was written from leaves
-- the item model, so an erased engagement does not live on in the summary.
{%- set is_ci = env_var("DBT_CLOUD_PR_ID", "") != "" -%}
{%- set default_cap = 2000 if target.name == "prod" and not is_ci else 25 -%}
{%- set explicit_cap = var("hubspot_note_summary_user_cap", none) -%}
{%- set user_cap = (default_cap if explicit_cap is none else explicit_cap) | int -%}
{%- set endpoint = var(
    "hubspot_note_summary_endpoint", "databricks-claude-haiku-4-5"
) -%}
-- Bump when the prompt changes, so every summary is rewritten under the new one.
{%- set prompt_version = "v2" -%}
{%- set prompt -%}
You summarize a candidate's history with GoodParty.org staff for an internal account page. Below are HubSpot call summaries, call notes, meeting notes and notes, oldest first, each tagged with its date and type.
Write at most 4 bullet points of no more than 20 words each, in plain English. Cover what office and election they are running for, where their campaign stands, what they asked for or need, which GoodParty.org products or plans came up and how they reacted, and any commitments or next steps with dates.
Leave out anything about health, family members, personal finances, legal matters, religion or personal political views, and leave out phone numbers, email addresses and street addresses. Never mention the candidate's relatives, even as campaign help. Only state what the notes say. If they hold nothing campaign-related, write the single bullet "No campaign details recorded." and never explain why or ask for more notes.
End with one line starting "Last contact:" giving the date and outcome of the most recent item. Output only the bullets and that line.

Notes:
{% endset -%}
{%- set prompt_literal = "'" ~ prompt | replace("'", "\\'") ~ "'" -%}

{{
    config(
        materialized="incremental",
        unique_key="user_id",
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        full_refresh=false,
        post_hook=["""
            delete from {{ this }}
            where user_id in (
                with current_items as (
                    select user_id, collect_set(touch_id) as touch_ids
                    from {{ ref('int__user_hubspot_note_items') }}
                    group by user_id
                )
                select s.user_id
                from {{ this }} as s
                left join current_items as c on c.user_id = s.user_id
                where c.user_id is null
                    or size(array_except(s.source_touch_ids, c.touch_ids)) > 0
            )
            """],
    )
}}

with
    inputs as (
        select
            user_id,
            count(*) as item_count,
            max(touch_at) as last_item_at,
            collect_list(touch_id) as source_touch_ids,
            array_join(
                transform(
                    array_sort(
                        collect_list(
                            struct(
                                touch_at,
                                touch_id,
                                concat(
                                    '[',
                                    date_format(touch_at, 'yyyy-MM-dd'),
                                    ' ',
                                    channel,
                                    '] ',
                                    item_text
                                ) as line
                            )
                        )
                    ),
                    x -> x.line
                ),
                '\n\n'
            ) as input_text
        from {{ ref("int__user_hubspot_note_items") }}
        where is_summary_input
        group by user_id
    ),

    fingerprinted as (
        select
            *,
            sha2(
                concat_ws('|', '{{ prompt_version }}', '{{ endpoint }}', input_text),
                256
            ) as input_fingerprint
        from inputs
    ),

    pending as (
        select f.*
        from fingerprinted as f
        {% if is_incremental() %}
            left join {{ this }} as s on s.user_id = f.user_id
            where s.user_id is null or s.input_fingerprint <> f.input_fingerprint
        {% endif %}
    ),

    {% if is_incremental() and explicit_cap is none %}
        last_write as (
            select coalesce(max(summarized_at), timestamp '1900-01-01') as last_at
            from {{ this }}
        ),
    {% endif %}

    batch as (
        select p.*
        from pending as p
        {% if is_incremental() and explicit_cap is none %}
            cross join last_write as w
            where w.last_at < current_timestamp() - interval 20 hours
        {% endif %}
        order by p.last_item_at desc, p.user_id
        limit {{ user_cap }}
    ),

    summarized as (
        select
            *,
            concat({{ prompt_literal }}, input_text) as prompt_text,
            ai_query(
                '{{ endpoint }}',
                prompt_text,
                modelparameters => named_struct('max_tokens', 300, 'temperature', 0.0),
                failonerror => false
            ) as response
        from batch
    )

-- A failed call writes no row, so the user is retried on the next run.
select
    user_id,
    trim(response.result) as summary,
    item_count,
    last_item_at,
    source_touch_ids,
    input_fingerprint,
    '{{ endpoint }}' as model_endpoint,
    '{{ prompt_version }}' as prompt_version,
    -- Estimates at four characters a token: ai_query returns no usage counts
    -- and the serving usage tables are not readable from here.
    ceil(length(prompt_text) / 4) as input_tokens_est,
    ceil(length(response.result) / 4) as output_tokens_est,
    current_timestamp() as summarized_at
from summarized
where response.errormessage is null and nullif(trim(response.result), '') is not null
