-- One row per run of the note summary that wrote anything: how many users it
-- summarized and the estimated tokens it spent. The summary table keeps only
-- each user's latest row, so without this a run's cost is gone once its rows
-- are rewritten.
--
-- Append-only and exempt from --full-refresh, which would erase the history.
{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        full_refresh=false,
    )
}}

select
    summarized_at as run_at,
    any_value(model_endpoint) as model_endpoint,
    count(*) as users_summarized,
    sum(input_tokens_est) as input_tokens_est,
    sum(output_tokens_est) as output_tokens_est
from {{ ref("int__user_hubspot_note_summary") }}
{% if is_incremental() %}
    where
        summarized_at
        > (select coalesce(max(run_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
group by summarized_at
