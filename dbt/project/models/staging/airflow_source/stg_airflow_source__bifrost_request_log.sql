{{ config(materialized="ephemeral") }}

-- Ephemeral, not a view: this carries per-person usage, and the `dbt` schema
-- grants SELECT to every data user. As a CTE the person column never
-- materializes outside mart_finance, the only schema this grain may reach.
--
-- One row per request id. The landing table is append-only and each export
-- re-emits a trailing window, so a request arrives more than once: first as
-- `processing`, later as its terminal self with the cost. The status key has
-- to come first: with the clock first, a `processing` row exported later than
-- the terminal one wins, and the request contributes no cost at all.
--
-- `exported_at`, not `loaded_at`, orders the two: `loaded_at` is identical for
-- every row of one file, so it cannot separate observations from two files.
with

    current_rows as (
        select *
        from {{ source("airflow_source", "bifrost_request_log") }}
        qualify
            row_number() over (
                partition by id
                order by
                    case when status = 'processing' then 1 else 0 end,
                    exported_at desc,
                    id
            )
            = 1
    ),

    renamed as (

        select
            id as request_id,
            `timestamp` as request_at,
            created_at,
            exported_at,
            provider,
            -- A model-list call lands an empty string here, never null, so an unguarded
            -- `not null` test would pass while the mart grouped the blank as a model.
            nullif(model, '') as model,
            canonical_model_name,
            `alias`,
            object_type,
            status,
            is_passthrough,
            fallback_index,
            parent_request_id,
            app,
            user_agent,
            virtual_key_id,
            virtual_key_name,
            team_id,
            team_name,
            user_id,
            user_name,

            -- Person = the gateway identity if there is one, else the email the key
            -- name
            -- carries. Extract rather than match whole: a key name can carry a
            -- suffix, and a
            -- second key or a re-minted one has to resolve to the same person.
            --
            -- regexp_extract returns '' on no match, not null, so the outer nullif is
            -- what
            -- keeps an unparseable key name from becoming an empty-string person and
            -- slipping
            -- past every null check downstream.
            coalesce(
                nullif(lower(trim(user_name)), ''),
                nullif(
                    regexp_extract(
                        lower(virtual_key_name), '([a-z0-9._%+-]+@goodparty[.]org)', 1
                    ),
                    ''
                )
            ) as person_email,

            cost as cost_usd,
            input_cost as input_cost_usd,
            output_cost as output_cost_usd,
            additional_cost as additional_cost_usd,

            prompt_tokens,
            completion_tokens,
            total_tokens,
            cached_read_tokens,
            latency as latency_ms,

            loaded_at

        from current_rows

    )

select *
from renamed
