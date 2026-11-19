{{ config(materialized="ephemeral") }}

-- Ephemeral: this carries per-person usage, and the `dbt` schema grants
-- SELECT to every data user.
--
-- One row per request id, the newest observation of it, since each export
-- re-emits a trailing window. Status sorts before `exported_at` because with
-- the clock first a later in-flight row wins and the request loses its cost.
-- `loaded_at` cannot order the two: it is the same for every row of one file.
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
            -- A model-list call lands a blank model, which a not-null test
            -- would pass while the mart grouped it as a real one.
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

            -- The gateway user wins, so it takes over with no re-export; else
            -- the key name's email, extracted so a suffixed or re-minted key is
            -- still one person. nullif wraps the extract because it returns ''
            -- on no match, which would pass every null check as a person.
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
