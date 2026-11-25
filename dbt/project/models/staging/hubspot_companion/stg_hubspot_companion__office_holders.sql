-- One row per live Office Holder, a HubSpot custom object with no Lakeflow
-- table; this loader is its only path into the warehouse.
--
-- Reads one run: the loader sweeps the whole object, so a run is a complete
-- snapshot, and this reads the newest run carrying a completion marker. A run
-- that died halfway leaves the previous snapshot in force. The landing table
-- grows by the record count per run: fine at this size, worth revisiting with
-- change-only appends if the feed grows.
--
-- Live only: a merged-away record stops being returned, and its id also appears
-- in its survivor's `hs_merged_object_ids`. The second check is the guard, since
-- a sweep that began before a merge can still return the absorbed record.
--
-- The loader may not have run yet. A missing source relation compiles to an
-- empty, typed projection rather than failing the build.
{%- set landing = source("hubspot_companion", "companion_office_holders") -%}
{%- set runs = source("hubspot_companion", "companion_office_holders_runs") -%}
{%- set columns = hubspot_office_holder_columns() -%}
{%- set landed = (
    execute
    and adapter.get_relation(
        landing.database, landing.schema, landing.identifier
    )
    is not none
    and adapter.get_relation(runs.database, runs.schema, runs.identifier)
    is not none
) -%}
{%- if not landed and execute -%}
    {{
        log(
            "hubspot_companion landings are absent; "
            ~ this.name
            ~ " emits an empty Office Holder set",
            info=True,
        )
    }}
{%- endif -%}
{% if landed %}
    with
        complete_run as (
            select run_id
            from {{ runs }}
            qualify row_number() over (order by completed_at desc, run_id desc) = 1
        ),

        -- One row per record within the run, newest load first. The run id is per task
        -- attempt, so a retry is its own run; this covers the remaining case, an INSERT
        -- that the server applied before answering a 5xx and that the client retried.
        snapshot as (
            select landing_table.*
            from {{ landing }} as landing_table
            inner join complete_run on landing_table.run_id = complete_run.run_id
            qualify
                row_number() over (
                    partition by landing_table.record_id
                    order by landing_table.loaded_at desc
                )
                = 1
        ),

        listed_merges as (
            select
                record_id as listing_record_id,
                explode(split(hs_merged_object_ids, ';')) as merged_record_id
            from snapshot
            where nullif(trim(hs_merged_object_ids), '') is not null
        ),

        -- A survivor that kept its id lists itself.
        merged_away as (
            select distinct merged_record_id
            from listed_merges
            where merged_record_id not in ('', listing_record_id)
        ),

        live as (
            select *
            from snapshot
            where
                record_id not in (
                    select merged_record_id
                    from merged_away
                    where merged_record_id is not null
                )
        ),

        resolved as (
            select
                *,
                -- `app` first: a merge can land the feed's key on an app-created
                -- record.
                case
                    when nullif(trim(gp_api_elected_office_id), '') is not null
                    then 'app'
                    when nullif(trim(gp_elected_official_term_id), '') is not null
                    then 'feed'
                    else 'other'
                end as origin
            from live
        )

    select
        {%- for name, _type, expression in columns %}
            {{ expression }} as {{ name }}{{ "," if not loop.last }}
        {%- endfor %}
    from resolved
{% else %}
    select
        {%- for name, column_type, _expression in columns %}
            cast(null as {{ column_type }}) as {{ name }}{{ "," if not loop.last }}
        {%- endfor %}
    where false
{% endif %}
