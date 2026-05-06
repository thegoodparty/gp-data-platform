{{ config(materialized="table") }}

-- Civics mart candidate table.
-- 2025 HubSpot archive UNION 2026+ 4-way FOJ over BR + TS + DDHQ + gp_api,
-- joined on gp_candidate_id (matched providers adopt BR's canonical via
-- int__civics_er_canonical_ids). Per-column precedence rules: see the
-- candidate model description in m_civics.yaml.
{%- set gp_api_wins_cols = [
    "hubspot_contact_id",
    "first_name",
    "last_name",
    "full_name",
    "state",
    "email",
    "created_at",
    "updated_at",
] -%}
{%- set br_then_ts_cols = [
    "candidate_id_tier",
    "street_address",
    "website_url",
    "linkedin_url",
    "twitter_handle",
    "instagram_handle",
] -%}
{# Subset of gp_api_wins_cols that DDHQ also supplies. The loop adds DDHQ
   to the coalesce chain only for cols listed here; cols in gp_api_wins_cols
   but NOT here render a 3-way coalesce(gp_api, br, ts) instead. state is
   sourced as state_postal_code (2-letter) to match BR/TS convention; the
   DDHQ int model exposes state as the full name. #}
{%- set ddhq_fallback_cols = [
    "first_name",
    "last_name",
    "full_name",
    "state",
    "created_at",
    "updated_at",
] %}
{# TS-wins columns: TS-leading then BR fallback for non-gp_api columns. #}
{%- set ts_wins_cols = ["birth_date", "facebook_url"] %}

with
    archive_2025 as (
        select
            gp_candidate_id,
            -- Column order must match merged_since_2026 (prod_db_user_id,
            -- gp_api_wins_cols loop order, br_then_ts_cols loop order,
            -- ts_wins_cols, phone_number, source_systems)
            prod_db_user_id,
            hubspot_contact_id,
            first_name,
            last_name,
            full_name,
            state,
            email,
            created_at,
            updated_at,
            candidate_id_tier,
            street_address,
            website_url,
            linkedin_url,
            twitter_handle,
            instagram_handle,
            coalesce(
                try_to_date(birth_date), try_to_date(birth_date, 'M/d/yyyy')
            ) as birth_date,
            facebook_url,
            phone_number,
            array_compact(
                array('hubspot', case when has_ddhq_match then 'ddhq' end)
            ) as source_systems
        from {{ ref("int__civics_candidate_2025") }}
    ),

    -- DDHQ projected into mart-row schema. The mart's `state` column
    -- carries 2-letter postal codes (BR/TS/gp_api convention) — pull DDHQ's
    -- state_postal_code through, not its `state` (human-readable).
    ddhq as (
        select
            gp_candidate_id,
            first_name,
            last_name,
            full_name,
            state_postal_code as state,
            created_at,
            updated_at
        from {{ ref("int__civics_candidate_ddhq") }}
    ),

    -- All four provider int models adopt BR's canonical gp_candidate_id via
    -- int__civics_er_canonical_ids, so a 4-way full outer join on
    -- gp_candidate_id auto-merges matched quadruples. Unmatched rows on any
    -- side pass through with NULLs on absent providers.
    merged_since_2026 as (
        select
            coalesce(
                gp_api.gp_candidate_id,
                br.gp_candidate_id,
                ts.gp_candidate_id,
                ddhq.gp_candidate_id
            ) as gp_candidate_id,
            coalesce(
                gp_api.prod_db_user_id, br.prod_db_user_id, ts.prod_db_user_id
            ) as prod_db_user_id,
            {% for col in gp_api_wins_cols %}
                {% if col in ddhq_fallback_cols %}
                    coalesce(
                        gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}
                    ) as {{ col }},
                {% else %}
                    coalesce(gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}) as {{ col }},
                {% endif %}
            {% endfor %}
            {% for col in br_then_ts_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            {% for col in ts_wins_cols %}
                coalesce(ts.{{ col }}, br.{{ col }}) as {{ col }},
            {% endfor %}
            -- phone_number: gp_api > TS > BR > DDHQ. DDHQ never carries phone,
            -- so omit it (would always be null on the right of the chain).
            coalesce(
                gp_api.phone_number, ts.phone_number, br.phone_number
            ) as phone_number,
            array_compact(
                array(
                    case when br.gp_candidate_id is not null then 'ballotready' end,
                    case when ts.gp_candidate_id is not null then 'techspeed' end,
                    case when ddhq.gp_candidate_id is not null then 'ddhq' end,
                    case when gp_api.gp_candidate_id is not null then 'gp_api' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidate_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidate_techspeed") }} as ts
            on br.gp_candidate_id = ts.gp_candidate_id
        full outer join
            ddhq
            on coalesce(br.gp_candidate_id, ts.gp_candidate_id) = ddhq.gp_candidate_id
        full outer join
            {{ ref("int__civics_candidate_gp_api") }} as gp_api
            on coalesce(br.gp_candidate_id, ts.gp_candidate_id, ddhq.gp_candidate_id)
            = gp_api.gp_candidate_id
    ),

    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_since_2026
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_candidate_id order by updated_at desc)
            = 1
    )

select
    gp_candidate_id,
    hubspot_contact_id,
    prod_db_user_id,
    candidate_id_tier,
    first_name,
    last_name,
    full_name,
    birth_date,
    state,
    email,
    phone_number,
    street_address,
    website_url,
    linkedin_url,
    twitter_handle,
    facebook_url,
    instagram_handle,
    source_systems,
    created_at,
    updated_at

from deduplicated
