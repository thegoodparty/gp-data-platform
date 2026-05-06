{{ config(materialized="table") }}

-- Civics mart candidate table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed + DDHQ.
-- Deduplicates on gp_candidate_id (a person may appear in multiple sources)
--
-- DDHQ contributes only first_name / last_name / full_name / state at the
-- candidate grain — every other column (HubSpot, contact, social) is
-- BR/TS-only. DDHQ-only candidates (no Splink match to BR/TS) appear as new
-- rows with source_systems = ['ddhq'] and BR/TS-only columns null.
{%- set br_wins_cols = [
    "hubspot_contact_id",
    "prod_db_user_id",
    "candidate_id_tier",
    "first_name",
    "last_name",
    "full_name",
    "state",
    "email",
    "street_address",
    "website_url",
    "linkedin_url",
    "twitter_handle",
    "instagram_handle",
    "created_at",
    "updated_at",
] -%}
{# TS wins: birth_date (TS: 2885, BR: 0), phone_number (TS: 48k, BR: 8k), facebook_url (TS: 9279 real) #}
{%- set ts_wins_cols = ["birth_date", "phone_number", "facebook_url"] %}
{# DDHQ-fallback columns. Must be a subset of br_wins_cols — the merge loop
   below tests `col in ddhq_fallback_cols` to decide whether to coalesce
   DDHQ in. Adding a BR-wins column that DDHQ also supplies requires
   adding it here too. state is sourced as state_postal_code (2-letter)
   to match BR/TS convention; the DDHQ int model exposes state as the
   full name. #}
{%- set ddhq_fallback_cols = [
    "first_name",
    "last_name",
    "full_name",
    "state",
    "created_at",
    "updated_at",
] %}

with
    archive_2025 as (
        select
            gp_candidate_id,
            -- Column order must match merged_since_2026 (br_wins_cols,
            -- ts_wins_cols, source_systems)
            hubspot_contact_id,
            prod_db_user_id,
            candidate_id_tier,
            first_name,
            last_name,
            full_name,
            state,
            email,
            street_address,
            website_url,
            linkedin_url,
            twitter_handle,
            instagram_handle,
            created_at,
            updated_at,
            -- TS-wins columns
            coalesce(
                try_to_date(birth_date), try_to_date(birth_date, 'M/d/yyyy')
            ) as birth_date,
            phone_number,
            facebook_url,
            array_compact(
                array('hubspot', case when has_ddhq_match then 'ddhq' end)
            ) as source_systems
        from {{ ref("int__civics_candidate_2025") }}
    ),

    -- DDHQ projected into the mart-row schema. The mart's `state` column
    -- carries 2-letter postal codes (BR/TS convention) — pull DDHQ's
    -- state_postal_code through, not its `state` (which is the human-readable
    -- name per the int_model convention).
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

    -- TS and DDHQ int models remap clustered rows to BR's gp_candidate_id, so
    -- a full outer join auto-merges matched triples. ddhq_fallback_cols above
    -- enumerates the subset of BR-wins columns DDHQ supplies; all other
    -- columns get null on DDHQ-only rows.
    merged_since_2026 as (
        select
            coalesce(
                br.gp_candidate_id, ts.gp_candidate_id, ddhq.gp_candidate_id
            ) as gp_candidate_id,
            {% for col in br_wins_cols %}
                {% if col in ddhq_fallback_cols %}
                    coalesce(br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}) as {{ col }},
                {% else %} coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
                {% endif %}
            {% endfor %}
            {% for col in ts_wins_cols %}
                coalesce(ts.{{ col }}, br.{{ col }}) as {{ col }},
            {% endfor %}
            array_compact(
                array(
                    case when br.gp_candidate_id is not null then 'ballotready' end,
                    case when ts.gp_candidate_id is not null then 'techspeed' end,
                    case when ddhq.gp_candidate_id is not null then 'ddhq' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidate_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidate_techspeed") }} as ts
            on br.gp_candidate_id = ts.gp_candidate_id
        full outer join
            ddhq
            on coalesce(br.gp_candidate_id, ts.gp_candidate_id) = ddhq.gp_candidate_id
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
