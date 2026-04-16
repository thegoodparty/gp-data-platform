-- Civics mart candidate table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
-- Deduplicates on gp_candidate_id (a person may appear in both sources)
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

with
    archive_2025 as (
        select
            gp_candidate_id,
            -- Column order must match merged_2026 (br_wins_cols, ts_wins_cols,
            -- source_systems)
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
            array('hubspot') as source_systems
        from {{ ref("int__civics_candidate_2025") }}
    ),

    -- TS int model remaps clustered rows to BR's gp_candidate_id, so a full
    -- outer join auto-merges matched pairs.
    merged_2026 as (
        select
            coalesce(br.gp_candidate_id, ts.gp_candidate_id) as gp_candidate_id,
            {% for col in br_wins_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            {% for col in ts_wins_cols %}
                coalesce(ts.{{ col }}, br.{{ col }}) as {{ col }},
            {% endfor %}
            array_compact(
                array(
                    case when br.gp_candidate_id is not null then 'ballotready' end,
                    case when ts.gp_candidate_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidate_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidate_techspeed") }} as ts
            on br.gp_candidate_id = ts.gp_candidate_id
    ),

    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_2026
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
