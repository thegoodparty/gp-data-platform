-- Civics mart candidate table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
-- Deduplicates on gp_candidate_id (a person may appear in both sources)
with
    -- =========================================================================
    -- 2025 archive (pass-through)
    -- =========================================================================
    archive_2025 as (
        select
            gp_candidate_id,
            hubspot_contact_id,
            prod_db_user_id,
            candidate_id_tier,
            first_name,
            last_name,
            full_name,
            -- Parse string birth_date to DATE (2025 intermediate keeps string
            -- to preserve UUID hashing; we convert at the mart level)
            coalesce(
                try_to_date(birth_date), try_to_date(birth_date, 'M/d/yyyy')
            ) as birth_date,
            state,
            email,
            phone_number,
            street_address,
            website_url,
            linkedin_url,
            twitter_handle,
            facebook_url,
            instagram_handle,
            created_at,
            updated_at,
            array('hubspot') as source_systems
        from {{ ref("int__civics_candidate_2025") }}
    ),

    -- =========================================================================
    -- Derive candidate-level pairs from cluster membership
    -- =========================================================================
    candidate_pairs as (
        select distinct br_m.gp_candidate_id as br_id, ts_m.gp_candidate_id as ts_id
        from {{ ref("int__civics_cluster_members") }} as br_m
        inner join {{ ref("int__civics_cluster_members") }} as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    -- =========================================================================
    -- Merge 2026+ BR + TS with survivorship
    -- BR wins by default; TS wins for birth_date, phone_number, facebook_url
    -- =========================================================================
    merged_2026 as (
        select
            coalesce(br.gp_candidate_id, ts.gp_candidate_id) as gp_candidate_id,
            coalesce(
                br.hubspot_contact_id, ts.hubspot_contact_id
            ) as hubspot_contact_id,
            coalesce(br.prod_db_user_id, ts.prod_db_user_id) as prod_db_user_id,
            coalesce(br.candidate_id_tier, ts.candidate_id_tier) as candidate_id_tier,
            coalesce(br.first_name, ts.first_name) as first_name,
            coalesce(br.last_name, ts.last_name) as last_name,
            coalesce(br.full_name, ts.full_name) as full_name,
            -- TS wins for birth_date (TS: 2,885, BR: 0)
            coalesce(ts.birth_date, br.birth_date) as birth_date,
            coalesce(br.state, ts.state) as state,
            coalesce(br.email, ts.email) as email,
            -- TS wins for phone_number (TS: 48k, BR: 8k)
            coalesce(ts.phone_number, br.phone_number) as phone_number,
            coalesce(br.street_address, ts.street_address) as street_address,
            coalesce(br.website_url, ts.website_url) as website_url,
            coalesce(br.linkedin_url, ts.linkedin_url) as linkedin_url,
            coalesce(br.twitter_handle, ts.twitter_handle) as twitter_handle,
            -- TS wins for facebook_url (TS: 9,279 real)
            coalesce(ts.facebook_url, br.facebook_url) as facebook_url,
            coalesce(br.instagram_handle, ts.instagram_handle) as instagram_handle,
            coalesce(br.created_at, ts.created_at) as created_at,
            coalesce(br.updated_at, ts.updated_at) as updated_at,
            array_compact(
                array(
                    case when br.gp_candidate_id is not null then 'ballotready' end,
                    case when ts.gp_candidate_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidate_ballotready") }} as br
        full outer join candidate_pairs as cw on br.gp_candidate_id = cw.br_id
        full outer join
            {{ ref("int__civics_candidate_techspeed") }} as ts
            on cw.ts_id = ts.gp_candidate_id
    ),

    -- =========================================================================
    -- Combine archive + merged 2026+
    -- =========================================================================
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
