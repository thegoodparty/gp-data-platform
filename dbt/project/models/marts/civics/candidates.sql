{{
    config(
        materialized="table",
        tags=["mart", "civics", "candidate"],
    )
}}

-- Candidates: One row per unique person
-- Example: "John Smith" = 1 row regardless of how many elections they've run in
with
    source as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    candidates as (
        select
            {{
                generate_salted_uuid(
                    fields=["br_candidate_id"], salt="civics_candidate"
                )
            }} as gp_candidate_id,

            -- BallotReady identifier
            br_candidate_id,

            -- Personal info
            candidate_first_name as first_name,
            candidate_middle_name as middle_name,
            candidate_last_name as last_name,
            candidate_suffix as suffix,
            candidate_nickname as nickname,

            -- Contact info (most recent)
            candidate_email as email,
            candidate_phone as phone,
            candidate_image_url as image_url,

            -- Future integrations (NULL placeholders)
            cast(null as bigint) as product_user_id,
            cast(null as bigint) as hs_contact_id,

            -- Timestamps
            candidacy_created_at as created_at,
            candidacy_updated_at as updated_at

        from source
        where br_candidate_id is not null
        qualify
            row_number() over (
                partition by br_candidate_id order by candidacy_updated_at desc
            )
            = 1
    )

select *
from candidates
