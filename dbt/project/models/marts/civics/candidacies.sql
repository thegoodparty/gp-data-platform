{{
    config(
        materialized="table",
        tags=["mart", "civics", "candidacy"],
    )
}}

-- Candidacies: One row per candidate + election (person + position + year)
-- Example: "John Smith for Seattle Mayor 2026" = 1 row
-- A Candidacy comprises a Candidate and an Election
with
    source as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    candidacies as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "br_candidate_id",
                        "br_position_id",
                        "cast(year(election_date) as string)",
                    ],
                    salt="civics_candidacy",
                )
            }} as gp_candidacy_id,

            -- FK to candidate
            {{
                generate_salted_uuid(
                    fields=["br_candidate_id"], salt="civics_candidate"
                )
            }} as gp_candidate_id,

            -- FK to election
            {{
                generate_salted_uuid(
                    fields=[
                        "br_position_id",
                        "cast(year(election_date) as string)",
                    ],
                    salt="civics_election",
                )
            }} as gp_election_id,

            -- BallotReady identifiers
            br_candidate_id,
            br_position_id,

            -- Party info
            party,
            is_independent_or_nonpartisan,
            is_major_party,

            -- BallotReady tier
            civicengine_tier,

            -- Future integrations (NULL placeholders)
            cast(null as bigint) as product_campaign_id,
            cast(null as bigint) as hs_company_id,

            -- Timestamps
            candidacy_created_at as created_at,
            candidacy_updated_at as updated_at

        from source
        where br_candidate_id is not null and br_position_id is not null
        qualify
            row_number() over (
                partition by br_candidate_id, br_position_id, year(election_date)
                order by candidacy_updated_at desc
            )
            = 1
    )

select *
from candidacies
