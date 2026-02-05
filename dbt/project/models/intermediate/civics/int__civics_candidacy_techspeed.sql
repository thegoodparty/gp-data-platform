{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart candidacy schema
-- Source: int__techspeed_candidates_clean (already deduplicated, has candidate_code)
--
-- CRITICAL: UUID fields MUST match int__hubspot_companies_w_contacts_2025 pattern
-- to ensure same candidacy from different sources gets same gp_candidacy_id
with
    source as (
        select
            * except (election_date),
            -- Add missing columns required by generate_gp_election_id macro
            cast(null as string) as seat_name,
            -- Parse election_date from multiple formats (32% of source has non-ISO
            -- formats)
            -- Without this, TRY_CAST loses ~32% of records with MM/DD/YYYY variants
            coalesce(
                try_cast(election_date as date),
                try_to_date(election_date, 'MM/dd/yyyy'),
                try_to_date(election_date, 'MM-dd-yyyy'),
                try_to_date(election_date, 'MM/dd/yy')
            ) as election_date_parsed,
            coalesce(
                try_cast(primary_election_date as date),
                try_to_date(primary_election_date, 'MM/dd/yyyy'),
                try_to_date(primary_election_date, 'MM-dd-yyyy'),
                try_to_date(primary_election_date, 'MM/dd/yy')
            ) as primary_election_date_parsed,
            coalesce(
                try_cast(general_election_date as date),
                try_to_date(general_election_date, 'MM/dd/yyyy'),
                try_to_date(general_election_date, 'MM-dd-yyyy'),
                try_to_date(general_election_date, 'MM/dd/yy')
            ) as general_election_date_parsed,
            -- Parsed election_date for generate_gp_election_id macro
            -- HubSpot stores DATE type which becomes ISO string; parse TechSpeed to
            -- match
            -- Note: Must duplicate parsing expression since we can't reference alias
            -- in same SELECT
            coalesce(
                try_cast(general_election_date as date),
                try_to_date(general_election_date, 'MM/dd/yyyy'),
                try_to_date(general_election_date, 'MM-dd-yyyy'),
                try_to_date(general_election_date, 'MM/dd/yy')
            ) as election_date,  -- parsed DATE for format parity with HubSpot
            -- Parse birth_date for UUID generation (format normalization)
            coalesce(
                try_cast(birth_date as date),
                try_to_date(birth_date, 'MM-dd-yyyy'),
                try_to_date(birth_date, 'MM/dd/yyyy'),
                try_to_date(birth_date, 'yyyy-MM-dd')
            ) as birth_date_parsed
        from {{ ref("int__techspeed_candidates_clean") }}
    ),

    candidacies as (
        select
            -- Primary identifier (vendor-agnostic)
            -- Fields aligned with int__hubspot_companies_w_contacts_2025.sql lines
            -- 37-50
            -- UUID fields must match HubSpot pattern for cross-source dedupe
            -- HubSpot: coalesce(field, '') for strings, DATE type for dates (becomes
            -- ISO on cast)
            -- TechSpeed: parse dates to DATE for format normalization, no coalesce on
            -- dates (NULL parity)
            -- TODO(DATA-1388): UUID parity mismatches observed when HubSpot has NULL
            -- district
            -- but TechSpeed has a populated district. Consider backfilling/normalizing
            -- HubSpot district values (preferred) or revisiting UUID inputs if parity
            -- is required.
            {{
                generate_salted_uuid(
                    fields=[
                        "coalesce(first_name, '')",
                        "coalesce(last_name, '')",
                        "coalesce(state, '')",
                        "coalesce(party, '')",
                        "coalesce(candidate_office, '')",
                        "cast(general_election_date_parsed as string)",
                        "coalesce(district, '')",
                    ]
                )
            }} as gp_candidacy_id,

            -- BallotReady ID: NULL today, COALESCE target when TS adds it
            cast(null as string) as ballotready_candidacy_id,

            -- Candidate FK (matches int__civics_candidate_techspeed generation)
            -- Fields aligned with int__civics_candidate_2025.sql lines 64-75
            -- HubSpot: raw fields, birth_date is STRING in ISO format
            -- TechSpeed: parse birth_date to DATE then cast to string for format
            -- normalization
            -- No coalesce - let NULLs pass through for parity with HubSpot's NULL
            -- handling
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "cast(birth_date_parsed as string)",
                        "email",
                        "phone",
                    ]
                )
            }} as gp_candidate_id,

            -- Election FK
            {{ generate_gp_election_id() }} as gp_election_id,

            -- External IDs (NULL for TS-sourced records)
            cast(null as string) as product_campaign_id,
            cast(null as string) as hubspot_contact_id,

            -- Source tracking
            'techspeed' as candidate_id_source,
            techspeed_candidate_code as candidate_code,

            -- Candidacy attributes
            party as party_affiliation,
            -- Note: civics schema uses 'Yes'/'No' strings, not booleans
            case
                when candidate_type = 'Incumbent'
                then 'Yes'
                when candidate_type = 'Challenger'
                then 'No'
                else null
            end as is_incumbent,
            case
                when upper(open_seat) in ('YES', 'TRUE')
                then 'Yes'
                when upper(open_seat) in ('NO', 'FALSE')
                then 'No'
                else null
            end as is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            -- NOTE: district and city NOT included - civics candidacy schema doesn't
            -- have them
            -- Status fields
            false as is_pledged,  -- TS records are leads, not pledged candidates
            true as is_verified,  -- TS data is verified by definition
            cast(null as string) as verification_status_reason,  -- Only populated when is_verified is false
            -- Note: civics schema uses lowercase 'partisan'/'nonpartisan' strings
            lower(partisan) as is_partisan,

            -- Election result (NULL for TS — they don't track outcomes)
            cast(null as string) as candidacy_result,

            -- Election dates (using pre-parsed values from source CTE)
            primary_election_date_parsed as primary_election_date,
            general_election_date_parsed as general_election_date,
            cast(null as date) as runoff_election_date,

            -- Assessment fields (populated downstream)
            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,

            -- Timestamps
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from source
        where
            -- Ensure we have minimum required fields for UUID generation
            techspeed_candidate_code is not null
            -- Filter on parsed date (used in UUID) - ensures valid date format
            and general_election_date_parsed is not null
    ),

    deduplicated as (
        select *
        from candidacies
        qualify
            row_number() over (
                partition by gp_candidacy_id
                order by updated_at desc, candidate_code asc nulls last
            )
            = 1
    )

select *
from deduplicated
