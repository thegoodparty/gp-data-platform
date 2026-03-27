{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart candidacy schema
-- Source: stg_airbyte_source__techspeed_gdrive_candidates (preserves candidacy-stage
-- grain)
--
-- CRITICAL: UUID fields MUST match int__hubspot_companies_w_contacts_2025 pattern
-- to ensure same candidacy from different sources gets same gp_candidacy_id
with
    clean_states as (select * from {{ ref("clean_states") }}),

    source as (
        select
            ts.* except (state),
            -- Standardize state to 2-letter postal code via clean_states seed
            coalesce(cs.state_cleaned_postal_code, ts.state) as state,
            -- Add missing columns required by generate_gp_election_id macro
            cast(null as string) as seat_name,
            try_cast(number_of_seats_available as int) as seats_available,
            -- Generate candidate code inline (was provided by _clean)
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code,
            coalesce(
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM/dd/yy')
            ) as primary_election_date_parsed,
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM/dd/yy')
            ) as general_election_date_parsed,
            -- Parsed election_date for generate_gp_election_id macro
            -- HubSpot stores DATE type which becomes ISO string; parse TechSpeed to
            -- match
            -- Falls back to primary_election_date when general is NULL (e.g. early
            -- 2026 primaries before general dates are set)
            -- Note: Must duplicate parsing expression since we can't reference alias
            -- in same SELECT
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM/dd/yy'),
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM/dd/yy')
            ) as election_date,  -- parsed DATE for format parity with HubSpot
            -- Parse birth_date for UUID generation (format normalization)
            coalesce(
                try_cast(ts.birth_date as date),
                try_to_date(ts.birth_date, 'MM-dd-yyyy'),
                try_to_date(ts.birth_date, 'yyyy-MM-dd')
            ) as birth_date_parsed
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
        left join
            clean_states as cs on upper(trim(ts.state)) = upper(trim(cs.state_raw))
    ),

    candidacies as (
        select
            -- Primary identifier (vendor-agnostic)
            -- UUID fields must match HubSpot pattern for cross-source dedupe
            -- HubSpot: coalesce(field, '') for strings, DATE type for dates (becomes
            -- ISO on cast)
            -- TechSpeed: parse dates to DATE for format normalization, no coalesce on
            -- dates (NULL parity)
            -- Keep district in UUID inputs so sub-district races remain distinct.
            -- Cross-source parity drift here is expected when one source lacks
            -- district.
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "party",
                        "candidate_office",
                        "cast(coalesce(general_election_date_parsed, primary_election_date_parsed) as string)",
                        "district",
                    ]
                )
            }}
            as gp_candidacy_id,

            -- BallotReady ID: NULL today, COALESCE target when TS adds it
            cast(null as string) as br_candidacy_id,

            -- Candidate FK (matches int__civics_candidate_techspeed generation)
            -- HubSpot: raw fields, birth_date is STRING in ISO format
            -- TechSpeed: parse birth_date to DATE then cast to string for format
            -- normalization
            -- generate_salted_uuid now normalizes NULL inputs to ''
            -- before salting/hashing for consistent ID generation.
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
            cast(null as string) as hubspot_company_ids,

            -- Source tracking
            'techspeed' as candidate_id_source,
            techspeed_candidate_code as candidate_code,

            -- Candidacy attributes
            party as party_affiliation,
            case
                when upper(trim(cast(is_incumbent as string))) in ('TRUE', 'YES')
                then true
                when upper(trim(cast(is_incumbent as string))) in ('FALSE', 'NO')
                then false
                else null
            end as is_incumbent,
            case
                when upper(trim(cast(open_seat as string))) in ('YES', 'TRUE')
                then true
                when upper(trim(cast(open_seat as string))) in ('NO', 'FALSE')
                then false
                else null
            end as is_open_seat,
            candidate_office,
            official_office_name,
            office_level,

            -- Status fields
            false as is_pledged,  -- TS records are leads, not pledged candidates
            true as is_verified,  -- TS data is verified by definition
            cast(null as string) as verification_status_reason,  -- Only populated when is_verified is false
            case
                when lower(trim(cast(partisan as string))) = 'partisan'
                then true
                when lower(trim(cast(partisan as string))) = 'nonpartisan'
                then false
                else null
            end as is_partisan,

            -- Election results are tracked at the candidacy_stage level, not here
            cast(null as string) as candidacy_result,

            -- Election dates (using pre-parsed values from source CTE)
            primary_election_date_parsed as primary_election_date,
            general_election_date_parsed as general_election_date,
            cast(null as date) as primary_runoff_election_date,
            cast(null as date) as general_runoff_election_date,

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
            -- Require at least one valid parsed date for UUID generation
            -- (matches BallotReady pattern: coalesce(general, primary, runoff))
            and coalesce(general_election_date_parsed, primary_election_date_parsed)
            is not null
            -- Keep aligned with int__civics_candidate_techspeed filters so
            -- relationship tests cannot orphan gp_candidate_id references
            and first_name is not null
            and last_name is not null
            and state is not null
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
