{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart candidacy schema
-- Source: stg_airbyte_source__techspeed_gdrive_candidates (preserves candidacy-stage
-- grain)
--
-- CRITICAL: UUID fields MUST match int__hubspot_companies_w_contacts_2025 pattern
-- to ensure same candidacy from different sources gets same gp_candidacy_id.
--
-- BR enrichment via br_race_id surfaces br_position_database_id, additional
-- election dates, and BR's gp_election_id for TS-only candidacies whose
-- br_race_id matches a BR election. The original TS *_election_date_parsed
-- columns are preserved unchanged for ID generation (so existing UUIDs stay
-- stable); coalesce(ts, br) is applied at the output / WHERE-clause layer.
with
    br_race_to_position as ({{ br_race_to_position_lookup() }}),

    -- Pivot all stages of a BR-side election into per-stage_type dates so we
    -- can fill in missing TS form dates (TS captures only primary or general,
    -- never runoffs).
    br_election_dates as (
        select
            gp_election_id as br_gp_election_id,
            max(
                case when stage_type = 'primary' then election_date end
            ) as br_primary_election_date,
            max(
                case when stage_type = 'general' then election_date end
            ) as br_general_election_date,
            max(
                case when stage_type = 'primary runoff' then election_date end
            ) as br_primary_runoff_election_date,
            max(
                case when stage_type = 'general runoff' then election_date end
            ) as br_general_runoff_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
        group by gp_election_id
    ),

    source as (
        select
            ts.* except (
                state, primary_election_date_parsed, general_election_date_parsed
            ),
            -- Alias for generate_gp_election_id macro compatibility
            ts.state_postal_code as state,
            cast(null as string) as seat_name,
            -- ID-generation macros (generate_ts_gp_candidacy_id, etc.) hash
            -- off these *_parsed columns. To keep UUIDs stable for existing
            -- rows, ONLY substitute BR fallback dates when the TS row has
            -- neither a primary nor a general parsed date — otherwise pass
            -- the TS values through unchanged. Rows with at least one TS
            -- date keep their original hash; rows with neither (previously
            -- filtered out) get a deterministic BR-derived hash.
            case
                when
                    ts.primary_election_date_parsed is not null
                    or ts.general_election_date_parsed is not null
                then ts.primary_election_date_parsed
                else bed.br_primary_election_date
            end as primary_election_date_parsed,
            case
                when
                    ts.primary_election_date_parsed is not null
                    or ts.general_election_date_parsed is not null
                then ts.general_election_date_parsed
                else bed.br_general_election_date
            end as general_election_date_parsed,
            -- BR enrichment columns surfaced separately from the substitution
            -- above so the candidacies CTE can apply coalesce(ts, br) for the
            -- final output dates without affecting hashing.
            brp.br_position_database_id,
            brp.br_gp_election_id,
            bed.br_primary_election_date,
            bed.br_general_election_date,
            bed.br_primary_runoff_election_date,
            bed.br_general_runoff_election_date,
            -- Generate candidate code inline (was provided by _clean)
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
        left join br_race_to_position as brp on ts.br_race_id = brp.br_race_id
        left join
            br_election_dates as bed on brp.br_gp_election_id = bed.br_gp_election_id
    ),

    -- ER crosswalk: for clustered TS candidacies, adopt BR's canonical gp_* IDs.
    -- Deduped to one row per source_candidate_id — all stages of a matched
    -- candidacy share the same BR candidacy/candidate/election IDs.
    canonical_candidacy as (
        select
            ts_source_candidate_id,
            canonical_gp_candidacy_id,
            canonical_gp_candidate_id,
            canonical_gp_election_id
        from {{ ref("int__civics_er_canonical_ids") }}
        qualify
            row_number() over (
                partition by ts_source_candidate_id order by canonical_gp_candidacy_id
            )
            = 1
    ),

    candidacies as (
        -- gp_candidate_id and gp_election_id use WINDOW propagation so all raw
        -- rows of the same person/election adopt BR's canonical if ANY candidacy
        -- of that person/election was matched — matching the propagation used
        -- in int__civics_candidate_techspeed and int__civics_election_techspeed
        -- (required for FK relationships to hold).
        -- gp_candidacy_id is per-row (each candidacy matches its own xw entry).
        --
        -- gp_election_id falls back to source.br_gp_election_id (when TS's
        -- br_race_id matches a BR election) before the TS-derived hash, so
        -- TS-only candidacies that share a BR election adopt the BR-side id.
        -- Mirrored in int__civics_election_techspeed and
        -- int__civics_election_stage_techspeed for end-to-end alignment.
        select
            coalesce(
                xw.canonical_gp_candidacy_id, {{ generate_ts_gp_candidacy_id() }}
            ) as gp_candidacy_id,

            cast(null as string) as br_candidacy_id,

            coalesce(
                max(xw.canonical_gp_candidate_id) over (
                    partition by {{ generate_ts_gp_candidate_id() }}
                ),
                {{ generate_ts_gp_candidate_id() }}
            ) as gp_candidate_id,

            coalesce(
                max(xw.canonical_gp_election_id) over (
                    partition by {{ generate_gp_election_id() }}
                ),
                source.br_gp_election_id,
                {{ generate_gp_election_id() }}
            ) as gp_election_id,

            cast(null as string) as product_campaign_id,
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as hubspot_company_ids,

            'techspeed' as candidate_id_source,
            techspeed_candidate_code as candidate_code,

            party as party_affiliation,
            is_incumbent,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,

            false as is_pledged,
            true as is_verified,
            cast(null as string) as verification_status_reason,
            is_partisan,

            -- Election results are tracked at the candidacy_stage level, not here
            cast(null as string) as candidacy_result,

            -- Output dates coalesce TS form data with BR election_stage data.
            -- Order is TS-first to preserve TS form values when present.
            coalesce(
                primary_election_date_parsed, source.br_primary_election_date
            ) as primary_election_date,
            coalesce(
                general_election_date_parsed, source.br_general_election_date
            ) as general_election_date,
            source.br_primary_runoff_election_date as primary_runoff_election_date,
            source.br_general_runoff_election_date as general_runoff_election_date,

            source.br_position_database_id,

            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,

            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from source
        left join
            canonical_candidacy as xw
            on source.techspeed_candidate_code = xw.ts_source_candidate_id
        where
            techspeed_candidate_code is not null
            -- After the source CTE's BR fallback substitution, this passes
            -- rows that had EITHER a TS date OR a BR-resolved date.
            and coalesce(general_election_date_parsed, primary_election_date_parsed)
            is not null
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
