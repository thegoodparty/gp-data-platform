-- TechSpeed candidates → Civics mart election schema.
-- Grain: one row per election (position + election year).
with
    br_race_to_position as ({{ br_race_to_position_lookup() }}),

    br_election_dates as (
        select
            gp_election_id as br_gp_election_id,
            max(
                case when stage_type = 'primary' then election_date end
            ) as br_primary_election_date,
            max(
                case when stage_type = 'general' then election_date end
            ) as br_general_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
        group by gp_election_id
    ),

    -- ER crosswalk: for clustered TS candidacies, adopt BR's canonical election_id.
    -- Deduped per source_candidate_id; any match for this candidacy gives BR's id.
    canonical_election as (
        -- BR-priority ordering — see int__civics_candidacy_techspeed.sql
        -- for full rationale. canonical_gp_election_id is NULL for non-BR cluster
        -- rows and populated for BR-anchored rows, so `is null` puts BR-anchored
        -- rows first; ties tiebreak on the existing UUID order.
        select ts_source_candidate_id, canonical_gp_election_id
        from {{ ref("int__civics_er_canonical_ids") }}
        qualify
            row_number() over (
                partition by ts_source_candidate_id
                order by canonical_gp_election_id is null, canonical_gp_election_id
            )
            = 1
    ),

    source as (
        select
            ts.* except (
                state, primary_election_date_parsed, general_election_date_parsed
            ),
            -- Aliases for generate_gp_election_id macro compatibility
            ts.state_postal_code as state,
            cast(null as string) as seat_name,
            -- Mirror the conditional BR-fallback substitution used in
            -- int__civics_candidacy_techspeed: only fill in dates when TS has
            -- neither, so the representative-row pick for partitions with at
            -- least one TS date stays unchanged.
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
            brp.br_gp_election_id,
            brp.br_position_database_id,
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

    with_election_id as (
        -- If ANY candidacy in an election was clustered to BR, all rows of that
        -- election adopt BR's canonical_gp_election_id (propagated via a window
        -- partitioned by the raw TS hash). Otherwise prefer the BR-derived id
        -- from br_race_id, then fall back to the TS-derived hash.
        select
            coalesce(
                max(xw.canonical_gp_election_id) over (
                    partition by {{ generate_gp_election_id() }}
                ),
                source.br_gp_election_id,
                {{ generate_gp_election_id() }}
            ) as gp_election_id,
            source.*
        from source
        left join
            canonical_election as xw
            on source.techspeed_candidate_code = xw.ts_source_candidate_id
        where election_date is not null and year(election_date) between 1900 and 2050
    ),

    -- Pick one representative row per election. Prefer rows with a general
    -- election date (more complete data), then break ties with latest extraction.
    representative_row as (
        select *
        from with_election_id
        qualify
            row_number() over (
                partition by gp_election_id
                order by
                    case
                        when general_election_date_parsed is not null then 0 else 1
                    end,
                    _airbyte_extracted_at desc,
                    first_name,
                    last_name
            )
            = 1
    ),

    elections as (
        select
            gp_election_id,
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            state,
            city,
            district,
            seat_name,
            election_date,
            year(election_date) as election_year,
            filing_deadline_parsed as filing_deadline,
            population,
            seats_available,
            cast(null as date) as term_start_date,
            -- Keep boolean type to match BallotReady election intermediate
            is_uncontested,
            -- carry the TechSpeed opponent count instead of
            -- hardcoding null. number_of_candidates is the count of candidates
            -- in the race from the TechSpeed source; opponents = candidates - 1.
            -- Kept as a string to match the BR/DDHQ/2025 election models and the
            -- scorer's string handling (int__civics_viability_scoring). Empty,
            -- non-numeric, and < 1 counts fall to null (a race has >= 1
            -- candidate). The representative-row pick already collapses to one
            -- row per election, so this is per-election with no fan-out.
            cast(
                case
                    when try_cast(number_of_candidates as int) >= 1
                    then try_cast(number_of_candidates as int) - 1
                end as string
            ) as number_of_opponents,
            is_open_seat,
            false as has_ddhq_match,
            -- BR-derived from br_race_id when TS captures one. Surfaces the
            -- position id for TS-only elections so candidacy↔election
            -- alignment holds end-to-end through the mart's coalesce.
            br_position_database_id,
            cast(null as boolean) as is_judicial,
            cast(null as boolean) as is_appointed,
            cast(null as string) as br_normalized_position_type,
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at
        from representative_row
    )

select *
from elections
