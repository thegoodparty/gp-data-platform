-- DDHQ election results → Civics mart candidacy_stage (base model)
-- Source: stg_airbyte_source__ddhq_gdrive_election_results
--
-- Grain: One row per candidacy stage (candidate + race), 1:1 with staging rows.
--
-- This is the foundational DDHQ intermediate model. It computes all five
-- GP IDs and carries through all staging columns so the other four DDHQ
-- models (candidate, candidacy, election_stage, election) can derive from it
-- without re-reading the staging table.
with
    source as (
        select *
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where
            election_date >= '2026-01-01'
            and ddhq_race_id is not null
            and candidate_id is not null
            and candidate_first_name is not null
            and candidate_last_name is not null
            and state_postal_code is not null
    ),

    -- General election date lookup: find the general-stage election date per
    -- position+year so primary/runoff stages get the same gp_election_id.
    general_election_dates as (
        select
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            state_postal_code as state,
            district,
            year(election_date) as election_year,
            max(election_date) as general_election_date,
            any_value(number_of_seats_in_election) as general_seats
        from source
        where election_stage = 'general'
        group by
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            state_postal_code,
            district,
            year(election_date)
    ),

    -- Candidacy-level date lookup: coalesce general > primary > runoff dates
    -- per candidate+position+year for stable gp_candidacy_id generation.
    candidacy_dates as (
        select
            candidate_first_name,
            candidate_last_name,
            state_postal_code,
            party_affiliation,
            candidate_office,
            official_office_name,
            office_level,
            district,
            year(election_date) as election_year,
            max(
                case when election_stage = 'general' then election_date end
            ) as general_date,
            max(
                case when election_stage = 'primary' then election_date end
            ) as primary_date,
            max(
                case when election_stage = 'general runoff' then election_date end
            ) as general_runoff_date,
            max(
                case when election_stage = 'primary runoff' then election_date end
            ) as primary_runoff_date
        from source
        group by
            candidate_first_name,
            candidate_last_name,
            state_postal_code,
            party_affiliation,
            candidate_office,
            official_office_name,
            office_level,
            district,
            year(election_date)
    ),

    -- ER crosswalk: when Splink clustered this DDHQ row with a BR row, adopt
    -- BR's canonical gp_* IDs so the mart's full outer join collapses
    -- matched DDHQ + BR/TS pairs onto the same key. Pre-restricted to DDHQ
    -- rows so we keep this lookup small.
    canonical_ids as (
        select
            ddhq_candidate_id,
            ddhq_race_id,
            canonical_gp_candidacy_stage_id,
            canonical_gp_election_stage_id,
            canonical_gp_candidacy_id,
            canonical_gp_candidate_id,
            canonical_gp_election_id
        from {{ ref("int__civics_er_canonical_ids") }}
        where ddhq_candidate_id is not null and ddhq_race_id is not null
    ),

    with_ids as (
        select
            -- === Computed IDs (canonical from BR if Splink-matched, else hashed) ===
            -- Lateral column refs let us derive gp_candidacy_stage_id from the
            -- same-row gp_candidacy_id / gp_election_stage_id without a chained CTE.
            coalesce(
                xw.canonical_gp_candidate_id,
                {{
                    generate_salted_uuid(
                        fields=[
                            "s.candidate_first_name",
                            "s.candidate_last_name",
                            "s.state_postal_code",
                            "cast(null as string)",
                            "cast(null as string)",
                            "cast(null as string)",
                        ]
                    )
                }}
            ) as gp_candidate_id,

            coalesce(
                xw.canonical_gp_candidacy_id,
                {{
                    generate_salted_uuid(
                        fields=[
                            "s.candidate_first_name",
                            "s.candidate_last_name",
                            "s.state_postal_code",
                            "s.party_affiliation",
                            "s.candidate_office",
                            "cast(coalesce(cd.general_date, cd.primary_date, cd.general_runoff_date, cd.primary_runoff_date) as string)",
                            "s.district",
                        ]
                    )
                }}
            ) as gp_candidacy_id,

            coalesce(
                xw.canonical_gp_election_stage_id,
                {{ generate_salted_uuid(fields=["cast(s.ddhq_race_id as string)"]) }}
            ) as gp_election_stage_id,

            coalesce(
                xw.canonical_gp_election_id,
                {{ generate_gp_election_id("elec_lookup") }}
            ) as gp_election_id,

            coalesce(
                xw.canonical_gp_candidacy_stage_id,
                {{
                    generate_salted_uuid(
                        fields=["gp_candidacy_id", "gp_election_stage_id"]
                    )
                }}
            ) as gp_candidacy_stage_id,

            -- === Staging passthrough ===
            s.candidate as candidate_full_name,
            s.candidate_first_name,
            s.candidate_last_name,
            cast(s.candidate_id as string) as source_candidate_id,
            cast(s.ddhq_race_id as string) as source_race_id,
            s.ddhq_race_id,
            s.state,
            s.state_postal_code,
            s.party_affiliation,
            s.candidate_office,
            s.official_office_name,
            s.office_level,
            s.office_type,
            s.district,
            s.election_stage,
            s.election_date,
            s.race_name,
            s.is_winner,
            s.is_uncontested,
            s.votes,
            s.number_of_seats_in_election,
            s.total_number_of_ballots_in_race,
            s._airbyte_extracted_at,

            -- === Candidacy-level dates (for candidacy rollup) ===
            cd.general_date as candidacy_general_date,
            cd.primary_date as candidacy_primary_date,
            cd.general_runoff_date as candidacy_general_runoff_date,
            cd.primary_runoff_date as candidacy_primary_runoff_date,

            -- === Candidacy-stage-specific derived columns ===
            case
                when s.is_winner
                then 'Won'
                when s.is_winner = false
                then 'Lost'
                else null
            end as election_result,
            case
                when s.is_winner is not null then 'ddhq' else null
            end as election_result_source

        from source as s
        inner join
            candidacy_dates as cd
            on s.candidate_first_name <=> cd.candidate_first_name
            and s.candidate_last_name <=> cd.candidate_last_name
            and s.state_postal_code <=> cd.state_postal_code
            and s.party_affiliation <=> cd.party_affiliation
            and s.candidate_office <=> cd.candidate_office
            and s.official_office_name <=> cd.official_office_name
            and s.office_level <=> cd.office_level
            and s.district <=> cd.district
            and year(s.election_date) = cd.election_year
        left join
            general_election_dates as ged
            on s.official_office_name <=> ged.official_office_name
            and s.candidate_office <=> ged.candidate_office
            and s.office_level <=> ged.office_level
            and s.office_type <=> ged.office_type
            and s.state_postal_code <=> ged.state
            and s.district <=> ged.district
            and year(s.election_date) = ged.election_year
        cross join
            lateral(
                select
                    s.official_office_name,
                    s.candidate_office,
                    s.office_level,
                    s.office_type,
                    s.state_postal_code as state,
                    cast(null as string) as city,
                    s.district,
                    cast(null as string) as seat_name,
                    coalesce(
                        ged.general_election_date, s.election_date
                    ) as election_date,
                    coalesce(
                        ged.general_seats, s.number_of_seats_in_election
                    ) as seats_available
            ) as elec_lookup
        left join
            canonical_ids as xw
            on cast(s.candidate_id as bigint) = xw.ddhq_candidate_id
            and cast(s.ddhq_race_id as bigint) = xw.ddhq_race_id
    ),

    deduplicated as (
        select *
        from with_ids
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by _airbyte_extracted_at desc
            )
            = 1
    )

select
    -- IDs
    gp_candidacy_stage_id,
    gp_candidacy_id,
    gp_candidate_id,
    gp_election_stage_id,
    gp_election_id,

    -- Candidate fields
    candidate_full_name,
    candidate_first_name,
    candidate_last_name,
    source_candidate_id,

    -- Race / election fields
    source_race_id,
    ddhq_race_id,
    state,
    state_postal_code,
    party_affiliation,
    candidate_office,
    official_office_name,
    office_level,
    office_type,
    district,
    election_stage,
    election_date,
    race_name,
    is_winner,
    is_uncontested,
    votes,
    number_of_seats_in_election,
    total_number_of_ballots_in_race,

    -- Candidacy-level dates
    candidacy_general_date,
    candidacy_primary_date,
    candidacy_general_runoff_date,
    candidacy_primary_runoff_date,

    -- Candidacy-stage derived
    election_result,
    election_result_source,

    -- Timestamps
    _airbyte_extracted_at as created_at,
    _airbyte_extracted_at as updated_at
from deduplicated
