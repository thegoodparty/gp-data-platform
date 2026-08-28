-- Historical archive of elections from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Self-contained archive: election logic is inlined here, not sourced from an
-- external mart
with
    elections as (
        select
            -- Identifiers
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_contest.official_office_name,
            tbl_contest.candidate_office,
            tbl_contest.office_level,
            tbl_contest.office_type,
            tbl_contest.state,
            tbl_contest.city,
            tbl_contest.district,
            tbl_contest.seat_name,
            -- Fall back to candidacy's general_election_date when the contest
            -- row is missing or has a null/out-of-range date. Keeps elections
            -- represented whenever a candidacy references them, closing the
            -- orphan gap that suppressed ~68k 2025 candidacies' election_stage
            -- + ICP flagging.
            coalesce(
                tbl_contest.election_date, tbl_candidacy.general_election_date
            ) as election_date,
            coalesce(
                tbl_contest.election_year, year(tbl_candidacy.general_election_date)
            ) as election_year,
            tbl_contest.filing_deadline,
            tbl_contest.population,
            tbl_contest.seats_available,
            tbl_contest.term_start_date,
            tbl_contest.is_uncontested,
            tbl_contest.number_of_opponents,
            tbl_contest.is_open_seat,
            tbl_ddhq_matches.ddhq_race_id is not null as has_ddhq_match,
            tbl_candidacy.br_position_database_id,
            tbl_br_position.is_judicial,
            tbl_br_position.is_appointed,
            tbl_br_normalized.name as br_normalized_position_type,
            tbl_contest.created_at,
            tbl_contest.updated_at
        from {{ ref("int__hubspot_contest_2025") }} as tbl_contest
        left join
            {{ ref("int__civics_candidacy_2025") }} as tbl_candidacy
            on tbl_candidacy.hubspot_contact_id = tbl_contest.contact_id
        left join
            {{ ref("stg_model_predictions__candidacy_ddhq_matches_20251016") }}
            as tbl_ddhq_matches
            on tbl_ddhq_matches.gp_candidacy_id = tbl_candidacy.gp_candidacy_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_br_position
            on tbl_candidacy.br_position_database_id = tbl_br_position.database_id
        left join
            {{ ref("int__ballotready_normalized_position") }} as tbl_br_normalized
            on tbl_br_position.normalized_position.`databaseId`
            = tbl_br_normalized.database_id
    ),

    archived_elections as (
        select *
        from elections
        where election_date <= '2025-12-31' and election_date >= '1900-01-01'
        qualify
            row_number() over (
                partition by gp_election_id
                order by has_ddhq_match desc, updated_at desc
            )
            = 1
    ),

    collapsed as (
        -- Trustworthy + unique br_position_database_id per
        -- (br_position_database_id, election_date):
        -- * office_mismatch rows (curated seed) drop the tag, and
        -- * among same-office duplicates, only the most complete row keeps it.
        -- The seed match is folded into the partition key so a mismatched row
        -- cannot occupy a group's winning slot. gp_election_id is the terminal
        -- tiebreak, so the winner is deterministic within a build. Uniqueness
        -- holds on every build regardless of gp_election_id drift.
        select
            archived_elections.* except (br_position_database_id),
            case
                when nullouts.gp_election_id is not null
                then null
                when
                    row_number() over (
                        partition by
                            case
                                when nullouts.gp_election_id is not null
                                then null
                                else archived_elections.br_position_database_id
                            end,
                            archived_elections.election_date
                        order by
                            (archived_elections.official_office_name is not null) desc,
                            archived_elections.has_ddhq_match desc,
                            archived_elections.updated_at desc nulls last,
                            archived_elections.gp_election_id
                    )
                    = 1
                then archived_elections.br_position_database_id
            end as br_position_database_id
        from archived_elections
        left join
            {{ ref("seed_civics_election_2025_position_nullouts") }} as nullouts
            on archived_elections.gp_election_id = nullouts.gp_election_id
    )

select
    collapsed.gp_election_id,
    collapsed.official_office_name,
    collapsed.candidate_office,
    collapsed.office_level,
    collapsed.office_type,
    tbl_states.state_cleaned_postal_code as state,
    collapsed.city,
    collapsed.district,
    collapsed.seat_name,
    collapsed.election_date,
    collapsed.election_year,
    collapsed.filing_deadline,
    collapsed.population,
    collapsed.seats_available,
    collapsed.term_start_date,
    collapsed.is_uncontested,
    collapsed.number_of_opponents,
    collapsed.is_open_seat,
    collapsed.has_ddhq_match,
    collapsed.br_position_database_id,
    -- Position-derived attributes must track the (possibly nulled)
    -- br_position_database_id: a null position means the tag is untrusted, so
    -- these must not surface the dropped position's values.
    case
        when collapsed.br_position_database_id is null
        then null
        else collapsed.is_judicial
    end as is_judicial,
    case
        when collapsed.br_position_database_id is null
        then null
        else collapsed.is_appointed
    end as is_appointed,
    case
        when collapsed.br_position_database_id is null
        then null
        else collapsed.br_normalized_position_type
    end as br_normalized_position_type,
    collapsed.created_at,
    collapsed.updated_at

from collapsed
left join
    {{ ref("clean_states") }} as tbl_states
    on trim(upper(collapsed.state)) = tbl_states.state_raw
