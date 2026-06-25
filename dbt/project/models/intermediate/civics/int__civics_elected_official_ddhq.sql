-- DDHQ election results -> Civics mart elected_officials schema (a third ER
-- source alongside ballotready_techspeed and gp_api). Grain: one row per DDHQ
-- general WINNER with a numeric vote tally (candidate_id + ddhq_race_id). A
-- winner is a sitting officeholder, so these records carry the official's own
-- identity (name + office + state) plus the vote count, which the support score
-- reads once the official is clustered to their gp_api elected_office.
-- Scope: general stages only (a primary win does not confer office) and
-- votes > 0 (uncontested/blank tallies give no support figure and are dropped).
-- election_date is the anchor that links a winner to the correct cycle of an
-- office; it becomes term_start_date in the prematch (an elected official's term
-- starts shortly after their winning election; see the support-score work that
-- validated the election-to-sworn-in gap at 0-3 months for ~85% of officials,
-- never beyond 14). office_type/office_level are NOT mapped: DDHQ's taxonomy
-- does not align with BallotReady's 7-bucket ExactMatch keys, so leaving them
-- null keeps those Splink features neutral rather than wrongly negative.
with
    winners as (
        select
            cast(candidate_id as string) as ddhq_candidate_id,
            cast(ddhq_race_id as string) as ddhq_race_id,
            lower(trim(candidate_first_name)) as first_name,
            lower(trim(candidate_last_name)) as last_name,
            trim(candidate_nickname) as candidate_nickname,
            state_postal_code as state,
            {{ parse_party_affiliation("party_affiliation") }} as party_affiliation,
            candidate_office,
            official_office_name,
            district,
            election_date,
            try_cast(votes as bigint) as votes
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
        where
            is_winner
            and lower(election_stage) like '%general%'
            and try_cast(votes as bigint) > 0
            and nullif(trim(candidate_first_name), '') is not null
            and nullif(trim(candidate_last_name), '') is not null
            and nullif(trim(state_postal_code), '') is not null
    )

-- One row per (candidate, race) winner; DDHQ delivers the same race across
-- multiple file syncs, so keep the highest reported tally for determinism.
select
    ddhq_candidate_id,
    ddhq_race_id,
    first_name,
    last_name,
    candidate_nickname,
    state,
    party_affiliation,
    candidate_office,
    official_office_name,
    district,
    election_date,
    votes
from winners
qualify
    row_number() over (partition by ddhq_candidate_id, ddhq_race_id order by votes desc)
    = 1
