# scripts/constants.py
"""Shared constants for entity resolution configs."""

OFFICE_STOP_WORDS = (
    "'city','of','the','county','board','council','school','district',"
    "'mayor','alderperson','trustee','at','large','zone','ward','seat',"
    "'position','commission','precinct','town','village','member',"
    "'councilmember','supervisor','supervisors','commissioner','judge',"
    "'branch','education','unified','public','elementary','consolidated',"
    "'central','special','independent','office','clerk','treasurer',"
    "'coroner','sheriff','magistrate','property','value','administrator',"
    "'emergency','services','director','justice','peace','representative',"
    "'house','representatives','legislature','legislative','metro',"
    "'president','attorney','executive','municipal','assessor','auditor',"
    "'recorder','register','surveyor','constable','marshal','comptroller',"
    "'controller','prosecutor','councilor','councilman','councilwoman',"
    "'alderman','alderwoman','selectman','selectperson','freeholder',"
    "'and','for','no.','odd','unexpired'"
)


def _office_locality_tokens(side: str) -> str:
    """DuckDB expression for the distinct meaningful (locality) tokens of an
    office name on one side of a pair. Drops stop words, single chars, and
    pure-digit tokens, leaving the locality/distinguishing tokens.
    """
    return (
        "list_distinct(list_filter("
        f"string_split(lower(official_office_name_{side}), ' '), "
        "x -> len(x) > 1 "
        f"AND NOT list_contains([{OFFICE_STOP_WORDS}], x) "
        "AND NOT regexp_matches(x, '^\\d+$')))"
    )


# Shared post-prediction filter: requires name + identity signal + office overlap.
# Each config can extend this with entity-specific clauses.
#
# Office overlap is now satisfied by gamma_official_office_name > 0, which the
# CustomComparison's ArrayIntersectLevel (over official_office_name_tokens)
# fires on cross-source naming variants like DDHQ "Lincoln County R-IV School
# District" ↔ BR "Winfield R-4 School Board". Token normalization (parens
# preserved, roman→arabic, "no. N" → "r-N") lives in the dbt office_name_tokens
# macro so the inline string-split fallback that used to live here is no
# longer needed.
BASE_POST_PREDICTION_FILTER = """
    gamma_last_name > 0
      AND gamma_official_office_name > 0
      AND (
        gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0
        OR (
          -- First-name variant rescue. Abbreviations and short forms
          -- ("rey"↔"reynaldo", "lin"↔"lindsay") fall below the first_name
          -- gamma JW>=0.92 level, so a source with no email/phone (notably
          -- ddhq) gets dropped from an otherwise-certain cluster even at
          -- pre-filter probability 0.9999. Rescue only when the office, last
          -- name, and election date are all a strong lock and the first names
          -- are still a close JW match, so two different people sharing a
          -- last name, office, and date are not pulled together.
          gamma_official_office_name >= 3
          AND gamma_last_name > 0
          AND gamma_election_date > 0
          AND first_name_l IS NOT NULL
          AND first_name_r IS NOT NULL
          AND jaro_winkler_similarity(lower(first_name_l), lower(first_name_r)) >= 0.80
        )
      )
"""

# EO-specific post-prediction filter: adds contact-info bypass and office_type
# fallback for cross-source office title synonyms. Contact-confirmed pairs
# (email or phone match) skip office checks entirely since identity is established.
# Does NOT include the candidacy-specific br_race_id guard.
EO_POST_PREDICTION_FILTER = f"""
    gamma_last_name > 0
      AND (gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)
      AND (
        gamma_email > 0
        OR gamma_phone > 0
        OR gamma_official_office_name > 0
        OR (
          list_has_any(
            {_office_locality_tokens("l")}, {_office_locality_tokens("r")}
          )
          AND (
            district_identifier_l IS NULL
            OR district_identifier_r IS NULL
            OR district_identifier_l = district_identifier_r
          )
        )
        OR gamma_office_type > 0
        OR gamma_ballotready_position_id > 0
      )
"""

# Race-level post-prediction filter for election_stage ER. No person fields
# (no first_name/last_name/email/phone), so race identity must be carried
# entirely by geography + office + election cycle. A pair is kept only when it
# agrees on:
#   - state, election_date, election_stage. An election stage is a distinct
#     entity: a primary and a general for the same office must not cluster.
#   - office identity: a near-exact full office name (>=0.95 JW tier), OR the
#     same normalized candidate_office AND a shared locality token. Requiring
#     candidate_office stops different offices in one county ("X county clerk"
#     vs "X county mayor") from merging on the shared "X" locality token, and
#     requiring the locality token stops same-office different-locality races
#     ("nelson village president" vs "suamico village president") from merging
#     on the shared generic office suffix.
#   - district_identifier and seat_name, when both sides expose them, so
#     "... district 1" and "... district 2" do not merge.
#
# state and election_date are exact-equality keys in the blocking rules / EM
# blocks, so Splink never trains their m and drops the gamma_<col>. Reference
# the retained raw _l/_r columns for those instead of gamma_*. The
# locality-token overlap mirrors BASE_POST_PREDICTION_FILTER's stop-word
# treatment; OFFICE_STOP_WORDS strips generic office nouns so the locality is
# the discriminating token.
_es_tok_l = _office_locality_tokens("l")
_es_tok_r = _office_locality_tokens("r")
_es_tok_overlap = f"len(list_intersect({_es_tok_l}, {_es_tok_r}))"
ELECTION_STAGE_POST_PREDICTION_FILTER = f"""
    state_l = state_r
      AND election_date_l = election_date_r
      -- election_stage is a hard identity discriminator (a primary and a
      -- general for the same office must NOT cluster), not an optional
      -- refinement like district_identifier/seat_name below. A NULL stage is
      -- failed CLOSED here (require both present and equal), NOT treated as a
      -- wildcard: wildcarding would let a NULL-stage record match both the
      -- primary and the general of one office and hub-chain the two distinct
      -- stages. 100% populated in the prematch today; the explicit IS NOT NULL
      -- documents the intended contract and makes the drop deliberate, not a
      -- silent three-valued-logic side effect.
      AND election_stage_l IS NOT NULL
      AND election_stage_r IS NOT NULL
      AND election_stage_l = election_stage_r
      AND (
        gamma_official_office_name >= 3
        -- BR-race-id anchor: a TS row carries its own br_race_id reference to a
        -- BR race, so a shared br_race_id IS the office/race identity -- it
        -- stands in for office-name agreement, which fails ~53% of the time on
        -- cross-source office-name normalization. state/date/stage (above) and
        -- district/seat (below) still gate, so this only bypasses office-name
        -- variance, and the stage gate rejects the ~2% where a TS br_race_id
        -- maps to the wrong BR stage. NULL br_race_id (DDHQ, BR<->BR) can't
        -- satisfy it, so it never relaxes non-anchored pairs.
        OR (br_race_id_l IS NOT NULL AND br_race_id_l = br_race_id_r)
        -- Candidacy-overlap anchor: the two races share a matched
        -- candidacy_stage ER cluster, i.e. a candidate in common -- that IS the
        -- race identity, so it stands in for office-name agreement (which fails
        -- on cross-source naming variance). Reaches DDHQ, which has no
        -- br_race_id. state/date/stage still gate above; empty arrays (races
        -- with no matched candidacies) can't satisfy it.
        OR len(
          list_intersect(
            matched_candidacy_stage_clusters_l, matched_candidacy_stage_clusters_r
          )
        ) > 0
        OR (
          candidate_office_l = candidate_office_r
          -- Require locality-token SET EQUALITY, not subset or overlap. A
          -- shorter set that is a subset of a longer one (e.g. {"grand"} vs
          -- {"grand","prairie"}) would let a generic single-token office
          -- hub-match multiple distinct localities and chain them transitively
          -- ("grand prairie" <-> "grand saline"). Equality keeps only genuinely
          -- same-locality races; for an identity crosswalk a missed match
          -- (separate ids) is safer than a wrong merge (corrupted canonical id).
          AND (
            -- No locality tokens on either side (a statewide / no-locality
            -- office whose name is all stop words) means no locality
            -- disagreement, so the candidate_office match alone suffices.
            -- Verified safe on current data: only ~18 such records, largest
            -- same (state, date, stage, office) group is 2 -- no blob risk.
            (len({_es_tok_l}) = 0 AND len({_es_tok_r}) = 0)
            OR (
              {_es_tok_overlap} > 0
              AND {_es_tok_overlap} = len({_es_tok_l})
              AND {_es_tok_overlap} = len({_es_tok_r})
            )
          )
        )
      )
      AND (
        district_identifier_l IS NULL
        OR district_identifier_r IS NULL
        OR district_identifier_l = district_identifier_r
      )
      AND (
        seat_name_l IS NULL
        OR seat_name_r IS NULL
        OR seat_name_l = seat_name_r
      )
"""
