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
    "'and','for','no.','odd','unexpired',"
    # Place/seat descriptors folded in from the former office_match_keys stop
    # list so the dbt office_name_tokens macro can reuse one list. Keep in sync
    # with the office_name_tokens macro in the gp-data-platform dbt project.
    "'township','twp','borough','regional','community','area','local','high',"
    "'authority','committee','members','schools','trustees','councillor',"
    "'post','place','group','division','at-large','atlarge'"
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

# Candidacy rescue for a changed last name. The BASE filter requires last-name
# agreement, so a candidate who files under a married, hyphenated or maiden
# name ("smith" vs "smith-jones" falls below the 0.88 JW level) is dropped even
# when every other signal says same person, same race. Admit the pair only when
# identity and race are both locked independently of the surname: the same
# email AND the same first name (households share an email, so first-name
# agreement keeps a spouse in the same race apart), AND the same BallotReady
# race id. DATA-2603 found 21 such gp_api <-> ballotready pairs on the
# 2026-11-03 cohort. Raw br_race_id columns, not a gamma: br_race_id is a
# blocking key only, so Splink never builds gamma_br_race_id.
CANDIDACY_LAST_NAME_CHANGE_RESCUE = """
    gamma_email > 0
      AND gamma_first_name > 0
      AND br_race_id_l IS NOT NULL
      AND br_race_id_l = br_race_id_r
"""

# Words dropped before two office token sets are compared for the surname
# variant guard below: term and vacancy filler, spelled-out numbers, and state
# names (a shared "florida" says nothing about the race).
_VARIANT_GUARD_NOISE_TOKENS = (
    "'year','years','term','terms','districted','unexpired','special','incumbent',"
    "'non-incumbent','one','two','three','four','five','six','seven','eight','nine',"
    "'ten','full','short','partial','vacancy','nonpartisan',"
    "'alabama','alaska','arizona','arkansas','california','colorado','connecticut',"
    "'delaware','florida','georgia','hawaii','idaho','illinois','indiana','iowa',"
    "'kansas','kentucky','louisiana','maine','maryland','massachusetts','michigan',"
    "'minnesota','mississippi','missouri','montana','nebraska','nevada','ohio',"
    "'oklahoma','oregon','pennsylvania','tennessee','texas','utah','vermont',"
    "'virginia','washington','wisconsin','wyoming','york','jersey','hampshire',"
    "'mexico','carolina','dakota','rhode'"
)


def _cleaned_office_tokens(side: str) -> str:
    """DuckDB expression for one side's office tokens with punctuation, codes
    and _VARIANT_GUARD_NOISE_TOKENS removed, sorted for set comparison."""
    return (
        "list_sort(list_distinct(list_filter("
        f"list_transform(official_office_name_tokens_{side}, t -> regexp_replace(t, '[^a-z/''-]', '', 'g')), "
        "t -> length(t) >= 2 AND NOT regexp_matches(t, '[0-9]') "
        f"AND NOT list_contains([{_VARIANT_GUARD_NOISE_TOKENS}], t))))"
    )


# Conflict guard for a candidacy pair whose surnames agree only through
# last_name_variants (gamma_last_name 1, the lowest non-else level). A record
# whose surname carries a prepended middle name can otherwise link to the same
# person's candidacy in a second race and chain two BallotReady candidacies
# together (a candidate who switched congressional districts; a mayor and a
# council seat on one ballot). Reject the pair on a race conflict:
#   - different districts or seats;
#   - two different br_race_ids with different office names (sources often carry
#     different ids for one race, so differing ids alone are not a conflict);
#   - no shared br_race_id and only a weak office match (gamma < 3, JW < 0.88),
#     unless the cleaned office token sets are identical. "taunton municipal
#     council" vs "taunton city council" passes; "sevier county mayor" vs
#     "sevier county register of deeds" and "north richland hills" vs "richland
#     hills" do not.
# A pair the last-name-change rescue admits already has the race locked.
CANDIDACY_LAST_NAME_VARIANT_LEVEL = 1
CANDIDACY_LAST_NAME_VARIANT_GUARD = f"""
    gamma_last_name <> {CANDIDACY_LAST_NAME_VARIANT_LEVEL}
      OR ({CANDIDACY_LAST_NAME_CHANGE_RESCUE})
      OR NOT (
        (district_identifier_l IS NOT NULL AND district_identifier_r IS NOT NULL
          AND district_identifier_l <> district_identifier_r)
        OR (seat_name_l IS NOT NULL AND seat_name_r IS NOT NULL AND seat_name_l <> seat_name_r)
        OR (br_race_id_l IS NOT NULL AND br_race_id_r IS NOT NULL AND br_race_id_l <> br_race_id_r
          AND official_office_name_l IS DISTINCT FROM official_office_name_r)
        OR ((br_race_id_l IS NULL OR br_race_id_r IS NULL)
          AND gamma_official_office_name < 3
          AND NOT coalesce(
            len({_cleaned_office_tokens("l")}) > 0
              AND {_cleaned_office_tokens("l")} = {_cleaned_office_tokens("r")},
            false))
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

# Person-level post-prediction filter.
#
# First-name agreement gates contact evidence. Households share an email and a
# phone: the 2026-08 study found 1,239 email-sharing and 4,053 phone-sharing
# pairs with the same last name and a different first name, and the audited
# sample was dominated by genuine two-person households.
#
# Two BallotReady people are a cannot-link. This refuses the direct pair;
# pipeline.cluster_with_links refuses the transitive case (BR1-X-BR2), which
# a pair filter cannot see.
#
# The name clause is the measured false-positive class: a pair with no shared
# contact key whose first names agree only because the nickname alias arrays
# intersect (antonio/antoinette, dennis/denise, nancy/hannah). Six of fifty
# sampled were wrong there against none elsewhere; abbreviations
# (ben/benjamin) were right every time and pass via contains().
PERSON_POST_PREDICTION_FILTER = """
    gamma_first_name > 0
      AND NOT (
        br_candidate_id_l IS NOT NULL
        AND br_candidate_id_r IS NOT NULL
        AND br_candidate_id_l <> br_candidate_id_r
      )
      AND (
        (email_l IS NOT NULL AND email_l = email_r)
        OR (phone_l IS NOT NULL AND phone_l = phone_r)
        OR first_name_l = first_name_r
        OR contains(first_name_l, first_name_r)
        OR contains(first_name_r, first_name_l)
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
