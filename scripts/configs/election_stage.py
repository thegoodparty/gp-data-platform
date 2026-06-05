# scripts/configs/election_stage.py
"""Election-stage (race/contest) entity resolution config."""

import splink.internals.comparison_library as cl
from splink import block_on
from splink.blocking_rule_library import CustomRule

from scripts.constants import ELECTION_STAGE_POST_PREDICTION_FILTER
from scripts.entity_config import EntityConfig

ELECTION_STAGE_CONFIG = EntityConfig(
    entity_type="election_stage",
    display_name="Election Stages",
    default_input_table="goodparty_data_catalog.dbt.int__er_prematch_election_stages",
    comparisons=[
        # ── Geography ──
        cl.ExactMatch("state"),
        # ── Office ──
        cl.JaroWinklerAtThresholds(
            "official_office_name",
            score_threshold_or_thresholds=[0.95, 0.88, 0.75],
        ),
        cl.ExactMatch("candidate_office"),
        cl.ExactMatch("office_level"),
        cl.ExactMatch("office_type"),
        cl.ExactMatch("district_identifier"),
        cl.ExactMatch("seat_name"),
        # ── Election cycle ──
        cl.ExactMatch("election_date"),
        cl.ExactMatch("election_stage"),
        cl.ExactMatch("is_special"),
        # ── Cross-source position FK (strong when both sides have it) ──
        cl.ExactMatch("ballotready_position_id"),
    ],
    blocking_rules_for_prediction=[
        # 1. Primary block: state + date + fuzzy office
        CustomRule(
            "l.state = r.state"
            " AND l.election_date = r.election_date"
            " AND jaro_winkler_similarity(l.official_office_name,"
            " r.official_office_name) >= 0.88",
            sql_dialect="duckdb",
        ),
        # 2. Normalized-office block: catches cross-source office-name variants
        # that fall below the rule-1 JW threshold (e.g. "lincoln county r-iv
        # school board" vs "lincoln r-4 school district"). candidate_office is
        # the normalized office and is populated on all sources, so this closes
        # the DDHQ-vs-BR blocking-recall gap the audit surfaced. The
        # post-prediction filter still requires a shared locality token.
        block_on("state", "election_date", "candidate_office"),
        # 3. Exact tuple block
        block_on("state", "election_date", "office_level", "district_identifier"),
        # 4. Position-FK fast path
        block_on("state", "election_date", "ballotready_position_id"),
        # 5. Position-FK without date — catches date drift between sources
        block_on("state", "ballotready_position_id"),
        # 6. BR-race-id anchor — TS rows carry their own br_race_id reference to
        # a BR race (the prematch surfaces it; DDHQ/BR-side is the BR race's own
        # id). Blocking on it generates BR<->TS candidate pairs the office/geo
        # rules miss when the office name diverges. It is NOT a deterministic
        # link: the post-prediction filter still confirms the pair (and rejects
        # the ~2% where one TS br_race_id maps to the wrong BR stage).
        block_on("br_race_id"),
        # 7. Candidacy-overlap anchor — two races that share a matched
        # candidacy_stage ER cluster have a candidate in common, strong evidence
        # they're the same race even when office names diverge. Reaches DDHQ,
        # which carries no br_race_id and can only link to BR via candidate
        # identity. state + date prefilter bounds the array-overlap join; the
        # filter confirms (a shared cluster satisfies office identity).
        CustomRule(
            "l.state = r.state"
            " AND l.election_date = r.election_date"
            " AND list_has_any(l.matched_candidacy_stage_clusters,"
            " r.matched_candidacy_stage_clusters)",
            sql_dialect="duckdb",
        ),
    ],
    additional_columns_to_retain=[
        "source_name",
        "source_id",
        # NOTE: state, official_office_name, candidate_office, office_level,
        # office_type, district_identifier, seat_name, election_date,
        # election_stage, is_special, ballotready_position_id are all
        # comparison columns — Splink retains them automatically.
        "district_raw",
        "br_race_id",
        "matched_candidacy_stage_clusters",
    ],
    em_training_blocks=[
        ("state", "election_date", "office_level"),
        ("state", "election_date", "ballotready_position_id"),
        ("state", "office_type", "district_identifier"),
        ("state", "election_stage", "election_date"),
    ],
    predict_threshold=0.01,
    cluster_threshold=0.95,
    date_columns=["election_date"],
    clustered_output_name="clustered_election_stages.csv",
    post_prediction_filters=[
        # is_special is fully derivable from election_stage (the stage string
        # contains "special" iff is_special is true -- 0 divergence across all
        # sources), and the filter already hard-gates election_stage equality,
        # so a separate is_special suppression can never drop a pair the stage
        # gate doesn't already drop. Omitted as redundant.
        ELECTION_STAGE_POST_PREDICTION_FILTER,
    ],
    audit_display_columns=[
        "source_name",
        "unique_id",
        "state",
        "official_office_name",
        "candidate_office",
        "office_level",
        "office_type",
        "district_identifier",
        "seat_name",
        "election_date",
        "election_stage",
        "is_special",
        "ballotready_position_id",
        "br_race_id",
    ],
    audit_gamma_columns=[
        "gamma_state",
        "gamma_official_office_name",
        "gamma_candidate_office",
        "gamma_office_level",
        "gamma_office_type",
        "gamma_district_identifier",
        "gamma_seat_name",
        "gamma_election_date",
        "gamma_election_stage",
        "gamma_is_special",
        "gamma_ballotready_position_id",
    ],
    false_negative_group_cols=["source_name", "state", "election_date"],
)
