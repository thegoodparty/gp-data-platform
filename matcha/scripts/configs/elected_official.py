# scripts/configs/elected_official.py
"""Elected officials entity resolution config."""

import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import block_on
from splink.blocking_rule_library import CustomRule
from splink.comparison_library import CustomComparison

from scripts.constants import EO_POST_PREDICTION_FILTER
from scripts.entity_config import EntityConfig

ELECTED_OFFICIAL_CONFIG = EntityConfig(
    entity_type="elected_official",
    display_name="Elected Officials",
    default_input_table="goodparty_data_catalog.dbt.int__er_prematch_elected_officials",
    comparisons=[
        # ── Person-level ──
        cl.JaroWinklerAtThresholds("last_name", score_threshold_or_thresholds=[0.95, 0.88]).configure(
            term_frequency_adjustments=True
        ),
        CustomComparison(
            output_column_name="first_name",
            comparison_levels=[
                cll.NullLevel("first_name"),
                cll.ExactMatchLevel("first_name").configure(
                    tf_adjustment_column="first_name",
                ),
                cll.ArrayIntersectLevel("first_name_aliases", min_intersection=1),
                # Compound first names overlap on a shared >=2-char token
                cll.ArrayIntersectLevel("first_name_tokens", min_intersection=1),
                cll.JaroWinklerLevel("first_name", distance_threshold=0.92),
                cll.ElseLevel(),
            ],
        ),
        cl.ExactMatch("party"),
        cl.ExactMatch("email"),
        cl.ExactMatch("phone"),
        # ── Office / geography-level ──
        cl.ExactMatch("state"),
        cl.JaroWinklerAtThresholds(
            "official_office_name",
            score_threshold_or_thresholds=[0.95, 0.88, 0.75],
        ),
        # Normalized office key (locality tokens + category + seat, at-large
        # treated as null). Catches cross-source office-name variants the raw
        # JaroWinkler above misses -- "Georgetown City Mayor" vs "Mayor of
        # Georgetown", "X Township Trustee" vs "X Township Trustee Board
        # At-Large". Null for offices with no distinctive locality, so sparse
        # offices stay neutral.
        cl.ExactMatch("official_office_norm"),
        # office_category alone (mayor/council/clerk/...) supplies a positive
        # office signal even when official_office_norm is null -- i.e. an
        # all-stop-word office name like a bare "Mayor", where the JaroWinkler on
        # official_office_name also falls below threshold. Derived from
        # official_office_name via office_match_keys, so it is comparable across
        # ballotready_techspeed / gp_api / ddhq.
        cl.ExactMatch("office_category"),
        cl.ExactMatch("district_identifier"),
        cl.ExactMatch("office_type"),
        cl.ExactMatch("office_level"),
        # ── Cross-source position ID (BR position FK) ──
        cl.ExactMatch("ballotready_position_id"),
        # ── Election cycle (links a DDHQ winner to the right term) ──
        # term_start_date carries the official's term start for BR/gp_api and the
        # winning election_date for ddhq (an official's term starts shortly after
        # their election). Same-cycle records land within a few months; different
        # cycles of the same office are years apart, so this separates them.
        cl.AbsoluteDateDifferenceAtThresholds(
            "term_start_date",
            input_is_string=True,
            metrics=["month", "month"],
            thresholds=[6, 24],
        ),
    ],
    blocking_rules_for_prediction=[
        # Rule 1: state + fuzzy office name + exact last name
        CustomRule(
            "l.state = r.state"
            " AND jaro_winkler_similarity(l.official_office_name,"
            " r.official_office_name) >= 0.88"
            " AND l.last_name = r.last_name",
            sql_dialect="duckdb",
        ),
        # Rule 2: state + fuzzy office + fuzzy last name (catches typos)
        CustomRule(
            "l.state = r.state"
            " AND jaro_winkler_similarity(l.official_office_name,"
            " r.official_office_name) >= 0.88"
            " AND jaro_winkler_similarity(l.last_name,"
            " r.last_name) >= 0.88",
            sql_dialect="duckdb",
        ),
        # Rule 3: broad catch-all
        block_on("state", "last_name"),
        # Rules 4-5: contact info (low coverage, high precision)
        block_on("phone"),
        block_on("email"),
        # Rule 6: state + ballotready_position_id (cross-source position match)
        CustomRule(
            "l.state = r.state" " AND l.ballotready_position_id = r.ballotready_position_id",
            sql_dialect="duckdb",
        ),
    ],
    additional_columns_to_retain=[
        "source_name",
        "source_id",
        "candidate_office",
        # NOTE: office_level and office_type are NOT listed here because
        # they are comparison columns — Splink retains them automatically.
        "district_raw",
        "city",
        "term_start_date",
        "term_end_date",
        "br_office_holder_id",
        "br_candidate_id",
        "ts_officeholder_id",
        "ts_position_id",
        # NOTE: ballotready_position_id is NOT listed here for the same reason as
        # office_level and office_type — it's a comparison column (cl.ExactMatch
        # at line 47), so Splink retains it automatically. Listing it here would
        # duplicate the column and risk SQL errors in some Splink versions.
        # NOTE: ICP flags (is_win_icp, is_serve_icp, is_win_supersize_icp) are NOT
        # listed here because they're functional attributes, not match-process
        # columns or identifiers. Downstream consumers fetch ICP from
        # int__civics_elected_official_ballotready via br_office_holder_id join.
        # gp-api product IDs for follow-up mart integration
        "gp_api_user_id",
        "gp_api_campaign_id",
        "gp_api_elected_office_id",
        "gp_api_organization_slug",
        # ddhq winner's own votes; read by the support score off the cluster
        "ddhq_votes",
    ],
    em_training_blocks=[
        ("last_name", "state", "office_level"),
        ("first_name", "state"),
        ("phone",),
        ("state", "office_type", "last_name"),
        ("state", "ballotready_position_id"),
    ],
    predict_threshold=0.01,
    cluster_threshold=0.95,
    date_columns=["term_start_date"],
    clustered_output_name="clustered_elected_officials.csv",
    post_prediction_filters=[EO_POST_PREDICTION_FILTER],
    audit_display_columns=[
        "source_name",
        "unique_id",
        "first_name",
        "last_name",
        "party",
        "email",
        "phone",
        "state",
        "official_office_name",
        "district_identifier",
        "candidate_office",
        "office_type",
        "office_level",
        "office_category",
        "ballotready_position_id",
        "term_start_date",
    ],
    audit_gamma_columns=[
        "gamma_last_name",
        "gamma_first_name",
        "gamma_party",
        "gamma_email",
        "gamma_phone",
        "gamma_state",
        "gamma_official_office_name",
        "gamma_district_identifier",
        "gamma_office_type",
        "gamma_office_level",
        "gamma_ballotready_position_id",
        "gamma_term_start_date",
        "gamma_official_office_norm",
        "gamma_office_category",
    ],
    false_negative_group_cols=["source_name", "state", "office_level"],
)
