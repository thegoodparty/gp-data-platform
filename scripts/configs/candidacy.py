# scripts/configs/candidacy.py
"""Candidacy stage entity resolution config."""

import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import block_on
from splink.blocking_rule_library import CustomRule
from splink.comparison_library import CustomComparison

from scripts.constants import BASE_POST_PREDICTION_FILTER
from scripts.entity_config import EntityConfig

CANDIDACY_CONFIG = EntityConfig(
    entity_type="candidacy_stage",
    display_name="Candidacy Stages",
    default_input_table="goodparty_data_catalog.dbt.int__er_prematch_candidacy_stages",
    comparisons=[
        cl.JaroWinklerAtThresholds(
            "last_name", score_threshold_or_thresholds=[0.95, 0.88]
        ).configure(term_frequency_adjustments=True),
        CustomComparison(
            output_column_name="first_name",
            comparison_levels=[
                cll.NullLevel("first_name"),
                cll.ExactMatchLevel("first_name").configure(
                    tf_adjustment_column="first_name",
                ),
                cll.ArrayIntersectLevel("first_name_aliases", min_intersection=1),
                cll.JaroWinklerLevel("first_name", distance_threshold=0.92),
                cll.ElseLevel(),
            ],
        ),
        cl.ExactMatch("party"),
        cl.ExactMatch("email"),
        cl.ExactMatch("phone"),
        cl.ExactMatch("state"),
        cl.ExactMatch("election_date"),
        cl.JaroWinklerAtThresholds(
            "official_office_name",
            score_threshold_or_thresholds=[0.95, 0.88, 0.75],
        ),
        cl.ExactMatch("district_identifier"),
        cl.ExactMatch("office_level"),
    ],
    blocking_rules_for_prediction=[
        block_on("br_race_id"),
        CustomRule(
            "l.state = r.state"
            " AND l.election_date = r.election_date"
            " AND jaro_winkler_similarity(l.official_office_name,"
            " r.official_office_name) >= 0.88"
            " AND l.last_name = r.last_name",
            sql_dialect="duckdb",
        ),
        block_on("state", "last_name", "election_date"),
        CustomRule(
            "l.state = r.state"
            " AND l.election_date = r.election_date"
            " AND jaro_winkler_similarity(l.official_office_name,"
            " r.official_office_name) >= 0.88"
            " AND jaro_winkler_similarity(l.last_name,"
            " r.last_name) >= 0.88",
            sql_dialect="duckdb",
        ),
        block_on("phone"),
        block_on("email"),
    ],
    additional_columns_to_retain=[
        "source_name",
        "source_id",
        "candidate_office",
        # NOTE: office_level is NOT listed here because it's a comparison column —
        # Splink retains it automatically. Listing it would duplicate the column
        # and risk SQL errors in some Splink versions. Mirrors EO convention at
        # scripts/configs/elected_official.py.
        "office_type",
        "district_raw",
        "seat_name",
        "br_race_id",
        "br_candidacy_id",
        "election_stage",
        # partisan_type forwarded so gp-data-platform's
        # stg_er_source__clustered_candidacy_stages can project it
        # (downstream consumers — e.g. mart_civics — require it). The
        # 2026-04-30 production snapshot included it via a one-off
        # un-committed local edit; this commit formalizes the retention.
        "partisan_type",
    ],
    em_training_blocks=[
        ("last_name", "state", "election_date"),
        ("first_name",),
        ("email",),
        ("state", "election_date", "last_name"),
        ("last_name", "state", "office_level"),
    ],
    predict_threshold=0.01,
    cluster_threshold=0.95,
    date_columns=["election_date"],
    clustered_output_name="clustered_candidacies.csv",
    post_prediction_filters=[
        BASE_POST_PREDICTION_FILTER,
        """
          NOT (
            br_race_id_l IS NOT NULL
            AND br_race_id_r IS NOT NULL
            AND br_race_id_l != br_race_id_r
            AND gamma_official_office_name < 2
          )
        """,
    ],
    audit_display_columns=[
        "source_name",
        "unique_id",
        "first_name",
        "last_name",
        "party",
        "email",
        "phone",
        "state",
        "election_date",
        "official_office_name",
        "district_identifier",
        "candidate_office",
        "office_level",
        "br_race_id",
    ],
    audit_gamma_columns=[
        "gamma_last_name",
        "gamma_first_name",
        "gamma_party",
        "gamma_email",
        "gamma_phone",
        "gamma_state",
        "gamma_election_date",
        "gamma_official_office_name",
        "gamma_district_identifier",
        "gamma_office_level",
    ],
    false_negative_group_cols=["source_name", "state", "election_date"],
)
