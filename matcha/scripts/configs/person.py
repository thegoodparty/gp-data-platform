# scripts/configs/person.py
"""Person entity resolution config."""

import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import block_on
from splink.comparison_library import CustomComparison

from scripts.constants import PERSON_POST_PREDICTION_FILTER
from scripts.entity_config import EntityConfig

PERSON_CONFIG = EntityConfig(
    entity_type="person",
    display_name="People",
    default_input_table="goodparty_data_catalog.dbt.int__er_prematch_people",
    comparisons=[
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
        cl.ExactMatch("email"),
        cl.ExactMatch("phone"),
        cl.ExactMatch("state"),
        cl.ExactMatch("city"),
        cl.ExactMatch("zip5"),
        cl.ExactMatch("party"),
        # Year, not date: HubSpot's birth_date property is year-only (every
        # value lands on Jan 1), so an exact-date comparison would never agree
        # with TechSpeed's full dates.
        CustomComparison(
            output_column_name="birth_year",
            comparison_levels=[
                cll.NullLevel("birth_date"),
                cll.CustomLevel(
                    sql_condition="substr(birth_date_l, 1, 4) = substr(birth_date_r, 1, 4)",
                    label_for_charts="Same birth year",
                ),
                cll.ElseLevel(),
            ],
        ),
    ],
    blocking_rules_for_prediction=[
        # Contact keys: low coverage, high precision. Institutional inboxes and
        # placeholder phones are already nulled in the prematch, so these cannot
        # chain unrelated people.
        block_on("email"),
        block_on("phone"),
        # The workhorse. Every rule needs a narrowing term beyond name and
        # state: at ~1M records under link_and_dedupe, a common surname alone
        # runs to hundreds of millions of pairs.
        block_on("state", "last_name", "substr(first_name, 1, 1)"),
        block_on("last_name", "substr(birth_date, 1, 4)"),
        # Nicknames that change the first initial ("bob"/"robert" agree, but
        # "peggy"/"margaret" do not), which rule 3 cannot reach.
        block_on("state", "last_name", "first_name_aliases", arrays_to_explode=["first_name_aliases"]),
        # The dbt graph already resolved these pairs. Scoring them anyway is the
        # calibration signal: a BallotReady and a TechSpeed record for one
        # person, agreeing on nothing but the name, lands around 0.45.
        block_on("pregroup_id"),
    ],
    additional_columns_to_retain=[
        "source_name",
        "source_id",
        # Splink retains comparison columns itself; listing one here duplicates it.
        "pregroup_id",
        "suffix_token",
        "br_candidate_id",
        "first_seen_at",
    ],
    em_training_blocks=[
        ("email",),
        ("phone",),
        ("state", "last_name", "first_name"),
    ],
    predict_threshold=0.01,
    cluster_threshold=0.95,
    # One person can hold several HubSpot contacts; deduping within a source is
    # the point here, not an anomaly.
    link_type="link_and_dedupe",
    date_columns=["birth_date"],
    clustered_output_name="clustered_people.csv",
    post_prediction_filters=[PERSON_POST_PREDICTION_FILTER],
    audit_display_columns=[
        "source_name",
        "unique_id",
        "first_name",
        "last_name",
        "suffix_token",
        "email",
        "phone",
        "state",
        "city",
        "zip5",
        "birth_date",
        "party",
        "br_candidate_id",
    ],
    audit_gamma_columns=[
        "gamma_last_name",
        "gamma_first_name",
        "gamma_email",
        "gamma_phone",
        "gamma_state",
        "gamma_city",
        "gamma_zip5",
        "gamma_party",
        "gamma_birth_year",
    ],
    false_negative_group_cols=["source_name", "state"],
)
