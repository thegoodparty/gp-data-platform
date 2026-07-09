# scripts/configs/candidacy.py
"""Candidacy stage entity resolution config."""

import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import block_on
from splink.blocking_rule_library import CustomRule
from splink.comparison_library import CustomComparison

from scripts.constants import BASE_POST_PREDICTION_FILTER
from scripts.entity_config import EntityConfig

# Two sources routinely report the same election on dates that differ by a few
# days (data-entry / reporting lag), so an exact-date requirement drops real
# cross-source matches. Treat election dates within this window as the same
# election for both blocking and scoring. Kept well under the gap between a
# primary, runoff, and general (>=3 weeks apart), so distinct stages of one
# race never merge — the candidacy_stage grain still holds.
ELECTION_DATE_WINDOW_DAYS = 10

# Blocking-rule form of the window (l./r. prefixes). election_date is a
# 'YYYY-MM-DD' string after prematch prep, so parse it with try_strptime before
# differencing. This mirrors the SQL that AbsoluteDateDifferenceAtThresholds
# emits for the election_date comparison below (ABS(EPOCH(...) - EPOCH(...)) <=
# threshold*86400), so blocking and scoring agree on what "within N days" means.
_ELECTION_DATE_WITHIN_WINDOW = (
    "abs(epoch(try_strptime(l.election_date, '%Y-%m-%d'))"
    " - epoch(try_strptime(r.election_date, '%Y-%m-%d')))"
    f" <= {ELECTION_DATE_WINDOW_DAYS * 86400}"
)

CANDIDACY_CONFIG = EntityConfig(
    entity_type="candidacy_stage",
    display_name="Candidacy Stages",
    default_input_table="goodparty_data_catalog.dbt.int__er_prematch_candidacy_stages",
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
        cl.ExactMatch("party"),
        cl.ExactMatch("email"),
        cl.ExactMatch("phone"),
        cl.ExactMatch("state"),
        # election_date is not exact-match: sources disagree on the reported
        # date of the same election by a few days. This builds null / exact /
        # within-window / else levels — exact scores highest, a within-window
        # match is a distinct middle level (gamma 1) that the same-stage
        # post-filter (gamma_election_date > 0) still admits, and genuinely
        # different stages (>10 days apart) fall to else (gamma 0).
        cl.AbsoluteDateDifferenceAtThresholds(
            "election_date",
            input_is_string=True,
            metrics="day",
            thresholds=ELECTION_DATE_WINDOW_DAYS,
        ),
        CustomComparison(
            output_column_name="official_office_name",
            comparison_levels=[
                cll.NullLevel("official_office_name"),
                cll.JaroWinklerLevel("official_office_name", distance_threshold=0.95),
                cll.JaroWinklerLevel("official_office_name", distance_threshold=0.88),
                cll.JaroWinklerLevel("official_office_name", distance_threshold=0.75),
                # Token-overlap fallback — picks up cross-source naming
                # variants where JW is too low but a meaningful locality or
                # school-district code is shared (e.g. DDHQ
                # "Lincoln County R-IV School District" ↔ BR "Winfield R-4
                # School Board" both yield "r-4"). Tokens are pre-normalized
                # in dbt via the office_name_tokens macro.
                cll.ArrayIntersectLevel("official_office_name_tokens", min_intersection=1),
                cll.ElseLevel(),
            ],
        ),
        cl.ExactMatch("district_identifier"),
        cl.ExactMatch("office_level"),
    ],
    blocking_rules_for_prediction=[
        block_on("br_race_id"),
        # block_on("state", "last_name", "election_date") can't express the
        # date window, so it's a CustomRule: same state + exact last_name, dates
        # within the window. This subsumes the old state + window + office-JW +
        # exact-last_name rule (which only added an office filter on the same
        # keys), so that rule is dropped as redundant.
        CustomRule(
            "l.state = r.state" " AND l.last_name = r.last_name" f" AND {_ELECTION_DATE_WITHIN_WINDOW}",
            sql_dialect="duckdb",
        ),
        CustomRule(
            "l.state = r.state"
            f" AND {_ELECTION_DATE_WITHIN_WINDOW}"
            " AND jaro_winkler_similarity(l.official_office_name,"
            " r.official_office_name) >= 0.88"
            " AND jaro_winkler_similarity(l.last_name,"
            " r.last_name) >= 0.88",
            sql_dialect="duckdb",
        ),
        # Token-overlap fuzzy-lastname rule: catches DDHQ typos like
        # "hasler" ↔ "hassler", "klingler" ↔ "klinger" where the office-name JW
        # falls below 0.88 ("mehlville school district r-9 director" vs
        # "mehlville r-9 school board" → JW=0.85) but the locality+code tokens
        # overlap. Required because DDHQ has 0% br_race_id, phone, email
        # coverage, so blocking depends entirely on name + office signals.
        # Date window (not exact): the window is far narrower than the gap
        # between the primary / runoff / general stages the candidacy_stage
        # grain keeps distinct (e.g. AR Prosecuting Attorney D11 West has the
        # 3/3 primary, 5/19 runoff, and 11/3 general — all >10 days apart, so
        # Evelyn Moorehead still lands in three clusters), so it only bridges
        # the same election reported on slightly different dates.
        CustomRule(
            "l.state = r.state"
            f" AND {_ELECTION_DATE_WITHIN_WINDOW}"
            " AND list_has_any(l.official_office_name_tokens,"
            " r.official_office_name_tokens)"
            " AND jaro_winkler_similarity(l.last_name,"
            " r.last_name) >= 0.88",
            sql_dialect="duckdb",
        ),
        # Election-year guard on the two contact-only rules: phone/email carry no
        # date or race term, so on the all-time cohort they would block every
        # cross-year pair sharing a number/address. The same-stage post-filter
        # (gamma_election_date > 0) already drops any surviving cross-date pair,
        # so this is behavior-preserving for the single-year cohort and only
        # prevents cross-year pair explosion. election_date is a string here, so
        # substr(...,1,4) is the year (year() needs a DATE).
        block_on("phone", "substr(election_date, 1, 4)"),
        block_on("email", "substr(election_date, 1, 4)"),
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
        # first_name alone blocks ~256M cross-source pairs on the all-time
        # cohort (~83M even year-guarded), and EM materializes the full
        # comparison frame per session. Adding state cuts it to ~5M, below
        # the ~22M single-year production baseline.
        ("first_name", "state", "substr(election_date, 1, 4)"),
        # email stays unguarded: near-unique so the block is tiny, and it is
        # the one session with no election_date term (Splink skips
        # m-estimation for any column referenced by the block, even inside
        # substr), so it is where election_date's m gets trained.
        ("email",),
        ("state", "election_date", "last_name"),
        ("last_name", "state", "office_level", "substr(election_date, 1, 4)"),
    ],
    predict_threshold=0.01,
    cluster_threshold=0.95,
    date_columns=["election_date"],
    clustered_output_name="clustered_candidacies.csv",
    post_prediction_filters=[
        BASE_POST_PREDICTION_FILTER,
        # Race key: keep only if offices match strongly (gamma 3 == JW >= 0.88), br_race_id is shared, or br_race_id/district/office_type do not conflict (null-wildcards).
        """
          (
            gamma_official_office_name >= 3
            OR (br_race_id_l IS NOT NULL AND br_race_id_l = br_race_id_r)
            OR (
              (br_race_id_l IS NULL OR br_race_id_r IS NULL OR br_race_id_l = br_race_id_r)
              AND (district_identifier_l IS NULL OR district_identifier_r IS NULL OR district_identifier_l = district_identifier_r)
              AND (office_type_l IS NULL OR office_type_r IS NULL OR office_type_l = office_type_r)
            )
          )
        """,
        # Same-stage guard: candidacy_stage entities are stage-grained, so
        # records on different election_dates are different candidacies even
        # when name + phone + email + office all match. Without this, the
        # phone / email / br_race_id blocking rules transitively merge a
        # candidate's primary, runoff, and general candidacies (same person
        # has the same phone across all three stages, but the BR records have
        # distinct br_race_ids and dates per stage). gamma_election_date > 0
        # admits both an exact match (gamma 2) and a within-window match
        # (gamma 1) as the same stage, while stages >10 days apart score gamma
        # 0 and are dropped.
        "gamma_election_date > 0",
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
