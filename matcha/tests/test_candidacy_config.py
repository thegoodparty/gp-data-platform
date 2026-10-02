# tests/test_candidacy_config.py
"""Tests for the candidacy ER config (DATA-1880 follow-up: office_level comparison)."""

from scripts.configs.candidacy import CANDIDACY_CONFIG, ELECTION_DATE_WINDOW_DAYS


def _election_date_comparison():
    return next(
        c
        for c in CANDIDACY_CONFIG.comparisons
        if c.get_comparison("duckdb").output_column_name == "election_date"
    )


def test_election_date_has_within_window_level():
    """election_date is not exact-only: it carries a within-window level between
    the exact match and ElseLevel, so near-duplicate dates score gamma 1. The
    level thresholds the date difference at ELECTION_DATE_WINDOW_DAYS (in
    seconds) so it agrees with the blocking-rule window."""
    cmp = _election_date_comparison().get_comparison("duckdb").as_dict()
    window_seconds = ELECTION_DATE_WINDOW_DAYS * 86400
    assert any(
        "epoch" in level.get("sql_condition", "").lower()
        and str(window_seconds) in level.get("sql_condition", "")
        for level in cmp["comparison_levels"]
    ), f"Expected a within-{ELECTION_DATE_WINDOW_DAYS}-day (<= {window_seconds}s) level"


def test_election_date_window_sql_is_idempotent():
    """Resolving the comparison repeatedly must not re-wrap the date column.

    AbsoluteDateDifference*Level(input_is_string=True) parses the string via
    try_strptime; a stateful implementation would re-wrap that column on each
    resolution, stacking try_strptime() until the string double-parses to NULL.
    The comparison builder must resolve to stable SQL (one try_strptime per side).
    """
    cmp = _election_date_comparison()
    counts = []
    for _ in range(3):
        d = cmp.get_comparison("duckdb").as_dict()
        sql = next(
            level["sql_condition"]
            for level in d["comparison_levels"]
            if "epoch" in level.get("sql_condition", "").lower()
        )
        counts.append(sql.lower().count("try_strptime"))
    assert counts == [2, 2, 2], f"date-window SQL not idempotent: try_strptime counts {counts}"


def test_office_level_in_comparisons():
    """office_level is added as an ExactMatch comparison."""
    comparison_columns = [c.get_comparison("duckdb").output_column_name for c in CANDIDACY_CONFIG.comparisons]
    assert "office_level" in comparison_columns


def test_office_level_em_training_block():
    """office_level appears in at least one EM training block (mirrors EO pattern)."""
    assert any(
        "office_level" in cols for cols in CANDIDACY_CONFIG.em_training_blocks
    ), "Expected an EM training block including office_level"


def test_office_level_not_in_additional_columns_to_retain():
    """office_level is a comparison column and should NOT be in additional_columns_to_retain.

    Splink retains comparison columns automatically; listing it would duplicate
    the column and risk SQL errors. Mirrors EO convention.
    """
    retained = set(CANDIDACY_CONFIG.additional_columns_to_retain)
    assert "office_level" not in retained, (
        "office_level is a comparison column and should not be listed in "
        "additional_columns_to_retain — Splink retains comparison columns automatically."
    )


def test_gamma_office_level_in_audit_gamma_columns():
    """gamma_office_level is exposed in the audit gamma columns list."""
    assert "gamma_office_level" in CANDIDACY_CONFIG.audit_gamma_columns


def _first_name_comparison_sql():
    fn = next(
        c
        for c in CANDIDACY_CONFIG.comparisons
        if c.get_comparison("duckdb").output_column_name == "first_name"
    )
    return fn.get_comparison("duckdb").as_dict()


def test_first_name_comparison_has_token_intersect_level():
    """first_name comparison includes an ArrayIntersectLevel over the precomputed
    first_name_tokens column."""
    cmp = _first_name_comparison_sql()
    assert any(
        "first_name_tokens" in level.get("sql_condition", "") for level in cmp["comparison_levels"]
    ), "Expected an ArrayIntersectLevel over first_name_tokens"


def _identity_filter_sql():
    # The first candidacy filter is BASE OR the last-name-change rescue.
    return CANDIDACY_CONFIG.post_prediction_filters[0]


def _eval_identity_filter(**overrides) -> bool:
    import duckdb

    row = {
        # Same person, same race, but the surname changed ("smith" -> "smith-jones").
        "gamma_last_name": 0,
        "gamma_first_name": 3,
        "gamma_email": 1,
        "gamma_phone": -1,
        "gamma_official_office_name": 4,
        "gamma_election_date": 2,
        "first_name_l": "maria",
        "first_name_r": "maria",
        "br_race_id_l": "1234",
        "br_race_id_r": "1234",
    }
    row.update(overrides)
    placeholders = ", ".join(f"? AS {k}" for k in row)
    sql = f"SELECT coalesce(({_identity_filter_sql()}), false) FROM (SELECT {placeholders})"
    return duckdb.connect().execute(sql, list(row.values())).fetchone()[0]


def test_last_name_change_rescued_on_email_first_name_and_race():
    assert _eval_identity_filter() is True


def test_last_name_change_not_rescued_without_email():
    assert _eval_identity_filter(gamma_email=0) is False


def test_last_name_change_not_rescued_for_household_member():
    # Shared household email, same race, different first name: a spouse, not a rename.
    assert _eval_identity_filter(gamma_first_name=0, first_name_r="david") is False


def test_last_name_change_not_rescued_across_races():
    assert _eval_identity_filter(br_race_id_r="9999") is False


def test_last_name_change_not_rescued_when_race_unknown():
    assert _eval_identity_filter(br_race_id_l=None) is False


def test_base_identity_path_unchanged():
    # Last name agrees: BASE admits it regardless of the rescue's race requirement.
    assert _eval_identity_filter(gamma_last_name=3, br_race_id_r="9999") is True


def _comparison(name):
    c = next(c for c in CANDIDACY_CONFIG.comparisons if c.get_comparison("duckdb").output_column_name == name)
    return c.get_comparison("duckdb").as_dict()


def test_last_name_comparison_has_variants_level_below_jaro_winkler():
    """The variants intersection is the last non-else level, so it only scores
    pairs that the exact and JW levels already rejected."""
    levels = [lvl.get("sql_condition", "") for lvl in _comparison("last_name")["comparison_levels"]]
    variant_idx = next(i for i, sql in enumerate(levels) if "last_name_variants" in sql)
    jw_idx = max(i for i, sql in enumerate(levels) if "jaro_winkler" in sql.lower())
    assert variant_idx == jw_idx + 1
    assert levels[-1].upper() == "ELSE"


def test_last_name_keeps_term_frequency_on_exact_level():
    exact = next(
        lvl
        for lvl in _comparison("last_name")["comparison_levels"]
        if lvl.get("tf_adjustment_column") == "last_name"
    )
    assert "=" in exact["sql_condition"]


def test_blocking_on_last_word_of_surname():
    rules = [
        r.get_blocking_rule("duckdb").blocking_rule_sql
        for r in CANDIDACY_CONFIG.blocking_rules_for_prediction
    ]
    assert any("last_name_variants[-1]" in sql for sql in rules)


def test_variant_guard_targets_the_variants_level():
    """Splink numbers non-null levels from the top down to 0 (ElseLevel), so the
    guard's hard-coded gamma must be the variants level's position from the end."""
    from scripts.constants import CANDIDACY_LAST_NAME_VARIANT_LEVEL

    levels = [lvl for lvl in _comparison("last_name")["comparison_levels"] if not lvl.get("is_null_level")]
    idx = next(i for i, lvl in enumerate(levels) if "last_name_variants" in lvl.get("sql_condition", ""))
    assert len(levels) - 1 - idx == CANDIDACY_LAST_NAME_VARIANT_LEVEL


def _eval_variant_guard(**overrides) -> bool:
    import duckdb

    from scripts.constants import CANDIDACY_LAST_NAME_VARIANT_GUARD

    row = {
        # "andre reynolds" vs "reynolds": surnames agree only through the variants.
        "gamma_last_name": 1,
        "gamma_official_office_name": 3,
        "gamma_email": 0,
        "gamma_first_name": 4,
        "br_race_id_l": None,
        "br_race_id_r": "2022089",
        "district_identifier_l": "5",
        "district_identifier_r": "5",
        "seat_name_l": None,
        "seat_name_r": None,
        "official_office_name_l": "u.s. house of representatives district 5",
        "official_office_name_r": "u.s. house of representatives - tennessee 5th congressional district",
        "official_office_name_tokens_l": ["u.s"],
        "official_office_name_tokens_r": ["u.s", "tennessee", "5th", "congressional"],
    }
    row.update(overrides)

    def cast(k):
        if k.endswith("tokens_l") or k.endswith("tokens_r"):
            return "VARCHAR[]"
        return "VARCHAR" if k.endswith(("_l", "_r")) else "INTEGER"

    placeholders = ", ".join(f"?::{cast(k)} AS {k}" for k in row)
    sql = f"SELECT coalesce(({CANDIDACY_LAST_NAME_VARIANT_GUARD}), false) FROM (SELECT {placeholders})"
    return duckdb.connect().execute(sql, list(row.values())).fetchone()[0]


def test_variant_guard_keeps_pair_without_conflict():
    assert _eval_variant_guard() is True


def test_variant_guard_rejects_district_conflict():
    """Same person filed in TN-5 and TN-7: the strong office JW must not bridge them."""
    assert _eval_variant_guard(district_identifier_r="7") is False


def test_variant_guard_rejects_seat_conflict():
    assert _eval_variant_guard(seat_name_l="1", seat_name_r="3") is False


def test_variant_guard_race_ids_differ():
    """Sources often carry different race ids for one race, so differing ids alone
    are not a conflict; differing ids plus a different office name are."""
    same_office = {"official_office_name_r": "u.s. house of representatives district 5"}
    assert _eval_variant_guard(br_race_id_l="2022090", **same_office) is True
    assert _eval_variant_guard(br_race_id_l="2022090") is False


def test_variant_guard_weak_office_needs_the_same_cleaned_tokens():
    weak = {"gamma_official_office_name": 2}

    def tokens(left, right):
        return {"official_office_name_tokens_l": left, "official_office_name_tokens_r": right}

    # Same place once filler, punctuation and codes are dropped.
    assert _eval_variant_guard(**weak, **tokens(["new", "plymouth", "#372"], ["new", "plymouth"])) is True
    assert (
        _eval_variant_guard(**weak, **tokens(["springfield", "(mahoning"], ["mahoning", "springfield"]))
        is True
    )
    # A shared place name with a different office, or only a shared state.
    assert _eval_variant_guard(**weak, **tokens(["sevier"], ["sevier", "deeds"])) is False
    assert (
        _eval_variant_guard(**weak, **tokens(["north", "richland", "hills"], ["richland", "hills"])) is False
    )
    assert (
        _eval_variant_guard(**weak, **tokens(["florida", "lieutenant"], ["florida", "congressional"]))
        is False
    )
    assert _eval_variant_guard(**weak, **tokens(["florida"], ["florida"])) is False
    assert _eval_variant_guard(**weak, **tokens(None, ["florida"])) is False
    # A shared race id needs no office agreement.
    assert _eval_variant_guard(**weak, br_race_id_l="2022089", **tokens(["sevier"], ["deeds"])) is True


def test_variant_guard_exempts_the_last_name_change_rescue():
    rescued = {"gamma_email": 1, "gamma_first_name": 4, "br_race_id_l": "2022089"}
    assert _eval_variant_guard(**rescued, district_identifier_r="7") is True


def test_variant_guard_ignores_pairs_matched_on_the_surname_itself():
    assert _eval_variant_guard(gamma_last_name=4, district_identifier_r="7") is True
