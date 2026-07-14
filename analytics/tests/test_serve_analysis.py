"""Tests for analytics/lib/serve_analysis.py helpers."""

import pandas as pd
import pytest
import serve_analysis as sa

COHORTS = {
    "eo": {
        "filter": "eo_activated_at IS NOT NULL",
    }
}


def _stub(df):
    """Return a run_query stub that ignores the SQL and returns a fixed DataFrame."""
    return lambda _sql: df.copy()


def _capture(df, captured):
    """Return a run_query stub that records the SQL it was given."""

    def run(sql):
        captured.append(sql)
        return df.copy()

    return run


def test_predicate_covers_all_three_surface_tests():
    pred = sa.serve_engagement_predicate()
    assert "family = 'serve'" in pred
    for prefix in sa.SERVE_EVENT_PREFIXES:
        assert f"'{prefix}%'" in pred
    assert "'Viewed'" in pred
    for path in sa.SERVE_SURFACE_PATH_PREFIXES:
        assert f"'{path}%'" in pred


def test_predicate_respects_alias():
    pred = sa.serve_engagement_predicate("ev")
    assert "ev.event_type" in pred
    assert "e.event_type" not in pred.replace("ev.event_type", "")


def test_working_set_sql_applies_structural_exclusions():
    captured = []
    df_in = pd.DataFrame({"user_id": [1], "cohort": ["eo"], "engaged_distinct_types": [0]})
    sa.build_serve_working_set(_capture(df_in, captured), COHORTS)
    sql = captured[0]
    # In-session only, impersonation taint excluded, internal emails excluded,
    # population restricted to serve users, default anchor applied.
    assert "session_id != -1" in sql
    assert "/impersonate" in sql and "/admin%" in sql
    assert "@goodparty.org" in sql
    assert "is_serve_user" in sql
    assert "eo_activated_at AS anchor" in sql


def test_working_set_custom_anchor_overrides_default():
    captured = []
    df_in = pd.DataFrame({"user_id": [1], "cohort": ["c"], "engaged_distinct_types": [0]})
    cohorts = {"c": {"filter": "TRUE", "anchor": "registered_at"}}
    sa.build_serve_working_set(_capture(df_in, captured), cohorts)
    assert "registered_at AS anchor" in captured[0]


def test_working_set_beyond_first_touch_boundary():
    df_in = pd.DataFrame({"user_id": [1, 2], "cohort": ["eo", "eo"], "engaged_distinct_types": [1, 2]})
    out = sa.build_serve_working_set(_stub(df_in), COHORTS)
    assert list(out["beyond_first_touch"]) == [0, 1]


def test_working_set_keeps_zero_event_user():
    df_in = pd.DataFrame({"user_id": [1], "cohort": ["eo"], "engaged_distinct_types": [0]})
    out = sa.build_serve_working_set(_stub(df_in), COHORTS)
    assert len(out) == 1
    assert out.loc[0, "beyond_first_touch"] == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"event_floor": "bad"},
        {"slice_dims": ("ok", "bad; DROP")},
    ],
)
def test_working_set_rejects_bad_params(kwargs):
    with pytest.raises(ValueError):
        sa.build_serve_working_set(_stub(pd.DataFrame()), COHORTS, **kwargs)


def test_wilson_is_shared_from_win_analysis():
    import win_analysis as wa

    assert sa.wilson is wa.wilson
