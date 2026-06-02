"""Tests for analytics/lib/win_analysis.py helpers."""

import math

import pytest
import win_analysis as wa


def test_wilson_zero_n_returns_nan():
    point, lo, hi = wa.wilson(0, 0)
    assert math.isnan(point)
    assert math.isnan(lo)
    assert math.isnan(hi)


def test_wilson_zero_successes():
    point, lo, hi = wa.wilson(0, 10)
    assert point == 0.0
    assert lo == 0.0
    assert 0.0 < hi < 100.0


def test_wilson_all_successes():
    point, lo, hi = wa.wilson(10, 10)
    assert point == 100.0
    assert hi == 100.0
    assert 0.0 < lo < 100.0


def test_wilson_midpoint_is_symmetric():
    point, lo, hi = wa.wilson(5, 10)
    assert point == 50.0
    assert lo < 50.0 < hi


@pytest.mark.parametrize("k,n", [(5, 3), (2, 1), (-1, 10)])
def test_wilson_rejects_k_outside_zero_to_n(k, n):
    with pytest.raises(ValueError):
        wa.wilson(k, n)


def test_win_event_predicate_sources_from_taxonomy_with_cutoff():
    pred = wa.win_event_predicate("2026-01-01")
    assert pred.startswith("event_type IN (")
    assert "int__amplitude_event_taxonomy" in pred
    assert "is_win" in pred
    assert "first_seen_date <= DATE'2026-01-01'" in pred


def test_win_event_predicate_default_cutoff():
    assert wa.DEFAULT_DRIFT_CUTOFF in wa.win_event_predicate()


@pytest.mark.parametrize("bad", ["2026/01/01", "not-a-date", "2026-1-1", ""])
def test_win_event_predicate_rejects_bad_cutoff(bad):
    with pytest.raises(ValueError):
        wa.win_event_predicate(bad)
