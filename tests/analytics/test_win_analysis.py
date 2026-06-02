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
