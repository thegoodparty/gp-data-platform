"""Tests for analytics/diagnostics/pipeline_profiler.py."""

import pipeline_profiler as pp


def test_module_exposes_stage_names():
    assert pp.STAGES == ["framing", "execution", "review", "calibration"]
