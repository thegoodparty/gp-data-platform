"""Tests for analytics/diagnostics/pipeline_profiler.py."""

from datetime import datetime

import pipeline_profiler as pp


def test_module_exposes_stage_names():
    assert pp.STAGES == ["framing", "execution", "review", "calibration"]


def test_parse_ts_handles_z_suffix():
    assert pp._parse_ts("2026-06-08T19:45:47Z") == datetime.fromisoformat("2026-06-08T19:45:47+00:00")


def test_parse_ts_handles_offset():
    assert pp._parse_ts("2026-06-08T19:45:47.123+00:00").year == 2026


def test_is_human_message_true_for_string_content():
    rec = {"type": "user", "message": {"content": "hello"}}
    assert pp._is_human_message(rec) is True


def test_is_human_message_false_for_tool_result():
    rec = {"type": "user", "message": {"content": [{"type": "tool_result", "tool_use_id": "x"}]}}
    assert pp._is_human_message(rec) is False


def test_is_human_message_false_for_meta():
    rec = {"type": "user", "isMeta": True, "message": {"content": "system reminder"}}
    assert pp._is_human_message(rec) is False


def test_is_human_message_false_for_assistant():
    rec = {"type": "assistant", "message": {"content": "hi"}}
    assert pp._is_human_message(rec) is False
