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


def test_iter_tool_uses_returns_name_input_id():
    rec = {
        "type": "assistant",
        "message": {
            "content": [
                {"type": "text", "text": "..."},
                {"type": "tool_use", "name": "Write", "id": "t1", "input": {"file_path": "/a/b_brief.yaml"}},
            ]
        },
    }
    assert pp._iter_tool_uses(rec) == [("Write", {"file_path": "/a/b_brief.yaml"}, "t1")]


def test_iter_tool_uses_empty_for_non_assistant():
    assert pp._iter_tool_uses({"type": "user", "message": {"content": "hi"}}) == []


def test_classify_marker_skill_load():
    assert pp._classify_marker("Skill", {"skill": "win-analytics-process"}) == "skill_load"


def test_classify_marker_brief_write():
    assert pp._classify_marker("Write", {"file_path": "/x/2026-06-09_q_brief.yaml"}) == "brief_write"


def test_classify_marker_reviewer_dispatch():
    assert pp._classify_marker("Agent", {"subagent_type": "product-data-scientist"}) == "reviewer_dispatch"


def test_classify_marker_calibration_write():
    assert pp._classify_marker("Write", {"file_path": "/r/CALIBRATION_2026-06-09.md"}) == "calibration_write"


def test_classify_marker_none_for_unrelated():
    assert pp._classify_marker("Bash", {"command": "ls"}) is None
    assert pp._classify_marker("Write", {"file_path": "/x/notes.md"}) is None
    assert pp._classify_marker("Agent", {"subagent_type": "Explore"}) is None
