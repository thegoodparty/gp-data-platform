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


def test_token_totals_sums_usage():
    records = [
        {
            "type": "assistant",
            "message": {
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 10,
                    "cache_creation_input_tokens": 5,
                    "cache_read_input_tokens": 1,
                }
            },
        },
        {"type": "user", "message": {"content": "hi"}},
        {
            "type": "assistant",
            "message": {
                "usage": {
                    "input_tokens": 200,
                    "output_tokens": 20,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 50,
                }
            },
        },
    ]
    assert pp._token_totals(records) == {
        "input_tokens": 300,
        "output_tokens": 30,
        "cache_creation_input_tokens": 5,
        "cache_read_input_tokens": 51,
    }


def test_token_totals_empty_slice_is_zeros():
    assert pp._token_totals([]) == {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
    }


def test_split_time_attributes_idle_before_human_and_active_otherwise():
    records = [
        {"type": "assistant", "timestamp": "2026-06-08T19:00:00Z", "message": {"content": "q?"}},
        # 120s waiting on the human -> human_idle
        {"type": "user", "timestamp": "2026-06-08T19:02:00Z", "message": {"content": "answer"}},
        # 30s of model/tool work -> model_active
        {"type": "assistant", "timestamp": "2026-06-08T19:02:30Z", "message": {"content": "done"}},
    ]
    active, idle = pp._split_time(records)
    assert idle == 120.0
    assert active == 30.0


def test_split_time_single_record_is_zero():
    rec = [{"type": "assistant", "timestamp": "2026-06-08T19:00:00Z", "message": {"content": "x"}}]
    assert pp._split_time(rec) == (0.0, 0.0)


def _tool_use_rec(ts, name, tid, **inp):
    return {
        "type": "assistant",
        "timestamp": ts,
        "message": {"content": [{"type": "tool_use", "name": name, "id": tid, "input": inp}]},
    }


def _tool_result_rec(ts, tid):
    return {
        "type": "user",
        "timestamp": ts,
        "message": {"content": [{"type": "tool_result", "tool_use_id": tid}]},
    }


def test_marker_indices_finds_first_of_each():
    records = [
        _tool_use_rec("2026-06-08T19:00:00Z", "Skill", "s1", skill="win-analytics-process"),
        _tool_use_rec("2026-06-08T19:05:00Z", "Write", "w1", file_path="/x/q_brief.yaml"),
        _tool_use_rec("2026-06-08T19:06:00Z", "Agent", "a1", subagent_type="product-data-scientist"),
        _tool_use_rec("2026-06-08T19:09:00Z", "Write", "c1", file_path="/r/CALIBRATION_2026-06-08.md"),
    ]
    idx = pp._marker_indices(records)
    assert idx == {"skill_load": 0, "brief_write": 1, "reviewer_dispatch": 2, "calibration_write": 3}


def test_reviewer_result_end_index_uses_last_matching_result():
    records = [
        _tool_use_rec("2026-06-08T19:06:00Z", "Agent", "a1", subagent_type="product-data-scientist"),
        _tool_use_rec("2026-06-08T19:06:05Z", "Agent", "a2", subagent_type="product-manager"),
        _tool_result_rec("2026-06-08T19:07:00Z", "a1"),
        _tool_result_rec("2026-06-08T19:08:00Z", "a2"),  # later result -> max-span end
        {"type": "assistant", "timestamp": "2026-06-08T19:08:30Z", "message": {"content": "ok"}},
    ]
    assert pp._reviewer_result_end_index(records) == 3


def test_reviewer_result_end_index_none_when_no_reviewers():
    records = [{"type": "assistant", "timestamp": "2026-06-08T19:00:00Z", "message": {"content": "x"}}]
    assert pp._reviewer_result_end_index(records) is None
