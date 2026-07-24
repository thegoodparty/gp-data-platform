"""Tests for the per-batch isolation spot-check (README no-decision gate 2
interim: flag out-of-arm filesystem access in run transcripts)."""

import json
from pathlib import Path

from quality_bench import spot_check

CWD = "/Users/op/.cache/gp_quality_bench/arms/runs/b1/q01__full__r1"
HOME = "/Users/op/.cache/gp_quality_bench/arms/runs/b1/q01__full__r1.home"


def line(tool: str, tool_input: dict) -> str:
    return json.dumps(
        {
            "type": "assistant",
            "cwd": CWD,
            "message": {"content": [{"type": "tool_use", "name": tool, "input": tool_input}]},
        }
    )


def write_transcript(tmp_path: Path, lines: list[str]) -> Path:
    t = tmp_path / "t.jsonl"
    t.write_text("\n".join(lines))
    return t


def test_in_arm_reads_not_flagged(tmp_path):
    t = write_transcript(
        tmp_path,
        [
            line("Read", {"file_path": f"{CWD}/.claude/skills/x/SKILL.md"}),
            line("Read", {"file_path": f"{HOME}/.claude/settings.json"}),
            line("Read", {"file_path": "relative/path.md"}),
            line("Bash", {"command": "cat > /tmp/q.py <<'EOF'\nprint(1)\nEOF"}),
            line("Bash", {"command": "uv run python /tmp/q.py"}),
        ],
    )
    assert spot_check.flag_transcript(t) == []


def test_out_of_arm_read_flagged(tmp_path):
    keys_path = "/Users/op/repos/gp-data-platform/analytics/diagnostics/quality_bench/keys/q01_key.yaml"
    t = write_transcript(tmp_path, [line("Read", {"file_path": keys_path})])
    flags = spot_check.flag_transcript(t)
    assert len(flags) == 1
    assert keys_path in flags[0]


def test_sibling_run_read_flagged(tmp_path):
    sibling = "/Users/op/.cache/gp_quality_bench/arms/runs/b1/q02__full__r1/answer.md"
    t = write_transcript(tmp_path, [line("Read", {"file_path": sibling})])
    assert len(spot_check.flag_transcript(t)) == 1


def test_bash_command_touching_checkout_flagged(tmp_path):
    t = write_transcript(
        tmp_path,
        [line("Bash", {"command": "cat /Users/op/repos/gp-data-platform/analytics/lib/x.py"})],
    )
    flags = spot_check.flag_transcript(t)
    assert len(flags) == 1
    assert "gp-data-platform" in flags[0]


def test_system_paths_in_bash_not_flagged(tmp_path):
    t = write_transcript(
        tmp_path,
        [line("Bash", {"command": "/usr/bin/env python3 -c 'print(1)' > /dev/null 2>/tmp/err"})],
    )
    assert spot_check.flag_transcript(t) == []


def test_check_batch_maps_flags_by_run(tmp_path):
    batch = tmp_path / "batch"
    batch.mkdir()
    bad = "/Users/op/repos/gp-data-platform/secret.md"
    clean_t = write_transcript(tmp_path, [line("Read", {"file_path": f"{CWD}/CLAUDE.md"})])
    dirty = batch / "dirty.jsonl"
    dirty.write_text(line("Read", {"file_path": bad}))
    state = {
        "runs": {
            "q01__full__r1": {"ok": True, "transcript_file": str(clean_t)},
            "q01__full__r2": {"ok": True, "transcript_file": str(dirty)},
            "q01__full__r3": {"ok": True, "transcript_file": None},
        }
    }
    (batch / "state.json").write_text(json.dumps(state))
    result = spot_check.check_batch(batch)
    assert result["q01__full__r1"] == []
    assert len(result["q01__full__r2"]) == 1
    assert result["q01__full__r3"] == ["transcript missing"]
