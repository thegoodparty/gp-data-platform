"""Binding tests for score_holdout.py's exit-code and meta-artifact contract.

Reuses test_score_holdout's packet/answers builders (full_packet, full_answers,
write_packet) rather than inventing new ones; only the meta-file builder is new
here, since no prior test exercised --meta at all. Calls score_holdout.main()
directly (argv list, in-process) rather than via subprocess, so a raised
ValueError is catchable with pytest.raises instead of a stderr string match.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

SKILL_ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SKILL_ROOT))
sys.path.insert(0, str(TESTS_DIR))

import score_holdout  # noqa: E402
from test_score_holdout import full_answers, full_packet, write_packet  # noqa: E402


def write_meta(dirpath, truth_path, offices, arm="test-arm"):
    """The subset of the archived gate-meta shape score_holdout actually binds against."""
    meta = {
        "arm": arm,
        "model_config": "bedrock",
        "school_whole_assertion_enabled": False,
        "instrument": {"sha256": hashlib.sha256(Path(truth_path).read_bytes()).hexdigest()},
        "offices": offices,
    }
    path = Path(dirpath) / "meta.json"
    path.write_text(json.dumps(meta))
    return path


def build_passing_arm(tmp_path):
    """Challenger recovers row 1, ties January on row 3: backlog strictly
    improves, served nets zero -> PASS (mirrors test_complete_run_prints_verdict)."""
    rows = full_packet()
    answers = full_answers(rows, matched=(1, 3))
    truth = write_packet(rows, tmp_path)
    answers_path = tmp_path / "answers.json"
    answers_path.write_text(json.dumps(answers))
    meta = write_meta(tmp_path, truth, offices=len(answers))
    return truth, answers_path, meta


def build_failing_arm(tmp_path):
    """Challenger answers identically to January's own defaults: backlog ties
    January (equal is not better), so the strict-superiority gate FAILS."""
    rows = full_packet()
    answers = full_answers(rows)
    truth = write_packet(rows, tmp_path)
    answers_path = tmp_path / "answers.json"
    answers_path.write_text(json.dumps(answers))
    meta = write_meta(tmp_path, truth, offices=len(answers))
    return truth, answers_path, meta


def test_fail_verdict_exits_nonzero(tmp_path, capsys):
    # packet where the challenger loses the backlog gate -> Verdict: FAIL
    truth, answers, meta = build_failing_arm(tmp_path)
    with pytest.raises(SystemExit) as exc:
        score_holdout.main(
            ["--truth", str(truth), "--answers", str(answers), "--meta", str(meta), "--label", "t"]
        )
    assert exc.value.code == 1


def test_meta_required_with_answers(tmp_path):
    truth, answers, _ = build_passing_arm(tmp_path)
    with pytest.raises(SystemExit):  # argparse error exits 2
        score_holdout.main(["--truth", str(truth), "--answers", str(answers), "--label", "t"])


def test_meta_instrument_sha_mismatch_refused(tmp_path):
    truth, answers, meta = build_passing_arm(tmp_path)
    meta_doc = json.loads(meta.read_text())
    meta_doc["instrument"]["sha256"] = "0" * 64
    meta.write_text(json.dumps(meta_doc))
    with pytest.raises(ValueError, match="instrument sha256"):
        score_holdout.main(
            ["--truth", str(truth), "--answers", str(answers), "--meta", str(meta), "--label", "t"]
        )
