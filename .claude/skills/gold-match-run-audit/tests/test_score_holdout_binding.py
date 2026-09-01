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


def write_meta(dirpath, truth_path, offices, arm="test-arm", answers_path=None):
    """The subset of the archived gate-meta shape score_holdout actually binds
    against. `answers_path=None` mirrors the archived legacy metas, which
    predate answers_sha256."""
    meta = {
        "arm": arm,
        "model_config": "bedrock",
        "school_whole_assertion_enabled": False,
        "instrument": {"sha256": hashlib.sha256(Path(truth_path).read_bytes()).hexdigest()},
        "offices": offices,
    }
    if answers_path is not None:
        meta["answers_sha256"] = hashlib.sha256(Path(answers_path).read_bytes()).hexdigest()
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
    meta = write_meta(tmp_path, truth, offices=len(answers), answers_path=answers_path)
    return truth, answers_path, meta


def build_failing_arm(tmp_path):
    """Challenger answers identically to January's own defaults: backlog ties
    January (equal is not better), so the strict-superiority gate FAILS."""
    rows = full_packet()
    answers = full_answers(rows)
    truth = write_packet(rows, tmp_path)
    answers_path = tmp_path / "answers.json"
    answers_path.write_text(json.dumps(answers))
    meta = write_meta(tmp_path, truth, offices=len(answers), answers_path=answers_path)
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


def test_meta_answers_sha_mismatch_refused(tmp_path):
    # Same instrument, same office count: exactly the same-size-arms swap the
    # digest exists to catch (the two archived gate arms share both).
    truth, answers, meta = build_passing_arm(tmp_path)
    meta_doc = json.loads(meta.read_text())
    meta_doc["answers_sha256"] = "0" * 64
    meta.write_text(json.dumps(meta_doc))
    with pytest.raises(ValueError, match="answers sha256"):
        score_holdout.main(
            ["--truth", str(truth), "--answers", str(answers), "--meta", str(meta), "--label", "t"]
        )


def test_legacy_meta_without_answers_sha_scores_with_weak_notice(tmp_path, capsys):
    # The archived gate metas predate answers_sha256 and must keep scoring.
    truth, answers, meta = build_passing_arm(tmp_path)
    meta_doc = json.loads(meta.read_text())
    del meta_doc["answers_sha256"]
    meta.write_text(json.dumps(meta_doc))
    with pytest.raises(SystemExit) as exc:
        score_holdout.main(
            ["--truth", str(truth), "--answers", str(answers), "--meta", str(meta), "--label", "t"]
        )
    assert exc.value.code == 0
    assert "WEAK (legacy meta" in capsys.readouterr().out


def test_surplus_answer_id_refused(tmp_path):
    # A missing-only check would accept a file carrying extra offices (a
    # production run's rows); exact set equality refuses both directions.
    truth, answers, meta = build_passing_arm(tmp_path)
    payload = json.loads(answers.read_text())
    surplus_row = dict(payload[0])
    surplus_row["br_database_id"] = 999999
    payload.append(surplus_row)
    answers.write_text(json.dumps(payload))
    meta_doc = json.loads(meta.read_text())
    meta_doc["offices"] = len(payload)  # keep the count check satisfied
    meta_doc["answers_sha256"] = hashlib.sha256(answers.read_bytes()).hexdigest()
    meta.write_text(json.dumps(meta_doc))
    with pytest.raises(ValueError, match="surplus"):
        score_holdout.main(
            ["--truth", str(truth), "--answers", str(answers), "--meta", str(meta), "--label", "t"]
        )
