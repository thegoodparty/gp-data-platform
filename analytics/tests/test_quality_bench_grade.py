import json

import pandas as pd
from quality_bench import grade
from quality_bench.bank import Question


def rows_for(question_ids, arm, passes: bool, sev1: bool = False, reps=(1, 2, 3)):
    return [
        {
            "run_id": f"{q}__{arm}__r{r}",
            "question_id": q,
            "arm": arm,
            "rep": r,
            "results_block_ok": True,
            "numbers_pass": passes,
            "sources_pass": True,
            "severity1_tripped": sev1,
            "assumptions_pass": True,
            "resolutions_match": True,
            "judge_severity1_found": False,
        }
        for q in question_ids
        for r in reps
    ]


QUESTIONS = [
    Question(f"q0{i}", "win", "denominator" if i % 2 else "none", "calibration", "", "") for i in range(1, 9)
]
QIDS = [q.id for q in QUESTIONS]


def cells(arms, consistent=True):
    return {(q, a): {"consistent": consistent} for q in QIDS for a in arms}


def test_rule1_passes_when_full_arm_clean():
    df = pd.DataFrame(rows_for(QIDS, "full", True) + rows_for(QIDS, "bare", False))
    v = grade.evaluate_verdicts(df, cells(["full", "bare"]), QUESTIONS)
    assert v["rule1_trustworthy"] is True
    assert v["rule2_earns_overhead"] is True


def test_rule1_fails_on_single_severity1():
    rows = rows_for(QIDS, "full", True)
    rows[0]["severity1_tripped"] = True
    df = pd.DataFrame(rows + rows_for(QIDS, "bare", False))
    v = grade.evaluate_verdicts(df, cells(["full", "bare"]), QUESTIONS)
    assert v["rule1_trustworthy"] is False


def test_rule2_fails_when_bare_matches_on_traps():
    df = pd.DataFrame(rows_for(QIDS, "full", True) + rows_for(QIDS, "bare", True))
    v = grade.evaluate_verdicts(df, cells(["full", "bare"]), QUESTIONS)
    assert v["rule2_earns_overhead"] is False


def test_rule1_fails_on_judge_only_severity1():
    """judge_severity1_found=True must trip rule 1 even when the deterministic
    severity1_tripped column is clean on every row."""
    rows = rows_for(QIDS, "full", True)  # all deterministic checks pass
    rows[0]["judge_severity1_found"] = True  # judge flags one rep
    df = pd.DataFrame(rows + rows_for(QIDS, "bare", False))
    v = grade.evaluate_verdicts(df, cells(["full", "bare"]), QUESTIONS)
    assert v["rule1_trustworthy"] is False
    assert "severity1=1" in v["rule1_detail"]


def test_rule2_inapplicable_without_trap_questions():
    """No trap questions in the batch -> rule 2 is None (inapplicable), not a
    failed 0 > 0 comparison."""
    no_trap = [q for q in QUESTIONS if q.trap == "none"][:1]
    qid = no_trap[0].id
    df = pd.DataFrame(rows_for([qid], "full", True) + rows_for([qid], "bare", True))
    v = grade.evaluate_verdicts(df, {(qid, a): {"consistent": True} for a in ("full", "bare")}, no_trap)
    assert v["rule2_earns_overhead"] is None


def test_rule3_flags_bloat_when_knowledge_matches_full():
    df = pd.DataFrame(rows_for(QIDS, "full", True) + rows_for(QIDS, "knowledge", True))
    v = grade.evaluate_verdicts(df, cells(["full", "knowledge"]), QUESTIONS)
    assert v["rule3_bloat_signal"] is True


def test_question_with_zero_ok_runs_still_counts_in_denominator():
    """A question absent from the graded df must still count against rule 1's
    8-question denominator, not be silently dropped."""
    # 7 of 8 questions present and passing; q08 has no ok runs at all.
    df_7 = pd.DataFrame(rows_for(QIDS[:7], "full", True))
    v7 = grade.evaluate_verdicts(df_7, cells(["full"]), QUESTIONS)
    assert v7["rule1_trustworthy"] is True  # 7/8 satisfies "≥ N-1"
    assert "7/8" in v7["rule1_detail"]

    # Two questions missing (6/8) → rule 1 must fail, not pass on a shrunk denom.
    df_6 = pd.DataFrame(rows_for(QIDS[:6], "full", True))
    v6 = grade.evaluate_verdicts(df_6, cells(["full"]), QUESTIONS)
    assert v6["rule1_trustworthy"] is False
    assert "6/8" in v6["rule1_detail"]


def test_rule1_not_vacuous_without_full_arm_cells():
    """A batch where only the bare arm produced graded runs (e.g. every full run
    failed) must not report trustworthy on empty full-arm evidence."""
    df = pd.DataFrame(rows_for(["q01"], "bare", True))
    v = grade.evaluate_verdicts(df, {("q01", "bare"): {"consistent": True}}, QUESTIONS[:1])
    assert v["rule1_trustworthy"] is False


def test_rule1_small_denominator_gets_no_free_slack():
    """With a 1-question batch, the proportional 7/8 bar rounds up to 1: the
    question must actually pass, not ride the old '>= N-1' slack."""
    df = pd.DataFrame(rows_for(["q01"], "full", False))
    v = grade.evaluate_verdicts(df, {("q01", "full"): {"consistent": True}}, QUESTIONS[:1])
    assert v["rule1_trustworthy"] is False


def test_rep_threshold_scales_with_reps():
    assert grade.rep_threshold(3) == 2
    assert grade.rep_threshold(6) == 4
    assert grade.rep_threshold(2) == 2
    assert grade.rep_threshold(1) == 1


def test_question_passes_uses_threshold():
    # 3 of 6 reps pass = 50%, below the 2/3 bar at reps=6.
    rows = rows_for(["q01"], "full", True) + rows_for(["q01"], "full", False, reps=(4, 5, 6))
    df = pd.DataFrame(rows)
    assert grade._question_passes(df, "full", "q01", grade.rep_threshold(6)) is False
    assert grade._question_passes(df, "full", "q01", grade.rep_threshold(3)) is True


def test_adherence_lines_report_per_cell_rates():
    rows = rows_for(["q01"], "full", True)
    for r in rows[1:]:
        r["sources_pass"] = False  # 2 of 3 reps skipped the mandatory source
    lines = grade.adherence_lines(pd.DataFrame(rows))
    assert any("q01 x full: sources 1/3, assumptions 3/3, resolutions 3/3" in line for line in lines)
    assert grade.adherence_lines(grade.scores_frame([])) == []


def test_scores_frame_empty_keeps_columns():
    """An all-failed batch grades zero rows; the frame must keep its columns so
    the report/verdict path still runs instead of crashing on sort_values."""
    df = grade.scores_frame([])
    assert df.empty
    assert list(df.columns) == grade.BASE_COLUMNS
    v = grade.evaluate_verdicts(df, {}, QUESTIONS[:1])
    assert v["rule1_trustworthy"] is False


def test_load_keys_for_batch_skips_unauthored_keys(tmp_path):
    """Keys are authored one gold run at a time, so the manifest registers key
    files that do not exist yet; grading a batch that never ran those questions
    must not touch their missing key files (PR #660)."""
    (tmp_path / "q01_key.yaml").write_text(
        'id: q01\nas_of: "2026-07-21"\n' "numbers:\n  - name: n\n    value: 1\n    tolerance_pct: 1.0\n"
    )
    questions = [
        Question("q01", "win", "none", "calibration", "q01.md", "q01_key.yaml"),
        Question("q03", "win", "point_in_time", "holdout", "q03.md", "q03_key.yaml"),
    ]
    keys = grade.load_keys_for_batch(tmp_path, questions, {"q01"})
    assert set(keys) == {"q01"}


def test_grade_run_carries_execution_meta(tmp_path):
    """Meta recorded by run_matrix flows through grade_run into the scores row;
    a legacy state.json without meta yields the same columns as None."""
    from quality_bench.bank import Key, NumberSpec

    key = Key(id="q01", as_of="2026-07-24", numbers=[NumberSpec("n", 1.0, 5)])
    answer = tmp_path / "a.md"
    answer.write_text("no results block")
    run_state = {
        "run_id": "q01__full__r1",
        "ok": True,
        "answer_file": str(answer),
        "transcript_file": None,
        "meta": {"total_cost_usd": 2.5, "output_tokens": 999, "num_turns": 4},
    }
    row = grade.grade_run(run_state, key)
    assert row["cost_usd"] == 2.5
    assert row["output_tokens"] == 999
    assert row["num_turns"] == 4

    legacy = {**run_state}
    legacy.pop("meta")
    row = grade.grade_run(legacy, key)
    assert row["cost_usd"] is None and row["output_tokens"] is None and row["num_turns"] is None


def test_cost_lines_report_per_arm_totals():
    df = pd.DataFrame(
        [
            {"arm": "full", "cost_usd": 2.0, "output_tokens": 100, "num_turns": 5},
            {"arm": "full", "cost_usd": 4.0, "output_tokens": 300, "num_turns": 15},
            {"arm": "bare", "cost_usd": 1.0, "output_tokens": 50, "num_turns": 2},
        ]
    )
    lines = grade.cost_lines(df)
    text = "\n".join(lines)
    assert "full" in text and "bare" in text
    assert "$6.00" in text  # full total
    assert "200" in text  # full mean output tokens


def test_cost_lines_empty_without_meta():
    df = pd.DataFrame([{"arm": "full"}])  # no meta columns at all (legacy batch)
    assert grade.cost_lines(df) == []


JUDGE_VERDICT = {
    "scores": {
        "scoping_correctness": 2,
        "confident_wrongness": 2,
        "caveat_quality": 1,
        "assumptions_surfaced": 2,
    },
    "severity1_found": False,
    "rationale": "fine",
}


def _key(qid="q01"):
    from quality_bench.bank import Key, NumberSpec

    return Key(id=qid, as_of="2026-07-24", numbers=[NumberSpec("n", 1.0, 5)])


def test_judge_cached_persists_provenance_and_reuses(tmp_path, monkeypatch):
    """First call judges and persists a provenance record (DATA-2165 item 3);
    second call reuses the file without re-invoking the judge."""
    calls = []

    def fake_judge(key, answer, model):
        calls.append(model)
        return dict(JUDGE_VERDICT)

    monkeypatch.setattr(grade.judge_mod, "judge_answer", fake_judge)
    j1 = grade.judge_cached(tmp_path, "q01__full__r1", _key(), "the answer", "m1")
    assert j1["scores"]["caveat_quality"] == 1
    record = json.loads((tmp_path / "q01__full__r1.judge.json").read_text())
    assert record["model"] == "m1"
    assert record["key_id"] == "q01"
    assert record["answer_sha256"]
    j2 = grade.judge_cached(tmp_path, "q01__full__r1", _key(), "the answer", "m1")
    assert calls == ["m1"]
    assert j2["scores"] == j1["scores"]


def test_judge_cached_rejudges_when_answer_changed(tmp_path, monkeypatch):
    """A cached verdict for a different answer text must not be reused."""
    calls = []

    def fake_judge(key, answer, model):
        calls.append(answer)
        return dict(JUDGE_VERDICT)

    monkeypatch.setattr(grade.judge_mod, "judge_answer", fake_judge)
    grade.judge_cached(tmp_path, "q01__full__r1", _key(), "answer v1", "m")
    grade.judge_cached(tmp_path, "q01__full__r1", _key(), "answer v2", "m")
    assert calls == ["answer v1", "answer v2"]


def test_judge_cached_rejudge_flag_forces(tmp_path, monkeypatch):
    calls = []

    def fake_judge(key, answer, model):
        calls.append(1)
        return dict(JUDGE_VERDICT)

    monkeypatch.setattr(grade.judge_mod, "judge_answer", fake_judge)
    grade.judge_cached(tmp_path, "q01__full__r1", _key(), "a", "m")
    grade.judge_cached(tmp_path, "q01__full__r1", _key(), "a", "m", rejudge=True)
    assert len(calls) == 2


def test_judge_cached_failed_judge_not_persisted(tmp_path, monkeypatch):
    monkeypatch.setattr(grade.judge_mod, "judge_answer", lambda *a: None)
    assert grade.judge_cached(tmp_path, "q01__full__r1", _key(), "a", "m") is None
    assert not (tmp_path / "q01__full__r1.judge.json").exists()


def test_judge_cached_corrupt_cache_rejudges(tmp_path, monkeypatch):
    """A truncated judge.json (crash mid-write) is a cache miss, not a crash:
    grading must re-judge and overwrite instead of aborting the batch
    (delegate, PR #686)."""
    calls = []

    def fake_judge(key, answer, model):
        calls.append(1)
        return dict(JUDGE_VERDICT)

    monkeypatch.setattr(grade.judge_mod, "judge_answer", fake_judge)
    judge_file = tmp_path / "q01__full__r1.judge.json"
    judge_file.write_text('{"scores": {"scop')  # truncated write
    j = grade.judge_cached(tmp_path, "q01__full__r1", _key(), "the answer", "m")
    assert j["scores"]["caveat_quality"] == 1
    assert calls == [1]
    assert json.loads(judge_file.read_text())["answer_sha256"]  # overwritten clean


def test_cost_lines_no_nan_when_tokens_missing():
    """cost_usd recorded but token/turn metadata absent must render n/a, not the
    literal string 'nan' (delegate, PR #686)."""
    df = pd.DataFrame([{"arm": "full", "cost_usd": 2.0, "output_tokens": None, "num_turns": None}]).astype(
        {"output_tokens": "float", "num_turns": "float"}
    )
    text = "\n".join(grade.cost_lines(df))
    assert "$2.00" in text
    assert "nan" not in text
    assert "n/a" in text


def test_grade_run_carries_cache_creation_and_api_duration(tmp_path):
    """cache_creation_input_tokens drives per-arm cost differences and
    duration_api_ms is the pure-API latency; both must reach scores.csv
    (delegate ce8e2bf0, PR #686)."""
    answer = tmp_path / "a.md"
    answer.write_text("x")
    run_state = {
        "run_id": "q01__full__r1",
        "ok": True,
        "answer_file": str(answer),
        "transcript_file": None,
        "meta": {"cache_creation_input_tokens": 777, "duration_api_ms": 1234},
    }
    row = grade.grade_run(run_state, _key())
    assert row["cache_creation_input_tokens"] == 777
    assert row["duration_api_ms"] == 1234
    assert "cache_creation_input_tokens" in grade.BASE_COLUMNS
    assert "duration_api_ms" in grade.BASE_COLUMNS


def test_cost_lines_mixed_arms_no_nan_cost():
    """One arm with cost, another all-NaN (mixed-metadata batch): the NaN arm
    must render n/a, not '$nan' mean or a misleading '$0.00' total
    (delegate 8016714b/325f2079, PR #686)."""
    df = pd.DataFrame(
        [
            {"arm": "full", "cost_usd": 2.0, "output_tokens": 100.0, "num_turns": 5.0},
            {"arm": "bare", "cost_usd": None, "output_tokens": None, "num_turns": None},
        ]
    ).astype({"cost_usd": "float", "output_tokens": "float", "num_turns": "float"})
    text = "\n".join(grade.cost_lines(df))
    bare_line = next(line for line in text.splitlines() if line.startswith("- bare"))
    assert "nan" not in bare_line
    assert "$0.00" not in bare_line
    assert "n/a" in bare_line
    assert "$2.00" in text  # full arm still reports
