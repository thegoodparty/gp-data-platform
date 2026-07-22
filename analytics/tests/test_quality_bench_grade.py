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
