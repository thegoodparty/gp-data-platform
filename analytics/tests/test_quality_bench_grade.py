import pandas as pd
from quality_bench import grade
from quality_bench.bank import Question


def rows_for(question_ids, arm, passes: bool, sev1: bool = False):
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
            "judge_severity1_found": False,
        }
        for q in question_ids
        for r in (1, 2, 3)
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


def test_rule3_flags_bloat_when_knowledge_matches_full():
    df = pd.DataFrame(rows_for(QIDS, "full", True) + rows_for(QIDS, "knowledge", True))
    v = grade.evaluate_verdicts(df, cells(["full", "knowledge"]), QUESTIONS)
    assert v["rule3_bloat_signal"] is True
