# analytics/diagnostics/quality_bench/grade.py
"""Combine the grading layers into scores.csv + report.md, and evaluate the
pre-registered verdict rules (design §8). Judge scores never override
deterministic failures: numbers_pass/sources_pass/severity1_tripped come from
grading.py regardless of judge output."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

try:
    from quality_bench import grading
    from quality_bench import judge as judge_mod
    from quality_bench.bank import Key, Question, load_key, load_manifest
except ImportError:  # bare `python grade.py`: diagnostics/ isn't on sys.path yet
    import sys

    sys.path.insert(0, str(Path(__file__).parents[1]))
    from quality_bench import grading
    from quality_bench import judge as judge_mod
    from quality_bench.bank import Key, Question, load_key, load_manifest


def grade_run(run_state: dict, key: Key) -> dict:
    run_id = run_state["run_id"]
    # Question ids are constrained (manifest convention) not to contain "__",
    # so splitting on it to recover question_id/arm/rep is unambiguous.
    question_id, arm, rep = run_id.split("__")
    answer = Path(run_state["answer_file"]).read_text() if run_state.get("answer_file") else ""
    transcript = Path(run_state["transcript_file"]).read_text() if run_state.get("transcript_file") else ""
    block = grading.parse_results_block(answer)
    numbers = grading.check_numbers(block, key)
    sources = grading.check_sources(transcript, key)
    sev1 = grading.check_severity1(answer, key)
    return {
        "run_id": run_id,
        "question_id": question_id,
        "arm": arm,
        "rep": int(rep.lstrip("r")),
        "results_block_ok": block is not None,
        "numbers_pass": all(c.passed for c in numbers),
        "sources_pass": all(c.passed for c in sources),
        "severity1_tripped": any(not c.passed for c in sev1),
        "_block": block,
    }


def _question_passes(df: pd.DataFrame, arm: str, qid: str) -> bool:
    reps = df[(df.arm == arm) & (df.question_id == qid)]
    return int(reps.numbers_pass.sum()) >= 2


def _pass_count(df: pd.DataFrame, arm: str, qids: list[str]) -> int:
    return sum(
        _question_passes(df, arm, q) for q in qids if not df[(df.arm == arm) & (df.question_id == q)].empty
    )


def evaluate_verdicts(df: pd.DataFrame, cells: dict, questions: list[Question]) -> dict:
    qids = [q.id for q in questions]
    trap_qids = [q.id for q in questions if q.trap != "none"]
    full_pass = _pass_count(df, "full", qids)
    full_runs = df[df.arm == "full"]
    sev1_count = int(full_runs.severity1_tripped.sum()) + int(
        full_runs.get("judge_severity1_found", pd.Series(dtype=bool)).fillna(False).sum()
    )
    full_cells_ok = all(c["consistent"] for (q, a), c in cells.items() if a == "full")
    rule1 = full_pass >= len(qids) - 1 and sev1_count == 0 and full_cells_ok

    rule2 = None
    if "bare" in set(df.arm):
        rule2 = _pass_count(df, "full", trap_qids) > _pass_count(df, "bare", trap_qids)

    rule3 = None
    if "knowledge" in set(df.arm):
        k_cells = sum(c["consistent"] for (q, a), c in cells.items() if a == "knowledge")
        f_cells = sum(c["consistent"] for (q, a), c in cells.items() if a == "full")
        rule3 = _pass_count(df, "knowledge", qids) >= full_pass and k_cells >= f_cells

    return {
        "rule1_trustworthy": rule1,
        "rule1_detail": f"full passes {full_pass}/{len(qids)} questions; severity1={sev1_count}; "
        f"cells_consistent={full_cells_ok}",
        "rule2_earns_overhead": rule2,
        "rule3_bloat_signal": rule3,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--judge", action="store_true")
    parser.add_argument("--model", default="claude-fable-5")
    args = parser.parse_args()

    here = Path(__file__).parent
    batch_dir = here / "results" / args.batch
    state = json.loads((batch_dir / "state.json").read_text())
    questions = load_manifest(here / "questions" / "manifest.yaml")
    keys = {q.id: load_key(here / "keys" / q.key_file) for q in questions}

    rows, blocks = [], {}
    for run_id, run_state in state["runs"].items():
        if not run_state.get("ok"):
            continue
        qid = run_id.split("__")[0]
        row = grade_run(run_state, keys[qid])
        blocks.setdefault((row["question_id"], row["arm"]), []).append(row.pop("_block"))
        if args.judge:
            answer = Path(run_state["answer_file"]).read_text()
            j = judge_mod.judge_answer(keys[qid], answer, args.model)
            if j:
                row.update({f"judge_{k}": v for k, v in j["scores"].items()})
                row["judge_severity1_found"] = bool(j["severity1_found"])
        rows.append(row)

    df = pd.DataFrame(rows).sort_values("run_id")
    df.to_csv(batch_dir / "scores.csv", index=False)

    cells = {ck: grading.cell_consistency(bs, keys[ck[0]]) for ck, bs in blocks.items()}
    verdicts = evaluate_verdicts(df, cells, [q for q in questions if q.id in set(df.question_id)])

    lines = [f"# Quality bench report — batch {args.batch}", ""]
    for k, v in verdicts.items():
        lines.append(f"- **{k}**: {v}")
    lines += ["", "## Cells (question x arm consistency)", ""]
    for (qid, arm), c in sorted(cells.items()):
        lines.append(
            f"- {qid} x {arm}: consistent={c['consistent']} "
            f"max_spread={c['max_spread_pct']:.2f}% parsed {c['n_parsed']}/{c['n_reps']}"
        )
    (batch_dir / "report.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
