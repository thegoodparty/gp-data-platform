# analytics/diagnostics/quality_bench/grade.py
"""Combine the grading layers into scores.csv + report.md, and evaluate the
pre-registered verdict rules (design §8). Judge scores never override
deterministic failures: numbers_pass/sources_pass/severity1_tripped come from
grading.py regardless of judge output."""

from __future__ import annotations

import argparse
import dataclasses
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
    # question_id/arm/rep are recovered by splitting run_id on "__". bank.load_manifest
    # rejects question ids containing "__", so this split is unambiguous.
    question_id, arm, rep = run_id.split("__")
    answer = Path(run_state["answer_file"]).read_text() if run_state.get("answer_file") else ""
    transcript_file = run_state.get("transcript_file")
    transcript = Path(transcript_file).read_text() if transcript_file else ""
    transcript_ok = bool(transcript_file) and bool(transcript.strip())
    block = grading.parse_results_block(answer)
    numbers = grading.check_numbers(block, key)
    sources = grading.check_sources(transcript, key)
    if key.mandatory_sources and not transcript_ok:
        # Sources already fail (empty transcript never matches), but say why so a
        # missing/empty transcript isn't mistaken for a genuine adherence miss.
        sources = [dataclasses.replace(c, detail="transcript missing") for c in sources]
    sev1 = grading.check_severity1(answer, key)
    return {
        "run_id": run_id,
        "question_id": question_id,
        "arm": arm,
        "rep": int(rep.lstrip("r")),
        "results_block_ok": block is not None,
        "transcript_ok": transcript_ok,
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
    judge_eligible = judge_ok = 0
    for run_id, run_state in state["runs"].items():
        if not run_state.get("ok"):
            continue
        qid = run_id.split("__")[0]
        row = grade_run(run_state, keys[qid])
        blocks.setdefault((row["question_id"], row["arm"]), []).append(row.pop("_block"))
        if args.judge:
            judge_eligible += 1
            answer = Path(run_state["answer_file"]).read_text()
            j = judge_mod.judge_answer(keys[qid], answer, args.model)
            if j:
                judge_ok += 1
                row.update({f"judge_{k}": v for k, v in j["scores"].items()})
                row["judge_severity1_found"] = bool(j["severity1_found"])
            else:
                print(f"{run_id}: judge FAILED")
        rows.append(row)

    df = pd.DataFrame(rows).sort_values("run_id")
    df.to_csv(batch_dir / "scores.csv", index=False)

    # Coverage denominator = every question with ANY recorded run, not only the
    # ok ones. A question whose runs all failed must still count against rule 1,
    # otherwise a silent drop would inflate the pass rate.
    qids_in_state = {rid.split("__")[0] for rid in state["runs"]}
    denom_questions = [q for q in questions if q.id in qids_in_state]
    cells = {ck: grading.cell_consistency(bs, keys[ck[0]]) for ck, bs in blocks.items()}
    verdicts = evaluate_verdicts(df, cells, denom_questions)

    graded = len(rows)
    recorded = len(state["runs"])
    lines = [f"# Quality bench report — batch {args.batch}", ""]
    lines.append(
        f"Coverage: graded {graded} of {recorded} recorded runs; "
        f"questions in rule-1 denominator: {len(denom_questions)}"
    )
    if args.judge:
        lines.append(f"Judged: {judge_ok}/{judge_eligible}")
    if graded < recorded:
        graded_ids = {r["run_id"] for r in rows}
        missing_by_arm: dict[str, int] = {}
        for rid in state["runs"]:
            if rid not in graded_ids:
                missing_by_arm[rid.split("__")[1]] = missing_by_arm.get(rid.split("__")[1], 0) + 1
        lines.append(
            "Missing runs by arm: " + ", ".join(f"{arm}={n}" for arm, n in sorted(missing_by_arm.items()))
        )
    lines.append("")
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
