# analytics/diagnostics/quality_bench/grade.py
"""Combine the grading layers into scores.csv + report.md, and evaluate the
pre-registered verdict rules (design §8). Judge scores never override
deterministic failures: numbers_pass/sources_pass/severity1_tripped come from
grading.py regardless of judge output."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
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


def load_keys_for_batch(keys_dir: Path, questions: list[Question], qids_in_batch: set[str]) -> dict[str, Key]:
    """Load only the keys for questions actually present in the batch.

    Keys are authored one gold run at a time, so the manifest may register
    key files that do not exist yet; a batch that never ran those questions
    must still grade (PR #660 review).
    """
    return {
        q.id: load_key(keys_dir / q.key_file, expected_id=q.id) for q in questions if q.id in qids_in_batch
    }


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
    assumptions = grading.check_assumptions(block, key)
    resolutions = grading.check_resolutions(block, key)
    meta = run_state.get("meta") or {}
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
        "assumptions_pass": all(c.passed for c in assumptions),
        "resolutions_match": all(c.passed for c in resolutions),
        # Execution metadata recorded by run_matrix (None on legacy batches).
        "cost_usd": meta.get("total_cost_usd"),
        "num_turns": meta.get("num_turns"),
        "duration_ms": meta.get("duration_ms"),
        "duration_api_ms": meta.get("duration_api_ms"),
        "input_tokens": meta.get("input_tokens"),
        "output_tokens": meta.get("output_tokens"),
        "cache_read_input_tokens": meta.get("cache_read_input_tokens"),
        "cache_creation_input_tokens": meta.get("cache_creation_input_tokens"),
        "_block": block,
    }


BASE_COLUMNS = [
    "run_id",
    "question_id",
    "arm",
    "rep",
    "results_block_ok",
    "transcript_ok",
    "numbers_pass",
    "sources_pass",
    "severity1_tripped",
    "assumptions_pass",
    "resolutions_match",
    "cost_usd",
    "num_turns",
    "duration_ms",
    "duration_api_ms",
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
]


def judge_cached(
    batch_dir: Path, run_id: str, key: Key, answer: str, model: str, rejudge: bool = False
) -> dict | None:
    """Judge with persisted provenance (DATA-2165 item 3): the verdict is
    written to <run_id>.judge.json alongside the answer hash, key id, and judge
    model, and reused on regrade instead of re-invoking the judge. The cache is
    keyed on the answer text (sha256) and model, so a changed answer or a
    different judge model re-judges; --rejudge forces it."""
    judge_file = batch_dir / f"{run_id}.judge.json"
    answer_sha256 = hashlib.sha256(answer.encode()).hexdigest()
    if rejudge:
        # --rejudge means the cached verdict is distrusted: drop it up front so
        # a failed re-judge cannot leave it behind for the next normal run to
        # silently reuse.
        judge_file.unlink(missing_ok=True)
    if judge_file.exists() and not rejudge:
        try:
            record = json.loads(judge_file.read_text())
        except (json.JSONDecodeError, OSError):
            # A truncated cache file (crash mid-write) is a cache miss, not a
            # reason to abort the batch; fall through and re-judge over it.
            record = {}
        if record.get("answer_sha256") == answer_sha256 and record.get("model") == model:
            return record
    j = judge_mod.judge_answer(key, answer, model)
    if j is None:
        return None
    record = {**j, "model": model, "key_id": key.id, "answer_sha256": answer_sha256}
    judge_file.write_text(json.dumps(record, indent=2))
    return record


def cost_lines(df: pd.DataFrame) -> list[str]:
    """Per-arm execution cost/size summary (DATA-2165 item 4). Reported next to
    the verdicts but never gated into them: rule 3's pre-registered criterion is
    pass-rate + consistency; cost is the context that makes a prune proposal's
    token delta concrete. Empty on legacy batches with no recorded meta."""
    if "cost_usd" not in df.columns or df.cost_usd.isna().all():
        return []
    lines = ["", "## Execution cost by arm (reported, not verdict-gated)", ""]
    for arm, g in df.groupby("arm"):
        # Guards are per-arm, not per-frame: a mixed-metadata batch can have one
        # arm with recorded cost and another all-NaN, and an all-NaN group would
        # format as '$nan' mean / misleading '$0.00' skipna total.
        cost_total = f"${g.cost_usd.sum():.2f}" if g.cost_usd.notna().any() else "n/a"
        cost_mean = f"${g.cost_usd.mean():.2f}" if g.cost_usd.notna().any() else "n/a"
        tokens = f"{g.output_tokens.mean():.0f}" if g.output_tokens.notna().any() else "n/a"
        turns = f"{g.num_turns.mean():.1f}" if g.num_turns.notna().any() else "n/a"
        # meta n/N makes partial coverage visible: sums/means computed over the
        # reps that recorded metadata would otherwise read as full-arm figures.
        lines.append(
            f"- {arm}: runs {len(g)}, meta {int(g.cost_usd.notna().sum())}/{len(g)}, "
            f"total {cost_total}, mean {cost_mean}, "
            f"mean output tokens {tokens}, mean turns {turns}"
        )
    return lines


def scores_frame(rows: list[dict]) -> pd.DataFrame:
    """Rows -> sorted scores frame. An all-failed batch has no rows; keep the
    base columns so downstream verdict code (df.arm etc.) still works."""
    if not rows:
        return pd.DataFrame(columns=BASE_COLUMNS)
    return pd.DataFrame(rows).sort_values("run_id")


def rep_threshold(reps: int) -> int:
    """Design §8: a question passes when >= 2/3 of its reps pass (2 of 3 at the
    default rep count). Scale with the batch's actual rep count."""
    return max(1, math.ceil(reps * 2 / 3))


def _question_passes(df: pd.DataFrame, arm: str, qid: str, threshold: int) -> bool:
    reps = df[(df.arm == arm) & (df.question_id == qid)]
    return int(reps.numbers_pass.sum()) >= threshold


def _pass_count(df: pd.DataFrame, arm: str, qids: list[str], threshold: int) -> int:
    return sum(
        _question_passes(df, arm, q, threshold)
        for q in qids
        if not df[(df.arm == arm) & (df.question_id == q)].empty
    )


def evaluate_verdicts(df: pd.DataFrame, cells: dict, questions: list[Question], threshold: int = 2) -> dict:
    qids = [q.id for q in questions]
    trap_qids = [q.id for q in questions if q.trap != "none"]
    full_pass = _pass_count(df, "full", qids, threshold)
    full_runs = df[df.arm == "full"]
    sev1_count = int(full_runs.severity1_tripped.sum()) + int(
        full_runs.get("judge_severity1_found", pd.Series(dtype=bool)).fillna(False).sum()
    )
    full_cells = [c for (q, a), c in cells.items() if a == "full"]
    # bool(full_cells) keeps rule 1 non-vacuous: a batch where the full arm
    # produced no graded cells must not pass on all([]) == True.
    full_cells_ok = bool(full_cells) and all(c["consistent"] for c in full_cells)
    # ceil(7/8 * N) generalizes the pre-registered ">= 7 of 8" proportionally,
    # so a small denominator (e.g. a 1-question smoke batch) gets no free slack.
    rule1 = bool(qids) and full_pass >= math.ceil(len(qids) * 7 / 8) and sev1_count == 0 and full_cells_ok

    rule2 = None
    # No trap questions in the batch -> rule 2 is inapplicable (None), the same
    # as when the bare arm is absent — not a failed verdict on two zero counts.
    if "bare" in set(df.arm) and trap_qids:
        rule2 = _pass_count(df, "full", trap_qids, threshold) > _pass_count(df, "bare", trap_qids, threshold)

    rule3 = None
    if "knowledge" in set(df.arm):
        k_cells = sum(c["consistent"] for (q, a), c in cells.items() if a == "knowledge")
        f_cells = sum(c["consistent"] for (q, a), c in cells.items() if a == "full")
        rule3 = _pass_count(df, "knowledge", qids, threshold) >= full_pass and k_cells >= f_cells

    return {
        "rule1_trustworthy": rule1,
        "rule1_detail": f"full passes {full_pass}/{len(qids)} questions; severity1={sev1_count}; "
        f"cells_consistent={full_cells_ok}",
        "rule2_earns_overhead": rule2,
        "rule3_bloat_signal": rule3,
    }


def adherence_lines(df: pd.DataFrame) -> list[str]:
    """Per question x arm: how many reps consulted mandatory sources, surfaced
    required assumptions, and resolved forks matching the key. Reported next to
    the verdicts but never gated into them (design §8 rules are
    numbers/severity-1/consistency only): it shows whether a passing number was
    derived independently vs. via the expected sources and resolutions."""
    if df.empty:
        return []
    lines = ["", "## Adherence (sources / assumptions / resolutions; reported, not verdict-gated)", ""]
    for (qid, arm), g in df.groupby(["question_id", "arm"]):
        lines.append(
            f"- {qid} x {arm}: sources {int(g.sources_pass.sum())}/{len(g)}, "
            f"assumptions {int(g.assumptions_pass.sum())}/{len(g)}, "
            f"resolutions {int(g.resolutions_match.sum())}/{len(g)}"
        )
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--judge", action="store_true")
    parser.add_argument(
        "--rejudge", action="store_true", help="re-invoke the judge even when a cached verdict exists"
    )
    parser.add_argument("--model", default="claude-fable-5")
    args = parser.parse_args()

    here = Path(__file__).parent
    batch_dir = here / "results" / args.batch
    state = json.loads((batch_dir / "state.json").read_text())
    questions = load_manifest(here / "questions" / "manifest.yaml")
    qids_in_state = {rid.split("__")[0] for rid in state["runs"]}
    keys = load_keys_for_batch(here / "keys", questions, qids_in_state)

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
            j = judge_cached(batch_dir, run_id, keys[qid], answer, args.model, rejudge=args.rejudge)
            if j:
                judge_ok += 1
                row.update({f"judge_{k}": v for k, v in j["scores"].items()})
                row["judge_severity1_found"] = bool(j["severity1_found"])
            else:
                print(f"{run_id}: judge FAILED")
        rows.append(row)

    df = scores_frame(rows)
    df.to_csv(batch_dir / "scores.csv", index=False)

    # Coverage denominator = every question with ANY recorded run, not only the
    # ok ones. A question whose runs all failed must still count against rule 1,
    # otherwise a silent drop would inflate the pass rate.
    denom_questions = [q for q in questions if q.id in qids_in_state]
    cells = {ck: grading.cell_consistency(bs, keys[ck[0]]) for ck, bs in blocks.items()}
    # Rep count comes from the recorded run ids, so a resumed or non-default
    # --reps batch grades against its own size, not a hardcoded 3.
    reps_recorded = max((int(rid.split("__")[2].lstrip("r")) for rid in state["runs"]), default=0)
    verdicts = evaluate_verdicts(df, cells, denom_questions, threshold=rep_threshold(reps_recorded))

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
    lines += adherence_lines(df)
    lines += cost_lines(df)
    (batch_dir / "report.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
