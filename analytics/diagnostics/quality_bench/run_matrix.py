"""Launch the question x arm x rep matrix as headless `claude -p` runs.

Each run: fresh session, cwd = the arm dir, prompt = the question verbatim.
State (results/<batch>/state.json) makes reruns resume instead of repeat.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

try:
    from quality_bench.bank import ARMS, Question, load_manifest
except ImportError:  # bare `python run_matrix.py`: diagnostics/ isn't on sys.path yet
    import sys

    sys.path.insert(0, str(Path(__file__).parents[1]))
    from quality_bench.bank import ARMS, Question, load_manifest


@dataclass(frozen=True)
class RunSpec:
    question_id: str
    arm: str
    rep: int
    prompt: str
    arm_dir: Path

    @property
    def run_id(self) -> str:
        return f"{self.question_id}__{self.arm}__r{self.rep}"


def build_runs(
    questions: list[Question], arm_dirs: dict[str, Path], reps: int, questions_dir: Path
) -> list[RunSpec]:
    runs = []
    for q in questions:
        prompt = (questions_dir / q.prompt_file).read_text()
        for arm, arm_dir in arm_dirs.items():
            for rep in range(1, reps + 1):
                runs.append(RunSpec(q.id, arm, rep, prompt, arm_dir))
    return runs


def transcript_path(arm_dir: Path, session_id: str) -> Path:
    munged = re.sub(r"[/_.]", "-", str(arm_dir))
    return Path.home() / ".claude" / "projects" / munged / f"{session_id}.jsonl"


def launch_run(spec: RunSpec, model: str, timeout_s: int, batch_dir: Path, runner=subprocess.run) -> dict:
    batch_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "claude",
        "-p",
        spec.prompt,
        "--output-format",
        "json",
        "--model",
        model,
        "--permission-mode",
        "acceptEdits",
    ]
    proc = runner(cmd, cwd=spec.arm_dir, capture_output=True, text=True, timeout=timeout_s)
    out: dict = {
        "run_id": spec.run_id,
        "ok": False,
        "session_id": None,
        "answer_file": None,
        "transcript_file": None,
    }
    if proc.returncode != 0:
        out["error"] = (proc.stderr or proc.stdout or "")[-2000:]
        return out
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        out["error"] = f"unparseable CLI output: {proc.stdout[-500:]}"
        return out
    session_id = payload.get("session_id", "")
    answer = payload.get("result", "")
    answer_file = batch_dir / f"{spec.run_id}.answer.md"
    answer_file.write_text(answer)
    out.update(ok=True, session_id=session_id, answer_file=str(answer_file))
    src = transcript_path(spec.arm_dir, session_id)
    if src.exists():
        dst = batch_dir / f"{spec.run_id}.transcript.jsonl"
        shutil.copy(src, dst)
        out["transcript_file"] = str(dst)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--model", default="claude-fable-5")
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--arms", nargs="*", default=ARMS)
    parser.add_argument("--questions", nargs="*", default=None)
    parser.add_argument(
        "--arms-root", type=Path, default=Path.home() / ".cache" / "gp_quality_bench" / "arms"
    )
    args = parser.parse_args()

    here = Path(__file__).parent
    batch_dir = here / "results" / args.batch
    batch_dir.mkdir(parents=True, exist_ok=True)
    state_file = batch_dir / "state.json"
    state = json.loads(state_file.read_text()) if state_file.exists() else {"runs": {}}

    questions = load_manifest(here / "questions" / "manifest.yaml")
    if args.questions:
        questions = [q for q in questions if q.id in set(args.questions)]
    arm_dirs = {arm: args.arms_root / arm for arm in args.arms}
    for d in arm_dirs.values():
        if not d.exists():
            raise SystemExit(f"arm dir missing: {d} — run prep_arms.py first")

    runs = build_runs(questions, arm_dirs, args.reps, here / "questions")
    todo = [r for r in runs if not state["runs"].get(r.run_id, {}).get("ok")]
    print(f"{len(runs)} total runs, {len(todo)} to do (resume skips {len(runs) - len(todo)})")

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {pool.submit(launch_run, r, args.model, args.timeout, batch_dir): r for r in todo}
        for fut in as_completed(futures):
            result = fut.result()
            state["runs"][result["run_id"]] = result
            state_file.write_text(json.dumps(state, indent=2))
            print(f"{result['run_id']}: {'ok' if result['ok'] else 'FAILED'}")


if __name__ == "__main__":
    main()
