"""Launch the question x arm x rep matrix as headless `claude -p` runs.

Each run: fresh session, cwd = a per-run copy of the arm dir
(runs/<batch>/<run_id>), an ephemeral HOME with an allowlisted environment,
prompt = the question verbatim. State (results/<batch>/state.json) makes reruns
resume instead of repeat; resume refuses when the batch inputs have changed.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import hashlib
import json
import os
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


def save_state(state_file: Path, state: dict) -> None:
    """Atomic write via temp file + rename: a Ctrl-C mid-write must not corrupt
    state.json, or every later resume dies on JSONDecodeError."""
    tmp = state_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(state_file)


def bench_fingerprint(here: Path, model: str) -> dict:
    """Hash the inputs that shape run answers: question prompts, the floor, and
    the model. Keys are deliberately exempt (grader-side, applied uniformly at
    grade time, so a key edit cannot mix run provenance); binding arm content
    and prep config is the fuller BatchConfig follow-up. The harness git sha is
    recorded for provenance but not gated, so an unrelated commit does not force
    a full re-run."""
    h = hashlib.sha256()
    for p in [*sorted((here / "questions").glob("*")), here / "floor.md"]:
        if p.is_file():
            h.update(p.name.encode())
            h.update(p.read_bytes())
    git = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=here, capture_output=True, text=True, check=False
    )
    return {
        "inputs_sha256": h.hexdigest(),
        "model": model,
        "harness_git_sha": git.stdout.strip() if git.returncode == 0 else "unknown",
    }


def resume_guard(state: dict, fingerprint: dict) -> None:
    """Refuse to resume a batch whose run-shaping inputs changed since it was
    created: mixed-provenance runs would grade as one batch with no record that
    half saw different questions or a different model."""
    old = state.get("fingerprint")
    if old is None:
        state["fingerprint"] = fingerprint
        return
    changed = [k for k in ("inputs_sha256", "model") if old.get(k) != fingerprint[k]]
    if changed:
        raise SystemExit(
            f"refusing to resume: {', '.join(changed)} changed since this batch was created; "
            "use a fresh --batch name"
        )


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


ENV_PASS = ("PATH", "TERM", "LANG", "LC_ALL", "TMPDIR")
ENV_PASS_PREFIXES = ("DATABRICKS_", "ANTHROPIC_", "CLAUDE_")


@functools.cache
def _uv_cache_dir() -> str:
    proc = subprocess.run(["uv", "cache", "dir"], capture_output=True, text=True, check=False)
    return proc.stdout.strip() if proc.returncode == 0 else ""


@functools.cache
def _keychain_credentials() -> str:
    """macOS: the CLI's OAuth token lives in the login keychain, which is found
    via HOME (~/Library/Keychains), so an ephemeral HOME cannot see it. Export
    it once per process and seed it file-based into each run HOME."""
    try:
        proc = subprocess.run(
            ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (subprocess.TimeoutExpired, OSError):
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def run_env(home: Path, source_home: Path | None = None) -> dict[str, str]:
    """Allowlisted environment with an ephemeral HOME. The run cannot see the
    operator's ~/.claude (session history, memory, other projects) or any env
    var outside the allowlist; its transcript lands inside `home`, where
    launch_run collects it. CLI credentials are seeded in when file-based
    (macOS keychain auth works regardless of HOME)."""
    source_home = source_home or Path.home()
    env = {"HOME": str(home)}
    for k, v in os.environ.items():
        if k in ENV_PASS or k.startswith(ENV_PASS_PREFIXES):
            env[k] = v
    # uv keys its cache off HOME by default; point the run at the real cache so
    # it does not re-download every dependency into the ephemeral HOME.
    cache = os.environ.get("UV_CACHE_DIR") or _uv_cache_dir()
    if cache:
        env["UV_CACHE_DIR"] = cache
    (home / ".claude").mkdir(parents=True, exist_ok=True)
    creds = source_home / ".claude" / ".credentials.json"
    if creds.exists():
        shutil.copy(creds, home / ".claude" / ".credentials.json")
    elif blob := _keychain_credentials():
        dest = home / ".claude" / ".credentials.json"
        dest.write_text(blob)
        dest.chmod(0o600)
    # Minimal CLI config: onboarding done, plus the login identity. The CLI
    # reads login STATE from .claude.json (oauthAccount) while the token itself
    # lives in the file-based credentials above or the macOS keychain (which is
    # HOME-independent); without this stanza a fresh HOME is "Not logged in".
    seed: dict = {"hasCompletedOnboarding": True}
    src_cfg = source_home / ".claude.json"
    if src_cfg.exists():
        with contextlib.suppress(OSError, json.JSONDecodeError):
            real = json.loads(src_cfg.read_text())
            seed.update({k: real[k] for k in ("oauthAccount", "userID") if k in real})
    home.mkdir(parents=True, exist_ok=True)
    (home / ".claude.json").write_text(json.dumps(seed))
    return env


def transcript_path(home: Path, cwd: Path, session_id: str) -> Path:
    """Transcript location under the run's ephemeral HOME. The CLI keys project
    dirs by cwd with '/', '_', and '.' munged to '-'."""
    munged = re.sub(r"[/_.]", "-", str(cwd))
    return home / ".claude" / "projects" / munged / f"{session_id}.jsonl"


def launch_run(
    spec: RunSpec,
    model: str,
    timeout_s: int,
    batch_dir: Path,
    keep_run_dir: bool = False,
    runner=subprocess.run,
) -> dict:
    batch_dir.mkdir(parents=True, exist_ok=True)
    # Per-run isolation: copy the arm into runs/<batch>/<run_id> and run there
    # with an ephemeral HOME. A fresh cwd per run means no cross-rep leakage
    # (one rep can't mutate the arm the next rep sees); the batch segment keeps
    # concurrent/sequential batches off each other's paths; the ephemeral HOME
    # means no shared Claude project state or auto-memory across runs. uv
    # re-creates the venv from cache on the first `uv run` in the copy.
    run_dir = spec.arm_dir.parent / "runs" / batch_dir.name / spec.run_id
    home = run_dir.parent / f"{spec.run_id}.home"
    for d in (run_dir, home):
        if d.exists():
            shutil.rmtree(d)
    shutil.copytree(spec.arm_dir, run_dir, ignore=shutil.ignore_patterns(".venv", "__pycache__"))
    env = run_env(home)
    out: dict = {
        "run_id": spec.run_id,
        "ok": False,
        "session_id": None,
        "answer_file": None,
        "transcript_file": None,
    }
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
    try:
        try:
            proc = runner(cmd, cwd=run_dir, env=env, capture_output=True, text=True, timeout=timeout_s)
        except (subprocess.TimeoutExpired, OSError) as e:
            out["error"] = f"{type(e).__name__}: {e}"
            return out
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
        src = transcript_path(home, run_dir, session_id)
        if src.exists():
            dst = batch_dir / f"{spec.run_id}.transcript.jsonl"
            shutil.copy(src, dst)
            out["transcript_file"] = str(dst)
        return out
    finally:
        if keep_run_dir:
            out["run_dir"] = str(run_dir)
            out["home_dir"] = str(home)
        else:
            shutil.rmtree(run_dir, ignore_errors=True)
            shutil.rmtree(home, ignore_errors=True)


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
    parser.add_argument(
        "--keep-run-dirs",
        action="store_true",
        help="keep the per-run copy of each arm (runs/<run_id>) instead of deleting it after the run",
    )
    args = parser.parse_args()

    here = Path(__file__).parent
    batch_dir = here / "results" / args.batch
    batch_dir.mkdir(parents=True, exist_ok=True)
    state_file = batch_dir / "state.json"
    state = json.loads(state_file.read_text()) if state_file.exists() else {"runs": {}}
    resume_guard(state, bench_fingerprint(here, args.model))
    save_state(state_file, state)

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
        futures = {
            pool.submit(launch_run, r, args.model, args.timeout, batch_dir, args.keep_run_dirs): r
            for r in todo
        }
        for fut in as_completed(futures):
            try:
                result = fut.result()
            except Exception as e:
                run_id = futures[fut].run_id
                print(f"{run_id}: CRASHED {e}")
                result = {"run_id": run_id, "ok": False, "error": str(e)}
            state["runs"][result["run_id"]] = result
            save_state(state_file, state)
            print(f"{result['run_id']}: {'ok' if result['ok'] else 'FAILED'}")


if __name__ == "__main__":
    main()
