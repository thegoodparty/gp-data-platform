import json
import subprocess
from pathlib import Path

import pytest
from quality_bench import run_matrix
from quality_bench.bank import Question


def q(qid="q01") -> Question:
    return Question(qid, "win", "none", "calibration", f"{qid}.md", f"{qid}_key.yaml")


def test_save_state_atomic(tmp_path: Path):
    state_file = tmp_path / "state.json"
    state = {"runs": {"q01__full__r1": {"ok": True}}}
    run_matrix.save_state(state_file, state)
    assert json.loads(state_file.read_text()) == state
    assert not (tmp_path / "state.tmp").exists()  # temp file renamed, not left behind


def test_build_runs_matrix(tmp_path: Path):
    (tmp_path / "q01.md").write_text("What is the monthly funnel?")
    arm_dirs = {"full": tmp_path / "full", "bare": tmp_path / "bare"}
    runs = run_matrix.build_runs([q()], arm_dirs, reps=2, questions_dir=tmp_path)
    assert len(runs) == 4  # 1 question x 2 arms x 2 reps
    assert runs[0].prompt == "What is the monthly funnel?"
    assert runs[0].run_id == "q01__full__r1"


def test_transcript_path_munges_cwd(tmp_path: Path):
    p = run_matrix.transcript_path(tmp_path, Path("/Users/t/.cache/gp_quality_bench/arms/full"), "abc123")
    assert p.name == "abc123.jsonl"
    assert "-Users-t--cache-gp-quality-bench-arms-full" in str(p.parent)
    assert str(p).startswith(str(tmp_path))  # rooted in the ephemeral HOME, not ~


def test_run_env_allowlists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABRICKS_API_KEY", "db-key")
    monkeypatch.setenv("SOME_UNRELATED_TOKEN", "secret")
    src = tmp_path / "srchome"
    (src / ".claude").mkdir(parents=True)
    (src / ".claude" / ".credentials.json").write_text("{}")
    (src / ".claude.json").write_text(
        json.dumps({"oauthAccount": {"emailAddress": "t@x"}, "numStartups": 99, "machineID": "m"})
    )
    home = tmp_path / "home"
    env = run_matrix.run_env(home, source_home=src)
    assert env["HOME"] == str(home)
    assert env["DATABRICKS_API_KEY"] == "db-key"
    assert "SOME_UNRELATED_TOKEN" not in env
    assert (home / ".claude" / ".credentials.json").exists()
    seeded = json.loads((home / ".claude.json").read_text())
    assert seeded["hasCompletedOnboarding"] is True
    # Login identity carried over; everything else from the real config is not.
    assert seeded["oauthAccount"] == {"emailAddress": "t@x"}
    assert "numStartups" not in seeded and "machineID" not in seeded


def test_run_env_keychain_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """No file-based credentials (macOS keychain auth): the exported blob is
    seeded into the ephemeral HOME with owner-only permissions."""
    monkeypatch.setattr(run_matrix, "_keychain_credentials", lambda: '{"tok":1}')
    src = tmp_path / "srchome"
    src.mkdir()
    home = tmp_path / "home"
    run_matrix.run_env(home, source_home=src)
    dest = home / ".claude" / ".credentials.json"
    assert dest.read_text() == '{"tok":1}'
    assert oct(dest.stat().st_mode & 0o777) == "0o600"


def test_resume_guard_blocks_changed_inputs():
    fp = {"inputs_sha256": "aaa", "model": "m", "harness_git_sha": "sha1"}
    state: dict = {"runs": {}}
    run_matrix.resume_guard(state, fp)
    assert state["fingerprint"] == fp
    # Unrelated commit (sha changes, inputs same): resumes, keeps original provenance.
    run_matrix.resume_guard(state, {**fp, "harness_git_sha": "sha2"})
    assert state["fingerprint"]["harness_git_sha"] == "sha1"
    with pytest.raises(SystemExit, match="model"):
        run_matrix.resume_guard(state, {**fp, "model": "other-model"})
    with pytest.raises(SystemExit, match="inputs_sha256"):
        run_matrix.resume_guard(state, {**fp, "inputs_sha256": "bbb"})


def test_bench_fingerprint_tracks_question_edits(tmp_path: Path):
    (tmp_path / "questions").mkdir()
    (tmp_path / "questions" / "q01.md").write_text("v1")
    (tmp_path / "floor.md").write_text("floor")
    a = run_matrix.bench_fingerprint(tmp_path, "m")
    (tmp_path / "questions" / "q01.md").write_text("v2")
    b = run_matrix.bench_fingerprint(tmp_path, "m")
    assert a["inputs_sha256"] != b["inputs_sha256"]
    assert run_matrix.bench_fingerprint(tmp_path, "other")["model"] == "other"


def make_arm_dir(tmp_path: Path) -> Path:
    """A minimal arm dir that launch_run can copytree into runs/<run_id>."""
    arm_dir = tmp_path / "arms" / "bare"
    arm_dir.mkdir(parents=True)
    (arm_dir / "CLAUDE.md").write_text("floor")
    return arm_dir


def test_launch_run_parses_cli_json(tmp_path: Path):
    arm_dir = make_arm_dir(tmp_path)
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", arm_dir)
    # Per-run copy lives at runs/<batch>/<run_id>, ephemeral HOME beside it.
    run_dir = arm_dir.parent / "runs" / "batch" / spec.run_id
    calls: dict = {}

    class FakeCompleted:
        returncode = 0
        stdout = json.dumps({"session_id": "sess1", "result": "answer"})
        stderr = ""

    def fake_runner(*a, **k):
        calls.update(k)
        # The real CLI writes the transcript under $HOME during the run.
        home = Path(k["env"]["HOME"])
        t = run_matrix.transcript_path(home, k["cwd"], "sess1")
        t.parent.mkdir(parents=True, exist_ok=True)
        t.write_text(
            json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "answer"}]}})
        )
        return FakeCompleted()

    out = run_matrix.launch_run(
        spec,
        "claude-fable-5",
        60,
        batch_dir=tmp_path / "batch",
        runner=fake_runner,
    )
    assert out["ok"] is True
    assert out["session_id"] == "sess1"
    # Ran in the per-run copy, not the shared arm dir, with an ephemeral HOME.
    assert calls["cwd"] == run_dir
    assert calls["env"]["HOME"] == str(run_dir.parent / f"{spec.run_id}.home")
    assert calls["env"]["HOME"] != str(Path.home())
    # Run dir and ephemeral HOME cleaned up by default; shared arm dir untouched.
    assert not run_dir.exists()
    assert not (run_dir.parent / f"{spec.run_id}.home").exists()
    assert arm_dir.exists()
    assert "run_dir" not in out
    assert (tmp_path / "batch" / "q01__bare__r1.answer.md").read_text() == "answer"
    assert (tmp_path / "batch" / "q01__bare__r1.transcript.jsonl").exists()


def test_launch_run_keep_run_dir(tmp_path: Path):
    arm_dir = make_arm_dir(tmp_path)
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", arm_dir)
    run_dir = arm_dir.parent / "runs" / "batch" / spec.run_id

    class FakeCompleted:
        returncode = 0
        stdout = json.dumps({"session_id": "sess1", "result": "answer"})
        stderr = ""

    out = run_matrix.launch_run(
        spec,
        "claude-fable-5",
        60,
        batch_dir=tmp_path / "batch",
        keep_run_dir=True,
        runner=lambda *a, **k: FakeCompleted(),
    )
    assert out["ok"] is True
    assert run_dir.exists()
    assert out["run_dir"] == str(run_dir)


def test_launch_run_timeout_expired(tmp_path: Path):
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", make_arm_dir(tmp_path))

    def fake_runner_timeout(*a, **k):
        raise subprocess.TimeoutExpired(cmd="claude", timeout=1)

    out = run_matrix.launch_run(
        spec,
        "claude-fable-5",
        1,
        batch_dir=tmp_path / "batch",
        runner=fake_runner_timeout,
    )
    assert out["ok"] is False
    assert "TimeoutExpired" in out["error"]
    assert out["run_id"] == "q01__bare__r1"


def test_launch_run_nonzero_returncode_with_stderr(tmp_path: Path):
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", make_arm_dir(tmp_path))

    class FakeFailed:
        returncode = 1
        stdout = ""
        stderr = "Command failed: invalid syntax"

    out = run_matrix.launch_run(
        spec,
        "claude-fable-5",
        60,
        batch_dir=tmp_path / "batch",
        runner=lambda *a, **k: FakeFailed(),
    )
    assert out["ok"] is False
    assert "invalid syntax" in out["error"]


def test_launch_run_unparseable_json(tmp_path: Path):
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", make_arm_dir(tmp_path))

    class FakeBadJson:
        returncode = 0
        stdout = "not valid json at all"
        stderr = ""

    out = run_matrix.launch_run(
        spec,
        "claude-fable-5",
        60,
        batch_dir=tmp_path / "batch",
        runner=lambda *a, **k: FakeBadJson(),
    )
    assert out["ok"] is False
    assert "unparseable" in out["error"]


def test_launch_run_success_no_transcript(tmp_path: Path):
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", make_arm_dir(tmp_path))

    class FakeCompleted:
        returncode = 0
        stdout = json.dumps({"session_id": "sess1", "result": "answer"})
        stderr = ""

    out = run_matrix.launch_run(
        spec,
        "claude-fable-5",
        60,
        batch_dir=tmp_path / "batch",
        runner=lambda *a, **k: FakeCompleted(),
    )
    assert out["ok"] is True
    assert out["session_id"] == "sess1"
    assert (tmp_path / "batch" / "q01__bare__r1.answer.md").exists()
    assert out["transcript_file"] is None
