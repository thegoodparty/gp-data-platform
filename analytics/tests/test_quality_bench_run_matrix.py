import json
import subprocess
from pathlib import Path

from quality_bench import run_matrix
from quality_bench.bank import Question


def q(qid="q01") -> Question:
    return Question(qid, "win", "none", "calibration", f"{qid}.md", f"{qid}_key.yaml")


def test_build_runs_matrix(tmp_path: Path):
    (tmp_path / "q01.md").write_text("What is the monthly funnel?")
    arm_dirs = {"full": tmp_path / "full", "bare": tmp_path / "bare"}
    runs = run_matrix.build_runs([q()], arm_dirs, reps=2, questions_dir=tmp_path)
    assert len(runs) == 4  # 1 question x 2 arms x 2 reps
    assert runs[0].prompt == "What is the monthly funnel?"
    assert runs[0].run_id == "q01__full__r1"


def test_transcript_path_munges_cwd(tmp_path: Path):
    p = run_matrix.transcript_path(Path("/Users/t/.cache/gp_quality_bench/arms/full"), "abc123")
    assert p.name == "abc123.jsonl"
    assert "-Users-t--cache-gp-quality-bench-arms-full" in str(p.parent)


def make_arm_dir(tmp_path: Path) -> Path:
    """A minimal arm dir that launch_run can copytree into runs/<run_id>."""
    arm_dir = tmp_path / "arms" / "bare"
    arm_dir.mkdir(parents=True)
    (arm_dir / "CLAUDE.md").write_text("floor")
    return arm_dir


def test_launch_run_parses_cli_json(tmp_path: Path):
    arm_dir = make_arm_dir(tmp_path)
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", arm_dir)
    # The run happens in a per-run copy at runs/<run_id>; the transcript is keyed
    # off that copy's path, so the fixture must live there.
    run_dir = arm_dir.parent / "runs" / spec.run_id
    transcript_dir = run_matrix.transcript_path(run_dir, "sess1").parent
    transcript_dir.mkdir(parents=True, exist_ok=True)
    (transcript_dir / "sess1.jsonl").write_text(
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "answer"}]}})
    )

    calls: dict = {}

    class FakeCompleted:
        returncode = 0
        stdout = json.dumps({"session_id": "sess1", "result": "answer"})
        stderr = ""

    def fake_runner(*a, **k):
        calls.update(k)
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
    # Ran in the per-run copy, not the shared arm dir.
    assert calls["cwd"] == run_dir
    assert "runs" in run_dir.parts and spec.run_id in run_dir.parts
    # Run dir cleaned up by default; shared arm dir untouched.
    assert not run_dir.exists()
    assert arm_dir.exists()
    assert "run_dir" not in out
    assert (tmp_path / "batch" / "q01__bare__r1.answer.md").read_text() == "answer"
    assert (tmp_path / "batch" / "q01__bare__r1.transcript.jsonl").exists()


def test_launch_run_keep_run_dir(tmp_path: Path):
    arm_dir = make_arm_dir(tmp_path)
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", arm_dir)
    run_dir = arm_dir.parent / "runs" / spec.run_id

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
