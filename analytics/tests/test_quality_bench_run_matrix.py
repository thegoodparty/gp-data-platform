import json
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


def test_launch_run_parses_cli_json(tmp_path: Path):
    spec = run_matrix.RunSpec("q01", "bare", 1, "prompt", tmp_path)
    transcript_dir = run_matrix.transcript_path(tmp_path, "sess1").parent
    transcript_dir.mkdir(parents=True, exist_ok=True)
    (transcript_dir / "sess1.jsonl").write_text(
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "answer"}]}})
    )

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
    assert (tmp_path / "batch" / "q01__bare__r1.answer.md").read_text() == "answer"
    assert (tmp_path / "batch" / "q01__bare__r1.transcript.jsonl").exists()
