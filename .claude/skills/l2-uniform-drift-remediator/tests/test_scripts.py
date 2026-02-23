from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT_TOOL = SKILL_ROOT / "scripts" / "l2_uniform_preflight_tool.py"
FAILURE_HANDLER = SKILL_ROOT / "scripts" / "dbt_failure_handler.py"


def sample_preflight_log() -> str:
    lines = [
        "noise line",
        'L2_PREFLIGHT|{"kind":"config","metadata_catalog":"prod_catalog","strict":true,"states_expected":51}',
        'L2_PREFLIGHT|{"kind":"stg_minus_src","state":"ME","columns":["old_col"]}',
        'L2_PREFLIGHT|{"kind":"summary","metadata_catalog":"prod_catalog","strict":true,"status":"fail","states_evaluated":51,"source_relations_found":51,"staging_relations_found":51,"finding_count":1}',
    ]
    return "\n".join(lines) + "\n"


class TestL2UniformPreflightTool(unittest.TestCase):
    """Tests for the preflight log analyzer and planner CLI."""

    def test_analyze_outputs_expected_summary(self) -> None:
        """`analyze` should summarize metadata and impacted states from logs."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(sample_preflight_log(), encoding="utf-8")
            json_out = tmp_path / "analysis.json"

            result = subprocess.run(
                [
                    sys.executable,
                    str(PREFLIGHT_TOOL),
                    "analyze",
                    "--log-file",
                    str(log_file),
                    "--json-out",
                    str(json_out),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(json_out.read_text(encoding="utf-8"))
            self.assertEqual(payload["metadata_catalog"], "prod_catalog")
            self.assertEqual(payload["finding_count"], 1)
            self.assertEqual(payload["impacted_states"], ["ME"])

    def test_plan_includes_staging_fix_and_follow_up(self) -> None:
        """`plan` should include safe staging refresh and follow-up commands."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(sample_preflight_log(), encoding="utf-8")
            json_out = tmp_path / "plan.json"
            shell_out = tmp_path / "plan.sh"

            result = subprocess.run(
                [
                    sys.executable,
                    str(PREFLIGHT_TOOL),
                    "plan",
                    "--log-file",
                    str(log_file),
                    "--json-out",
                    str(json_out),
                    "--shell-out",
                    str(shell_out),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(json_out.read_text(encoding="utf-8"))
            plan = payload["plan"]
            self.assertTrue(
                any(
                    "stg_dbt_source__l2_s3_me_uniform" in command
                    for command in plan["safe_commands"]
                )
            )
            self.assertFalse(
                any(
                    "int__l2_nationwide_uniform int__l2_nationwide_uniform_w_haystaq"
                    in command
                    for command in plan["safe_commands"]
                )
            )
            self.assertTrue(plan["follow_up_commands"])


class TestFailureHandler(unittest.TestCase):
    """Tests for end-to-end failure handler artifact generation."""

    def test_failure_handler_writes_artifacts(self) -> None:
        """Handler should create one run directory with summary artifacts."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(sample_preflight_log(), encoding="utf-8")
            output_dir = tmp_path / "out"

            result = subprocess.run(
                [
                    sys.executable,
                    str(FAILURE_HANDLER),
                    "--log-file",
                    str(log_file),
                    "--output-dir",
                    str(output_dir),
                    "--dbt-project-path",
                    str(tmp_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            run_dirs = [p for p in output_dir.iterdir() if p.is_dir()]
            self.assertEqual(len(run_dirs), 1)
            summary_path = run_dirs[0] / "summary.md"
            self.assertTrue(summary_path.exists())
            summary = summary_path.read_text(encoding="utf-8")
            self.assertIn("Generated Artifacts", summary)

    def test_failure_handler_returns_hint_when_no_preflight_lines(self) -> None:
        """Handler should return a clear hint when logs lack preflight entries."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "build.log"
            log_file.write_text("no preflight lines\n", encoding="utf-8")
            output_dir = tmp_path / "out"

            result = subprocess.run(
                [
                    sys.executable,
                    str(FAILURE_HANDLER),
                    "--log-file",
                    str(log_file),
                    "--output-dir",
                    str(output_dir),
                    "--dbt-project-path",
                    str(tmp_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1)
            self.assertIn("does not contain preflight output", result.stderr)


if __name__ == "__main__":
    unittest.main()
