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


def sample_preflight_log_with_manual_actions() -> str:
    lines = [
        'L2_PREFLIGHT|{"kind":"config","metadata_catalog":"prod_catalog","strict":true,"states_expected":51}',
        'L2_PREFLIGHT|{"kind":"stg_minus_src","state":"ME","columns":["old_col"]}',
        'L2_PREFLIGHT|{"kind":"target_minus_src","target_model":"int__l2_nationwide_uniform","columns":["legacy_col"]}',
        'L2_PREFLIGHT|{"kind":"summary","metadata_catalog":"prod_catalog","strict":true,"status":"fail","states_evaluated":51,"source_relations_found":51,"staging_relations_found":51,"finding_count":2}',
    ]
    return "\n".join(lines) + "\n"


def sample_truncated_preflight_log() -> str:
    lines = [
        'L2_PREFLIGHT|{"kind":"config","metadata_catalog":"prod_catalog","strict":true,"states_expected":51}',
        'L2_PREFLIGHT|{"kind":"summary","metadata_catalog":"prod_catalog","strict":true,"status":"fail","states_evaluated":51,"source_relations_found":51,"staging_relations_found":51,"finding_count":2}',
    ]
    return "\n".join(lines) + "\n"


def sample_preflight_log_with_string_strict_false() -> str:
    lines = [
        'L2_PREFLIGHT|{"kind":"config","metadata_catalog":"prod_catalog","strict":"false","states_expected":51}',
        'L2_PREFLIGHT|{"kind":"summary","metadata_catalog":"prod_catalog","strict":"false","status":"warn","states_evaluated":51,"source_relations_found":51,"staging_relations_found":51,"finding_count":0}',
    ]
    return "\n".join(lines) + "\n"


class TestL2UniformPreflightTool(unittest.TestCase):
    """Tests for the preflight triage CLI."""

    def test_tool_outputs_expected_summary_and_plan(self) -> None:
        """Tool should output summary and safe plan for staging drift."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(sample_preflight_log(), encoding="utf-8")
            json_out = tmp_path / "triage.json"
            shell_out = tmp_path / "plan.sh"

            result = subprocess.run(
                [
                    sys.executable,
                    str(PREFLIGHT_TOOL),
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
            summary = payload["summary"]
            plan = payload["plan"]
            self.assertEqual(summary["metadata_catalog"], "prod_catalog")
            self.assertEqual(summary["finding_count"], 1)
            self.assertEqual(summary["impacted_states"], ["ME"])
            self.assertTrue(
                any(
                    "stg_dbt_source__l2_s3_me_uniform" in command
                    for command in plan["safe_commands"]
                )
            )
            self.assertTrue(
                any(
                    "l2_uniform_schema_preflight" in command
                    for command in plan["safe_commands"]
                )
            )

    def test_tool_returns_2_when_manual_actions_exist(self) -> None:
        """Tool should return 2 when manual actions remain."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(
                sample_preflight_log_with_manual_actions(), encoding="utf-8"
            )
            json_out = tmp_path / "triage.json"

            result = subprocess.run(
                [
                    sys.executable,
                    str(PREFLIGHT_TOOL),
                    "--log-file",
                    str(log_file),
                    "--json-out",
                    str(json_out),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 2, result.stderr)
            payload = json.loads(json_out.read_text(encoding="utf-8"))
            self.assertTrue(payload["plan"]["manual_actions"])

    def test_tool_fails_when_log_is_incomplete(self) -> None:
        """Tool should fail when summary count exceeds parsed findings."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(sample_truncated_preflight_log(), encoding="utf-8")

            result = subprocess.run(
                [
                    sys.executable,
                    str(PREFLIGHT_TOOL),
                    "--log-file",
                    str(log_file),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1)
            self.assertIn("appears incomplete", result.stderr)

    def test_tool_parses_string_false_strict_flag(self) -> None:
        """Tool should parse string 'false' strict values as False."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(
                sample_preflight_log_with_string_strict_false(), encoding="utf-8"
            )
            json_out = tmp_path / "triage.json"

            result = subprocess.run(
                [
                    sys.executable,
                    str(PREFLIGHT_TOOL),
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
            self.assertIs(payload["summary"]["strict"], False)


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

    def test_failure_handler_returns_2_when_manual_actions_exist(self) -> None:
        """Handler should return 2 when triage finds manual actions."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_file = tmp_path / "preflight.log"
            log_file.write_text(
                sample_preflight_log_with_manual_actions(), encoding="utf-8"
            )
            output_dir = tmp_path / "out"

            result = subprocess.run(
                [
                    sys.executable,
                    str(FAILURE_HANDLER),
                    "--log-file",
                    str(log_file),
                    "--output-dir",
                    str(output_dir),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 2)
            run_dirs = [p for p in output_dir.iterdir() if p.is_dir()]
            self.assertEqual(len(run_dirs), 1)
            summary_path = run_dirs[0] / "summary.md"
            summary = summary_path.read_text(encoding="utf-8")
            self.assertIn("Manual Actions Required", summary)

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
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1)
            self.assertIn("does not contain complete preflight output", result.stderr)


if __name__ == "__main__":
    unittest.main()
