#!/usr/bin/env python3
"""Handle failed dbt runs by parsing preflight logs and generating fix artifacts."""

from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parent.parent / "dbt_logs" / "failure_handler"
)


def _run_cmd(cmd: list[str], cwd: Path | None = None) -> tuple[int, str]:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=False,
        capture_output=True,
        text=True,
    )
    output = (result.stdout or "") + (result.stderr or "")
    return result.returncode, output


def _format_command(cmd: list[str]) -> str:
    return shlex.join(cmd)


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and optionally execute L2 uniform drift remediation steps from a dbt log."
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        required=True,
        help="Path to dbt Cloud/log output containing L2_PREFLIGHT JSON lines.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where generated artifacts are written.",
    )
    parser.add_argument(
        "--dbt-project-path",
        type=Path,
        default=Path("dbt/project"),
        help="Path to dbt project root for safe command execution.",
    )
    parser.add_argument(
        "--execute-safe",
        action="store_true",
        help="Execute safe, non-destructive commands after generating the plan.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if not args.log_file.exists():
        print(f"Log file not found: {args.log_file}", file=sys.stderr)
        return 1

    tool_path = Path(__file__).with_name("l2_uniform_preflight_tool.py")
    if not tool_path.exists():
        print(f"Preflight tool not found: {tool_path}", file=sys.stderr)
        return 1

    started_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir = args.output_dir / started_at
    run_dir.mkdir(parents=True, exist_ok=False)

    copied_log = run_dir / "dbt_failure.log"
    shutil.copy2(args.log_file, copied_log)

    analysis_text = run_dir / "analysis.txt"
    analysis_json = run_dir / "analysis.json"
    plan_text = run_dir / "plan.txt"
    plan_json = run_dir / "plan.json"
    plan_shell = run_dir / "safe_fix_plan.sh"
    summary_md = run_dir / "summary.md"

    analyze_cmd = [
        sys.executable,
        str(tool_path),
        "analyze",
        "--log-file",
        str(copied_log),
        "--json-out",
        str(analysis_json),
    ]
    rc, out = _run_cmd(analyze_cmd)
    _write_text(analysis_text, out)
    if rc != 0:
        print(out, file=sys.stderr)
        print(
            "Hint: this log does not contain preflight output.",
            file=sys.stderr,
        )
        print(
            "Run preflight after the failed build, download that preflight log, then rerun this handler:",
            file=sys.stderr,
        )
        print(
            "dbt run-operation l2_uniform_schema_preflight --args '{\"strict\": true}'",
            file=sys.stderr,
        )
        print(f"Analyze step failed. See: {analysis_text}", file=sys.stderr)
        return rc

    plan_cmd = [
        sys.executable,
        str(tool_path),
        "plan",
        "--log-file",
        str(copied_log),
        "--json-out",
        str(plan_json),
        "--shell-out",
        str(plan_shell),
    ]
    if args.execute_safe:
        plan_cmd.extend(
            [
                "--execute-safe",
                "--dbt-project-path",
                str(args.dbt_project_path),
            ]
        )

    rc, out = _run_cmd(plan_cmd)
    _write_text(plan_text, out)

    if not plan_json.exists():
        print("Plan output JSON was not created.", file=sys.stderr)
        print(f"Plan/execute step failed. See: {plan_text}", file=sys.stderr)
        print(f"Command: {_format_command(plan_cmd)}", file=sys.stderr)
        return rc if rc != 0 else 1

    try:
        plan_payload = _load_json(plan_json)
    except json.JSONDecodeError as exc:
        print(f"Plan output JSON is invalid: {exc}", file=sys.stderr)
        print(f"Plan/execute step failed. See: {plan_text}", file=sys.stderr)
        return rc if rc != 0 else 1
    summary = plan_payload.get("summary", {})
    plan = plan_payload.get("plan", {})
    manual_actions = plan.get("manual_actions", [])

    summary_lines = [
        "# L2 Uniform Failure Handler Summary",
        "",
        f"- source_log: `{args.log_file}`",
        f"- copied_log: `{copied_log}`",
        f"- status: `{summary.get('status')}`",
        f"- finding_count: `{summary.get('finding_count')}`",
        f"- impacted_states: `{', '.join(summary.get('impacted_states', [])) or '(none)'}`",
        f"- execute_safe: `{str(args.execute_safe).lower()}`",
        f"- manual_actions_count: `{len(manual_actions)}`",
        "",
        "## Generated Artifacts",
        f"- `{analysis_text}`",
        f"- `{analysis_json}`",
        f"- `{plan_text}`",
        f"- `{plan_json}`",
        f"- `{plan_shell}`",
        "",
        "## Safe Commands",
    ]
    for command in plan.get("safe_commands", []):
        summary_lines.append(f"- `{command}`")

    follow_up_commands = plan.get("follow_up_commands", [])
    if follow_up_commands:
        summary_lines.append("")
        summary_lines.append("## Follow-up Commands")
        for command in follow_up_commands:
            summary_lines.append(f"- {command}")

    if manual_actions:
        summary_lines.append("")
        summary_lines.append("## Manual Actions Required")
        for action in manual_actions:
            summary_lines.append(f"- {action}")

    summary_lines.append("")
    summary_lines.append("## Notes")
    summary_lines.append(
        "- Keep raw logs under `.claude/skills/l2-uniform-drift-remediator/dbt_logs/` locally and copy sanitized summaries to PR description or tracked runbooks."
    )
    _write_text(summary_md, "\n".join(summary_lines) + "\n")

    print(f"Wrote failure-handler artifacts to: {run_dir}")
    print(f"Summary: {summary_md}")

    if rc == 0:
        return 0
    if rc == 2:
        print("Safe commands completed, but manual actions remain.", file=sys.stderr)
        return 2

    print(f"Plan/execute step failed. See: {plan_text}", file=sys.stderr)
    print(f"Command: {_format_command(plan_cmd)}", file=sys.stderr)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
