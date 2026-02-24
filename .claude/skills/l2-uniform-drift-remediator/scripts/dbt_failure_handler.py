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
        description="Generate deterministic L2 uniform remediation instructions from a preflight log."
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

    copied_log = run_dir / "dbt_preflight.log"
    shutil.copy2(args.log_file, copied_log)

    triage_text = run_dir / "triage.txt"
    triage_json = run_dir / "triage.json"
    plan_shell = run_dir / "safe_fix_plan.sh"
    summary_md = run_dir / "summary.md"

    triage_cmd = [
        sys.executable,
        str(tool_path),
        "--log-file",
        str(copied_log),
        "--json-out",
        str(triage_json),
        "--shell-out",
        str(plan_shell),
    ]

    rc, out = _run_cmd(triage_cmd)
    _write_text(triage_text, out)

    if rc == 1:
        print(out, file=sys.stderr)
        print(
            "Hint: this log does not contain complete preflight output.",
            file=sys.stderr,
        )
        print(
            "Run preflight after the failed build, download the full preflight log, then rerun this handler:",
            file=sys.stderr,
        )
        print(
            "dbt run-operation l2_uniform_schema_preflight --args '{\"strict\": true}'",
            file=sys.stderr,
        )
        print(f"Triage step failed. See: {triage_text}", file=sys.stderr)
        return 1

    if not triage_json.exists():
        print("Triage output JSON was not created.", file=sys.stderr)
        print(f"Triage step failed. See: {triage_text}", file=sys.stderr)
        print(f"Command: {_format_command(triage_cmd)}", file=sys.stderr)
        return rc if rc != 0 else 1

    try:
        triage_payload = _load_json(triage_json)
    except json.JSONDecodeError as exc:
        print(f"Triage output JSON is invalid: {exc}", file=sys.stderr)
        print(f"Triage step failed. See: {triage_text}", file=sys.stderr)
        return rc if rc != 0 else 1

    summary = triage_payload.get("summary", {})
    plan = triage_payload.get("plan", {})
    manual_actions = plan.get("manual_actions", [])

    summary_lines = [
        "# L2 Uniform Failure Handler Summary",
        "",
        f"- source_log: `{args.log_file}`",
        f"- copied_log: `{copied_log}`",
        f"- status: `{summary.get('status')}`",
        f"- finding_count: `{summary.get('finding_count')}`",
        f"- impacted_states: `{', '.join(summary.get('impacted_states', [])) or '(none)'}`",
        f"- manual_actions_count: `{len(manual_actions)}`",
        f"- triage_exit_code: `{rc}`",
        "",
        "## Generated Artifacts",
        f"- `{triage_text}`",
        f"- `{triage_json}`",
        f"- `{plan_shell}`",
        f"- `{summary_md}`",
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
        "- This handler does not execute dbt commands. Run safe commands manually in an ad hoc dbt Cloud job (or equivalent controlled run)."
    )
    summary_lines.append(
        "- Keep raw logs under `.claude/skills/l2-uniform-drift-remediator/dbt_logs/` locally and copy sanitized summaries to PR description or tracked runbooks."
    )
    _write_text(summary_md, "\n".join(summary_lines) + "\n")

    print(f"Wrote failure-handler artifacts to: {run_dir}")
    print(f"Summary: {summary_md}")

    if rc == 2:
        print("Manual actions remain. See summary for required steps.", file=sys.stderr)
        return 2

    if rc != 0:
        print(f"Triage step failed. See: {triage_text}", file=sys.stderr)
        print(f"Command: {_format_command(triage_cmd)}", file=sys.stderr)
        return rc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
