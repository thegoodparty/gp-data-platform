#!/usr/bin/env python3
"""Parse L2 preflight JSON lines and produce safe remediation plans."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

PREFIX = "L2_PREFLIGHT|"


def _load_records(log_file: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw_line in log_file.read_text(encoding="utf-8", errors="replace").splitlines():
        idx = raw_line.find(PREFIX)
        if idx == -1:
            continue
        payload = raw_line[idx + len(PREFIX) :].strip()
        if not payload:
            continue
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            records.append(obj)
    return records


def _derive_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    config = next((r for r in records if r.get("kind") == "config"), {})
    summary = next((r for r in reversed(records) if r.get("kind") == "summary"), {})
    findings = [r for r in records if r.get("kind") not in {"config", "summary"}]

    counts = Counter(str(f.get("kind", "unknown")) for f in findings)
    impacted_states = sorted(
        {
            str(f.get("state")).upper()
            for f in findings
            if f.get("kind") in {"stg_minus_src", "src_minus_stg"} and f.get("state")
        }
    )
    target_minus_src = [f for f in findings if f.get("kind") == "target_minus_src"]
    relation_missing = [f for f in findings if f.get("kind") == "relation_missing"]

    metadata_catalog = summary.get("metadata_catalog") or config.get("metadata_catalog")
    status = summary.get("status")
    if not status:
        status = "clean" if not findings else "unknown"

    return {
        "metadata_catalog": str(metadata_catalog) if metadata_catalog else None,
        "strict": bool(summary.get("strict", config.get("strict", True))),
        "status": str(status),
        "states_evaluated": summary.get("states_evaluated"),
        "source_relations_found": summary.get("source_relations_found"),
        "staging_relations_found": summary.get("staging_relations_found"),
        "finding_count": len(findings),
        "findings_by_kind": dict(sorted(counts.items())),
        "impacted_states": impacted_states,
        "target_minus_src": target_minus_src,
        "relation_missing": relation_missing,
        "findings": findings,
    }


def _command_to_string(command_argv: list[str]) -> str:
    return shlex.join(command_argv)


def _preflight_command_argv(
    metadata_catalog: str | None, strict: bool = True
) -> list[str]:
    command_argv = [
        "dbt",
        "run-operation",
        "l2_uniform_schema_preflight",
        "--args",
        json.dumps({"strict": strict}, separators=(",", ":")),
    ]
    if metadata_catalog:
        vars_json = json.dumps(
            {"preflight_metadata_catalog": metadata_catalog},
            separators=(",", ":"),
        )
        command_argv.extend(["--vars", vars_json])
    return command_argv


def _build_plan(summary: dict[str, Any]) -> dict[str, Any]:
    impacted_states: list[str] = summary["impacted_states"]
    staging_selectors = [
        f"stg_dbt_source__l2_s3_{state.lower()}_uniform" for state in impacted_states
    ]

    safe_command_argv: list[list[str]] = []
    if staging_selectors:
        safe_command_argv.append(
            [
                "dbt",
                "run",
                "--select",
                *staging_selectors,
            ]
        )

    manual_actions: list[str] = []
    if summary["target_minus_src"]:
        for item in summary["target_minus_src"]:
            target_model = item.get("target_model", "unknown_target")
            cols = item.get("columns", [])
            manual_actions.append(
                f"Manual deprecation required for {target_model}: target_minus_src columns={cols}"
            )
            if target_model == "int__l2_nationwide_uniform":
                manual_actions.append(
                    "When deprecating target-only columns on int__l2_nationwide_uniform, "
                    "apply the same removals to int__l2_nationwide_uniform_w_haystaq and rerun preflight."
                )

    if summary["relation_missing"]:
        for item in summary["relation_missing"]:
            relation = item.get("relation", "unknown_relation")
            scope = item.get("scope", "unknown_scope")
            manual_actions.append(
                f"Relation missing ({scope}): {relation}. Verify permissions/catalog/schema."
            )

    can_auto_complete = len(manual_actions) == 0
    # When manual steps remain, run preflight in warn-only mode so the safe phase
    # can complete and callers can receive an explicit "manual actions remain" signal.
    safe_command_argv.append(
        _preflight_command_argv(
            summary["metadata_catalog"],
            strict=can_auto_complete,
        )
    )
    follow_up_commands = [
        "Rerun the originally failed dbt command/job after safe fixes succeed."
    ]

    safe_commands = [_command_to_string(command) for command in safe_command_argv]

    return {
        "safe_commands": safe_commands,
        "safe_command_argv": safe_command_argv,
        "follow_up_commands": follow_up_commands,
        "manual_actions": manual_actions,
        "can_auto_complete": can_auto_complete,
        "staging_selectors": staging_selectors,
    }


def _print_analysis(summary: dict[str, Any]) -> None:
    print("# L2 Uniform Preflight Analysis")
    print(f"- status: {summary['status']}")
    print(f"- strict: {summary['strict']}")
    print(
        f"- metadata_catalog: {summary['metadata_catalog'] or '(not provided in log)'}"
    )
    print(f"- finding_count: {summary['finding_count']}")
    print(f"- impacted_states: {', '.join(summary['impacted_states']) or '(none)'}")
    print("")
    print("## Findings by Kind")
    if not summary["findings_by_kind"]:
        print("- none")
    else:
        for kind, count in summary["findings_by_kind"].items():
            print(f"- {kind}: {count}")


def _print_plan(summary: dict[str, Any], plan: dict[str, Any]) -> None:
    print("# L2 Uniform Safe Remediation Plan")
    print(f"- status: {summary['status']}")
    print(f"- impacted_states: {', '.join(summary['impacted_states']) or '(none)'}")
    print("")
    print("## Safe Commands")
    for idx, command in enumerate(plan["safe_commands"], start=1):
        print(f"{idx}. {command}")

    if plan["manual_actions"]:
        print("")
        print("## Manual Actions Required")
        for idx, action in enumerate(plan["manual_actions"], start=1):
            print(f"{idx}. {action}")

    if plan["follow_up_commands"]:
        print("")
        print("## Follow-up Commands")
        for idx, command in enumerate(plan["follow_up_commands"], start=1):
            print(f"{idx}. {command}")

    print("")
    print(f"- can_auto_complete: {str(plan['can_auto_complete']).lower()}")


def _write_shell_script(shell_out: Path, plan: dict[str, Any]) -> None:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "# Generated by l2_uniform_preflight_tool.py",
    ]
    for command_argv in plan["safe_command_argv"]:
        lines.append(_command_to_string(command_argv))
    shell_out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    shell_out.chmod(0o755)


def _execute_safe_commands(plan: dict[str, Any], dbt_project_path: Path) -> int:
    for command_argv in plan["safe_command_argv"]:
        print(f"$ {_command_to_string(command_argv)}", flush=True)
        result = subprocess.run(
            command_argv,
            cwd=dbt_project_path,
            shell=False,
            check=False,
        )
        if result.returncode != 0:
            return result.returncode
    return 0


def _analyze(args: argparse.Namespace) -> int:
    records = _load_records(args.log_file)
    if not records:
        print(
            f"No {PREFIX} JSON lines found in {args.log_file}",
            file=sys.stderr,
        )
        return 1

    summary = _derive_summary(records)
    _print_analysis(summary)

    if args.json_out:
        args.json_out.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    return 0


def _plan(args: argparse.Namespace) -> int:
    records = _load_records(args.log_file)
    if not records:
        print(
            f"No {PREFIX} JSON lines found in {args.log_file}",
            file=sys.stderr,
        )
        return 1

    summary = _derive_summary(records)
    plan = _build_plan(summary)
    _print_plan(summary, plan)

    if args.json_out:
        args.json_out.write_text(
            json.dumps({"summary": summary, "plan": plan}, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )

    if args.shell_out:
        _write_shell_script(args.shell_out, plan)
        print(f"\nWrote shell plan: {args.shell_out}")

    if args.execute_safe:
        exit_code = _execute_safe_commands(plan, args.dbt_project_path)
        if exit_code != 0:
            return exit_code
        if plan["manual_actions"]:
            return 2

    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse L2 preflight output and generate safe remediation plans."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze_parser = subparsers.add_parser("analyze", help="Analyze preflight output.")
    analyze_parser.add_argument("--log-file", type=Path, required=True)
    analyze_parser.add_argument("--json-out", type=Path)
    analyze_parser.set_defaults(func=_analyze)

    plan_parser = subparsers.add_parser("plan", help="Generate remediation plan.")
    plan_parser.add_argument("--log-file", type=Path, required=True)
    plan_parser.add_argument("--json-out", type=Path)
    plan_parser.add_argument("--shell-out", type=Path)
    plan_parser.add_argument("--execute-safe", action="store_true")
    plan_parser.add_argument(
        "--dbt-project-path",
        type=Path,
        default=Path.cwd(),
        help="Path where dbt commands should run when --execute-safe is used.",
    )
    plan_parser.set_defaults(func=_plan)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
