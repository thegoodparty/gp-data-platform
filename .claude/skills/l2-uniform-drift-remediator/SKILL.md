---
name: l2-uniform-drift-remediator
description: Triage and safely remediate L2 uniform schema drift using `l2_uniform_schema_preflight` output. Use when dbt runs touching `int__l2_nationwide_uniform` or `int__l2_nationwide_uniform_w_haystaq` fail, or when `L2_PREFLIGHT|` lines report drift (`stg_minus_src`, `src_minus_stg`, `target_minus_src`, `relation_missing`). Parse preflight logs, generate deterministic fix plans, optionally execute only non-destructive staging rebuild fixes, and produce operator-ready next steps.
---

# L2 Uniform Drift Remediator

Use this workflow to convert post-failure preflight output into safe actions.

## Workflow

All script paths below are repo-root-relative.

1. If a build fails, run preflight to collect full drift visibility:
```bash
dbt run-operation l2_uniform_schema_preflight --args '{"strict": true}'
```
2. Download the preflight log containing `L2_PREFLIGHT|` JSON lines.
3. Run the failure handler (recommended):
```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/dbt_failure_handler.py \
  --log-file /path/to/preflight.log \
  --output-dir /path/to/gp-data-platform/.claude/skills/l2-uniform-drift-remediator/dbt_logs/failure_handler
```
4. Parse and classify findings (manual mode):
```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/l2_uniform_preflight_tool.py analyze --log-file /path/to/log
```
5. Generate the concrete remediation plan:
```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/l2_uniform_preflight_tool.py plan --log-file /path/to/log
```
6. Execute safe fixes only when findings are limited to state staging drift (`stg_minus_src`, `src_minus_stg`):
```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/l2_uniform_preflight_tool.py plan \
  --log-file /path/to/log \
  --execute-safe \
  --dbt-project-path /path/to/gp-data-platform/dbt/project
```
7. If `target_minus_src` or `relation_missing` exists, stop auto-fix and produce manual remediation steps.
8. If `target_minus_src` includes `int__l2_nationwide_uniform`, apply approved column deprecations to both:
- `int__l2_nationwide_uniform`
- `int__l2_nationwide_uniform_w_haystaq`
Then rerun strict preflight.

## Guardrails

- Keep `on_schema_change="append_new_columns"` as-is.
- Do not switch to `sync_all_columns`.
- Do not execute destructive DDL automatically.
- Auto-execute only:
  - targeted staging rebuilds for impacted states
  - preflight rerun (`strict=true` when no manual actions remain; `strict=false` when manual actions remain so full instructions can still be generated)

## Inputs and Outputs

Input:
- dbt Cloud CLI log or local dbt log containing `L2_PREFLIGHT|{...}` lines.

Output:
- Deterministic triage summary.
- Command plan for safe fixes.
- Optional generated shell script with executable safe fix commands.

## Notes for dbt Cloud

This skill itself does not run inside dbt Cloud unless you call its script from a dbt Cloud job command environment that has Python and this repo checked out.

To surface drift context directly in dbt Cloud logs, rely on the preflight macro logging (`L2_PREFLIGHT|...`).

## Log Capture and PR Guidance

- You can download dbt Cloud run logs and pass them to the failure handler.
- Store raw logs in `.claude/skills/l2-uniform-drift-remediator/dbt_logs/` (local, untracked) or incident tooling.
- For PR context, commit only sanitized summaries/command plans (not raw operational logs with secrets/tokens).

Detailed operational steps are in:
- `references/l2_uniform_schema_drift_runbook.md`
