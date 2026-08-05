---
name: l2-uniform-drift-remediator
description: Triage and remediate L2 uniform schema drift using `l2_uniform_schema_preflight` output. Use when dbt runs touching `int__l2_nationwide_uniform` or `int__l2_nationwide_uniform_w_haystaq` fail, or when `L2_PREFLIGHT|` lines report drift (`stg_minus_src`, `src_minus_stg`, `target_minus_src`, `relation_missing`). Parse preflight logs, generate deterministic safe commands and manual instructions, and produce operator-ready next steps.
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
3. Run the failure handler:
```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/dbt_failure_handler.py \
  --log-file /path/to/preflight.log \
  --output-dir /path/to/gp-data-platform/.claude/skills/l2-uniform-drift-remediator/dbt_logs/failure_handler
```
4. Review `summary.md`, run safe commands manually in a one-off dbt Cloud job (or equivalent), and complete follow-up steps.
5. If `target_minus_src` or `relation_missing` exists, follow manual remediation steps in the summary.
6. If `target_minus_src` includes `int__l2_nationwide_uniform`, apply approved column deprecations to both:
- `int__l2_nationwide_uniform`
- `int__l2_nationwide_uniform_w_haystaq`
Then rerun strict preflight.

## Guardrails

- Keep `on_schema_change="append_new_columns"` as-is.
- Do not switch to `sync_all_columns`.
- Do not execute destructive DDL automatically.
- This skill generates instructions only; it does not execute dbt commands.

## Inputs and Outputs

Input:
- dbt Cloud CLI log or local dbt log containing `L2_PREFLIGHT|{...}` lines.

Output:
- Deterministic triage summary.
- Command plan for safe fixes.
- Generated shell script containing safe commands to run manually.

## Notes for dbt Cloud

This skill itself does not run inside dbt Cloud unless you call its script from a dbt Cloud job command environment that has Python and this repo checked out.

To surface drift context directly in dbt Cloud logs, rely on the preflight macro logging (`L2_PREFLIGHT|...`).

## Log Capture and PR Guidance

- You can download dbt Cloud run logs and pass them to the failure handler.
- Store raw logs in `.claude/skills/l2-uniform-drift-remediator/dbt_logs/` (local, untracked) or incident tooling.
- For PR context, commit only sanitized summaries/command plans (not raw operational logs with secrets/tokens).

Detailed operational steps are in:
- `references/l2_uniform_schema_drift_runbook.md`
