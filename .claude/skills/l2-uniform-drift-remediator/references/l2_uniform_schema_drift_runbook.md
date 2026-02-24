# L2 Uniform Schema Drift Runbook

## Post-Failure Workflow

When a dbt build fails, run preflight to gather complete schema drift visibility:

1. `dbt run-operation l2_uniform_schema_preflight --args '{"strict": true}'`

## Failure Triage Workflow

1. Download the dbt Cloud log for the preflight run.
2. Run the failure handler in this repo:

```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/dbt_failure_handler.py \
  --log-file /absolute/path/to/downloaded_preflight_log.txt \
  --output-dir /path/to/gp-data-platform/.claude/skills/l2-uniform-drift-remediator/dbt_logs/failure_handler
```

3. Review generated artifacts in the timestamped output directory:
- `triage.txt`
- `triage.json`
- `safe_fix_plan.sh`
- `summary.md`

4. Kick off a one-off dbt Cloud job/command to apply the safe commands from `summary.md` or `safe_fix_plan.sh`.
5. If `target_minus_src` appears, do not run destructive DDL automatically; follow manual deprecation workflow.
6. If deprecating target-only columns from `int__l2_nationwide_uniform`, apply equivalent removals to `int__l2_nationwide_uniform_w_haystaq`, then rerun strict preflight.

## PR Hygiene

- Keep raw logs/artifacts in `.claude/skills/l2-uniform-drift-remediator/dbt_logs/` (local, untracked by default).
- Put concise remediation summaries and command outputs in PR description or a tracked runbook update.
- Do not commit raw logs containing sensitive metadata or tokens.

## Troubleshooting

- If dbt Cloud CLI reports `Session occupied. Please wait until your invocation has been completed.` and Databricks run has already finished, cancel the stale invocation and retry:
  - `dbt cancel --id <invocation_id>`
  - rerun preflight or the intended dbt command.
