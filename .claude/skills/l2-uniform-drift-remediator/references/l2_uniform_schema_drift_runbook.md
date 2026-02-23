# L2 Uniform Schema Drift Runbook

## Daily dbt Cloud Job Commands

Run preflight before the existing daily build command:

1. `dbt run-operation l2_uniform_schema_preflight --args '{"strict": true}'`
2. `dbt build --exclude="tag:dbt_source tag:l2_s3 tag:weekly write__l2_databricks_to_gp_api"`

## Failure Triage Workflow

1. Download the dbt Cloud log for the failed run.
2. Run the failure handler in this repo:

```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/dbt_failure_handler.py \
  --log-file /absolute/path/to/downloaded_dbt_log.txt \
  --output-dir /path/to/gp-data-platform/.claude/skills/l2-uniform-drift-remediator/dbt_logs/failure_handler \
  --dbt-project-path /path/to/gp-data-platform/dbt/project
```

3. Review generated artifacts in the timestamped output directory:
- `analysis.txt`
- `analysis.json`
- `plan.txt`
- `plan.json`
- `safe_fix_plan.sh`
- `summary.md`

4. If only staging drift is present (`stg_minus_src`, `src_minus_stg`), optionally rerun with safe execution:

```bash
python .claude/skills/l2-uniform-drift-remediator/scripts/dbt_failure_handler.py \
  --log-file /absolute/path/to/downloaded_dbt_log.txt \
  --output-dir /path/to/gp-data-platform/.claude/skills/l2-uniform-drift-remediator/dbt_logs/failure_handler \
  --dbt-project-path /path/to/gp-data-platform/dbt/project \
  --execute-safe
```

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
