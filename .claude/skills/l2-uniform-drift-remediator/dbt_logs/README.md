# dbt_logs

Local working directory for downloaded dbt Cloud logs and generated remediation artifacts.

Usage pattern:
- download failed-run preflight log(s) into this folder
- run the L2 drift failure handler with `--output-dir .claude/skills/l2-uniform-drift-remediator/dbt_logs/failure_handler`
- copy sanitized summaries into PR descriptions or tracked runbook files

Do not commit raw operational logs from this folder.
