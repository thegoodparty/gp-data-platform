#!/usr/bin/env bash
# One-shot reverse pull: Databricks Workspace scratchpad -> a project's local notebook.
# Use AFTER editing in the Databricks UI to capture changes locally before commit.
#
# Usage:
#   ./analytics/scripts/pull.sh <project_folder> <notebook_name_no_ext>
#
# Examples:
#   ./analytics/scripts/pull.sh win_churn win_churn_phase1
#   ./analytics/scripts/pull.sh win_outcomes_scout inventory_queries
#
# Recommended flow:
#   1. Ctrl+C the sync.sh watcher.
#   2. Save the notebook in the Databricks UI (Cmd+S).
#   3. ./analytics/scripts/pull.sh <project> <notebook>
#   4. git diff to review.
#   5. Commit locally (so pre-commit / nbstripout runs).
#   6. Re-launch sync.sh.
#
# Override DATABRICKS_WORKSPACE_USER env var if your Databricks Workspace
# userName is not tristan.aubert@goodparty.org.

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <project_folder> <notebook_name_no_ext>"
    echo "Example: $0 win_outcomes_scout inventory_queries"
    exit 1
fi

PROJECT="$1"
NB_NAME="$2"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCAL_FILE="${REPO_ROOT}/analytics/${PROJECT}/notebooks/${NB_NAME}.ipynb"
WORKSPACE_USER="${DATABRICKS_WORKSPACE_USER:-tristan.aubert@goodparty.org}"
# Source path has NO .ipynb extension — Databricks strips it when registering
# a notebook. Including it triggers "Path doesn't exist".
SOURCE="/Workspace/Users/${WORKSPACE_USER}/scratchpad/${NB_NAME}"

if [ ! -d "$(dirname "${LOCAL_FILE}")" ]; then
    echo "Error: $(dirname "${LOCAL_FILE}") does not exist"
    exit 1
fi

echo "Pulling ${SOURCE} -> ${LOCAL_FILE}"
# --file overwrites silently; there is no --overwrite flag for export.
databricks workspace export "${SOURCE}" --file "${LOCAL_FILE}" --format JUPYTER

echo "Done. Review with: git diff -- ${LOCAL_FILE}"
