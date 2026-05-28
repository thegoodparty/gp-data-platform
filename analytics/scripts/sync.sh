#!/usr/bin/env bash
# One-way watch sync: a project's local notebooks/ -> Databricks Workspace scratchpad.
# Run once per session in a dedicated terminal tab and leave running.
# Ctrl+C to stop.
#
# Usage:
#   ./analytics/scripts/sync.sh <project_folder>
#
# Examples:
#   ./analytics/scripts/sync.sh win_churn
#   ./analytics/scripts/sync.sh win_outcomes_scout
#
# Source: analytics/<project_folder>/notebooks (the directory passed in)
# Target: /Workspace/Users/${DATABRICKS_WORKSPACE_USER}/scratchpad/
#         (flat scratchpad — all projects' notebooks land side-by-side, so
#         notebook filenames must be unique across projects)
#
# Override DATABRICKS_WORKSPACE_USER env var if your Databricks Workspace
# userName is not tristan.aubert@goodparty.org.
#
# See analytics/<project>/SESSION_NOTES.md for the rest of the per-project workflow.

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <project_folder>"
    echo "Example: $0 win_outcomes_scout"
    exit 1
fi

PROJECT="$1"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SOURCE_DIR="${REPO_ROOT}/analytics/${PROJECT}/notebooks"
WORKSPACE_USER="${DATABRICKS_WORKSPACE_USER:-tristan.aubert@goodparty.org}"
TARGET="/Workspace/Users/${WORKSPACE_USER}/scratchpad"

if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: ${SOURCE_DIR} does not exist"
    exit 1
fi

echo "Syncing ${SOURCE_DIR} -> ${TARGET} (Ctrl+C to stop)"
# --include '*.ipynb' is required because the repo-root .gitignore matches
# analytics/*/notebooks/ and `databricks sync` honors gitignore by default,
# which would otherwise silently skip every file in this directory.
databricks sync "${SOURCE_DIR}" "${TARGET}" --include '*.ipynb' --watch
