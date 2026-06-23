Regenerate the committed Amplitude event git-provenance dataset and open a PR with the refreshed file. Built to run unattended on a schedule (a Claude Code Desktop routine), and to work by hand. The data work lives in `analytics/lib/amplitude_event_provenance_backfill.py`; this runbook is the orchestration around it (preflight, verify, branch, commit, PR).

## Prerequisites

- **Omni checkout.** `OMNI_REPO` must be set to a local omni checkout (machine-specific; keep it in `~/.zshrc`, not committed). The script resolves `--repo` else `$OMNI_REPO`, expands `~`, and aborts if the path is unset or not a git checkout. It fetches `origin/develop` itself, so the checkout only needs a reachable remote, not a clean working tree.
- **Databricks access.** One read per run (the event universe from `airbyte_source.amplitude_taxonomy_event_type`). Auth is the Databricks SDK profile in `~/.databrickscfg` (default profile `tristana_goodparty`; refresh with `databricks auth login --profile tristana_goodparty`). `DATABRICKS_HTTP_PATH` must be set. No PAT.
- **Git/PR access.** `gh` authenticated and push access to the repo, since the run opens a PR. Never push to `main`.
- **Python env.** Run from `analytics/` via `uv` (the subproject's own env), per the repo's multi-venv setup.

## Steps

1. **Preflight. Stop loudly on any failure; never commit a half-run.**
   - `databricks auth profiles` shows `Valid: YES` for the default profile. If not, stop and report that re-auth is needed (`databricks auth login --profile tristana_goodparty`). An expired token is the most common unattended failure.
   - `OMNI_REPO` is set and `"$OMNI_REPO/.git"` exists. (The script also checks this, but failing here gives a cleaner message.)
2. **Branch.** From an up-to-date `main`, create `chore/refresh-event-provenance-<YYYY-MM-DD>`. Do the work on the branch, not on `main`.
3. **Run the refresh.**
   ```sh
   cd analytics && uv run python lib/amplitude_event_provenance_backfill.py
   ```
   The script auto-detects: no state file means a full backfill, a state file means an incremental walk of `last_sha..origin/develop`. It rewrites `analytics/data/amplitude_event_provenance.csv` and `..._state.json`.
4. **Verify before committing.**
   - Read the summary line on stderr: `N rows (present=…, removed=…, not_found_in_code=…)`. `N` should be ~430+. If it has collapsed toward zero, something went wrong upstream (bad universe read, empty walk); stop and report rather than commit.
   - `git status --porcelain` shows only `analytics/data/amplitude_event_provenance.csv` and `analytics/data/amplitude_event_provenance_state.json`. Any other changed path means stop and investigate.
   - If `git status` is clean (no new commits on `origin/develop` since the last watermark), there is nothing to refresh. Delete the throwaway branch and finish without a PR.
5. **Commit and open a PR.** Commit the two data files; push the branch; open a PR summarizing the row delta (e.g. the before/after summary counts). Commit message ends with the co-author trailer:
   ```
   chore(analytics): refresh Amplitude event provenance dataset

   Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
   ```
6. **Report** the PR URL in the run summary.

## Notes

- **Idempotent and self-catching-up.** The watermark is the last processed commit SHA, so a missed or failed run is harmless: the next successful run walks a larger window and catches up. There is no need to backfill a skipped day.
- **Refresh limitation.** A refresh only adds or updates events that changed in the window. A brand-new universe event whose instrumentation predates the watermark is not added automatically; the run logs a `WARNING: … absent from the CSV`. To fully resync, delete `analytics/data/amplitude_event_provenance_state.json` and run again for a full backfill.

## Troubleshooting

- Databricks auth / token-refresh error → the profile's OAuth token is stale; run `databricks auth login --profile tristana_goodparty`. This needs a browser, so it cannot be done by the unattended run; the run fails loudly and waits for the next scheduled run after you re-auth.
- `ERROR: pass --repo or set OMNI_REPO …` → `OMNI_REPO` is not set in the run's environment.
- `git fetch … failed` → the omni checkout's remote is unreachable or its credentials lapsed; the script aborts rather than walking stale `origin/develop` state.
- `WARNING: N universe event(s) absent from the CSV` → see the refresh-limitation note above; resync with a full backfill if the gap matters.

## Running it on a schedule (Claude Code Desktop)

Create a Local routine whose working directory is the repo root and whose prompt is "follow `analytics/runbook/refresh-event-provenance.md`." Set the permission mode to auto-approve so it can run `uv`, `git`, and `gh` without stalling, and pick a cadence and a time the machine is awake and 1Password / the Databricks profile are usable. A missed run is harmless (see Notes), so err toward a time you are typically logged in.
