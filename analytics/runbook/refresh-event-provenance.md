Regenerate the committed Amplitude event git-provenance dataset and open a PR with the refreshed file. Built to run unattended on a schedule (a Claude Code Desktop routine), and to work by hand. The data work lives in `analytics/lib/amplitude_event_provenance_backfill.py`; this runbook is the orchestration around it (preflight, verify, branch, commit, PR).

## Prerequisites

- **Omni checkout.** `OMNI_REPO` must be set to a local omni checkout (machine-specific, not committed; see Scheduled-shell environment below for where to set it). The script resolves `--repo` else `$OMNI_REPO`, expands `~`, and aborts if the path is unset or not a git checkout. It fetches `origin/develop` itself, so the checkout only needs a reachable remote, not a clean working tree.
- **Databricks access.** One read per run (the event universe from `airbyte_source.amplitude_taxonomy_event_type`). Auth resolves the executor's default Databricks SDK profile in `~/.databrickscfg` (whoever runs it; refresh with `databricks auth login`, or `databricks auth login --profile <name>` for a named profile). `DATABRICKS_HTTP_PATH` (the SQL warehouse path, non-secret) must be set; see below. No PAT.
- **Scheduled-shell environment.** A Claude Code scheduled run is a non-interactive shell and does NOT source `~/.zshrc`, so any var defined only there is absent (symptom: `DATABRICKS_HTTP_PATH is not set`). Set the two non-secret vars this job needs, `DATABRICKS_HTTP_PATH` and `OMNI_REPO`, where a non-interactive shell reads them: a `~/.claude/settings.json` `env` block (Claude Code injects it into every Bash subprocess it spawns) or `~/.zshenv`. Auth itself needs no env here, since it reads the file-based OAuth profile.
- **Git/PR access.** `gh` authenticated and push access to the repo, since the run opens a PR. Never push to `main`.
- **Python env.** Run from `analytics/` via `uv` (the subproject's own env), per the repo's multi-venv setup.

## Steps

1. **Preflight. Stop loudly on any failure; never commit a half-run.**
   - `databricks auth profiles` shows `Valid: YES` for the executor's default profile. If not, stop and report that re-auth is needed (`databricks auth login`, or `--profile <name>` for a named profile). An expired token is the most common unattended failure.
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

- Databricks auth / token-refresh error → the executor's OAuth token is stale; run `databricks auth login` (or `--profile <name>`). This needs a browser, so it cannot be done by the unattended run; the run fails loudly and waits for the next scheduled run after you re-auth.
- `DATABRICKS_HTTP_PATH is not set` → the scheduled (non-interactive) shell did not load `~/.zshrc`; set `DATABRICKS_HTTP_PATH` (and `OMNI_REPO`) via a `~/.claude/settings.json` env block or `~/.zshenv`. See the Scheduled-shell environment note in Prerequisites.
- `ERROR: pass --repo or set OMNI_REPO …` → `OMNI_REPO` is not set in the run's environment (same non-interactive-shell cause as above).
- `git fetch … failed` → the omni checkout's remote is unreachable or its credentials lapsed; the script aborts rather than walking stale `origin/develop` state.
- `WARNING: N universe event(s) absent from the CSV` → see the refresh-limitation note above; resync with a full backfill if the gap matters.

## Test run (no commit, no PR)

To validate the pipeline end to end without touching the repo or opening a PR, point the output at files outside the repo (`--csv` / `--state`). This exercises everything (auth, the `origin/develop` fetch, the walk, the write) and leaves an inspectable dataset in `~/Downloads`. The only in-repo side effect is the `git fetch` inside the omni checkout, which is expected.

1. Preflight as in Step 1 above (profile valid, `OMNI_REPO` set).
2. Seed the test output from the committed dataset so the run takes the incremental path (what the scheduled job does each run):
   ```sh
   cp analytics/data/amplitude_event_provenance.csv ~/Downloads/amplitude_event_provenance_TEST.csv
   cp analytics/data/amplitude_event_provenance_state.json ~/Downloads/amplitude_event_provenance_state_TEST.json
   ```
3. Run, writing only to the Downloads copies:
   ```sh
   cd analytics && uv run python lib/amplitude_event_provenance_backfill.py \
     --repo "$OMNI_REPO" \
     --csv ~/Downloads/amplitude_event_provenance_TEST.csv \
     --state ~/Downloads/amplitude_event_provenance_state_TEST.json
   ```
4. Inspect: read the summary line on stderr, `diff` the Downloads CSV against `analytics/data/amplitude_event_provenance.csv` to see the delta, and confirm `git status --porcelain` is clean.

For a clean full backfill instead of the incremental path, skip step 2 and point `--state` at a Downloads path that does not exist yet; with no watermark the script rebuilds the whole dataset into Downloads (slower, a few minutes).

## Running it on a schedule (Claude Code Desktop)

Create a Local routine whose working directory is the repo root and whose prompt is "follow `analytics/runbook/refresh-event-provenance.md`." Set the permission mode to auto-approve so it can run `uv`, `git`, and `gh` without stalling, and pick a cadence and a time the machine is awake and 1Password / the Databricks profile are usable. A missed run is harmless (see Notes), so err toward a time you are typically logged in.
