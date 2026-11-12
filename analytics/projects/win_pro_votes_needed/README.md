# Win Pro usage by votes needed to win (DATA-2809)

Input to Win pricing and packaging. For paying Win Pro customers with a 2026 election (Pro today, or
canceled on or after election day), it shows
feature use, outreach channels, and texting volume and spend by votes-needed-to-win band.

## Files

| File | What it is |
|---|---|
| `win_pro_votes_needed_brief.yaml` | The approved framing: population, window, definitions, checks. Read this first. |
| `build_working_set.py` | Two warehouse queries. Writes `datasets/working_set.parquet` (one row per user) and `datasets/area_days.parquet`. |
| `analyze.py` | Every cut, in pandas. Runs no queries. `python analyze.py` prints all tables. |
| `build_notebook.py` | Builds and runs `notebooks/win_pro_votes_needed.ipynb` from `analyze.py`. |
| `notebooks/win_pro_votes_needed.ipynb` | The results, with outputs saved. |

`datasets/` is gitignored because it holds per-user rows. Rebuild it locally.

## Rerun

You need Databricks access set up for `analytics/` (see `analytics/runbook/run-product-analysis.md`,
Prerequisites: a `databricks auth login` profile and `DATABRICKS_HTTP_PATH`).

```bash
cd analytics
uv sync
uv run --with pyarrow python projects/win_pro_votes_needed/build_working_set.py
uv run --with pyarrow python projects/win_pro_votes_needed/analyze.py          # tables to stdout
uv run --with pyarrow --with nbformat --with nbclient --with ipykernel \
    python projects/win_pro_votes_needed/build_notebook.py                     # refresh the notebook
```

Numbers move between runs because `user_journey` and the turnout projections update. Each
run prints its timestamp. Quote it beside any number.

## Definitions that are easy to get wrong

- **Votes needed to win.** gp-api stores no win number. It computes it on request
  (`CampaignsService.fetchLiveRaceTargetMetrics` in omni gp-api) as half the projected turnout plus
  one, from the first of: the organization's override district (`mart_civics.organizations`), the
  campaign's race (`mart_civics.campaigns.ballotready_race_id` = `m_election_api__race.br_database_id`,
  the same race gp-api reads from `campaign.details.raceId`), the organization's position's district.
  `build_working_set.py` repeats that rule. Do not use `win_number` on `users_win_candidacy` or
  `user_journey`: it is BallotReady-only and empty for 2026. The `gp_api_db_path_to_victory` table
  stops in April 2026.
- **Spend.** The Revenue dashboard's rule: `user_journey_events` Payment and Refund rows labelled
  `outreach_purchase`, primary Stripe customer row only, refunds already negative. Only texting is
  paid, so other channels are counts.
- **Sends.** `user_journey_events` Outreach send rows (committed sends, DATA-2477), channel in
  `event_label`, text count in `event_value`. Identical to `int__user_outreach_sends` for this population.
  A texter is anyone with a texting send; volume is measured only where the count is known.
- **Feature areas.** Same action events and area mapping as DATA-2690
  (`projects/win_usage_archetypes/build_working_set.py`), machine-emitted events dropped.
- **Window.** From six months before the election date through the end of election day.
- **Sources.** Marts first (`user_journey`, `user_journey_events`, `mart_civics`). The brief's
  `source_model` lists each upstream read and why no mart covers it.

To change the population, window or bands, edit the constants at the top of `build_working_set.py`
(`ELECTION_YEAR`, `WINDOW_MONTHS`) and `analyze.py` (`EDGES`, `BANDS`, `BUNDLE_TEXTS`). Note the change
in the brief.
