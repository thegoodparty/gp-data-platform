# Baseline benchmark question

A fixed, reusable yardstick for comparing how Win-analytics pipeline decisions
affect runtime. Run the same question through the pipeline under different
"arms" (decision branches), profile each run with `pipeline_profiler.py`, and
read the per-stage time table plus the `**Decisions:**` line to see which choices
cost what. The flags are observational, not causal: they record which branch a
run took so you can compare like with like.

## The fixed question

Keep this verbatim and fixed-window so a run today and a run next month do the
same work and stay comparable:

> For each complete calendar month of 2026 (January through May), for Win users,
> give the breakdown of: total users, activated users, users that viewed
> dashboard, users that completed onboarding, and the percentage of each metric
> relative to total users.

Single, well-defined, but deliberately carries real scoping decisions so it
exercises the framing stage (the stage that dominates runtime). Do not widen the
window to "the current month" or the workload drifts over time.

## Scoping answer key

The question contains scoping decisions that the framing stage resolves. Do NOT
pre-resolve them here. Run the **full** arm first; copy its approved brief's
resolutions into this section, then every other arm reuses them so framing is
held constant and the arms stay comparable.

- **"total users" (the percentage denominator):** per month, does it mean Win
  users who signed up that month, who were active that month, or cumulative as
  of that month? (resolved by first full run: **cumulative** registered Win users
  as of each month-end, restricted to an upcoming/live election: per-user
  `MAX(election_date)` **bounded to `[2020-01-01, 2050-01-01]`** `>= the measured
  month's start`, using `users_win_candidacy.election_date` (the per-stage field,
  NOT the leaky `users_win_base.election_date`), deduped to distinct `user_id`
  (MIN `user_created_at`), `is_latest_version AND NOT is_demo`. The bound removes
  corrupt far-future dates (e.g. year 20021) that would otherwise read as
  perpetually upcoming — see win-analytics-knowledge `gotchas.md`. Reproducible
  base: Jan 9,880 / Feb 10,710 / Mar 11,241 / Apr 11,286 / May 10,880 (as of
  2026-07-15, DATA-2114 re-baseline, midnight reading). The filter drops
  past-election candidates whose candidacy is over. Month-end cutoffs in these
  numbers use the midnight reading — `user_created_at <= DATE month_end`, which
  excludes activity later on the last calendar day; the full-last-day reading
  adds ~6–40 users/month (measured 2026-07-15, DATA-2114). State the chosen
  reading in the brief's `observation_window.boundary_semantics`.)
- **"activated users":** the governed activation definition. (resolved: anchored
  point-in-time via `first_campaign_sent_at <= month-end`, NOT the lifetime
  `is_activated` flag, so post-month activations don't leak into earlier months.)
- **"completed onboarding":** spans the May-2026 onboarding-flow cutover
  (`onboarding_complete` vs `Onboarding - Candidate Pledge Completed`); needs the
  unified definition. (resolved: the version-agnostic raw event
  `Onboarding - Candidate Pledge Completed` fired within 14 days of signup, with
  `event_time <= month-end`.)
- **"viewed dashboard":** the dashboard-view event. (resolved: the raw event
  `Dashboard - Candidate Dashboard Viewed` fired within 14 days of signup, with
  `event_time <= month-end`.)

## Grading tolerance

The mart is live and not snapshotted, so the recorded base numbers drift as
election dates and campaign versions churn (~0.1–0.3% observed on 2026-06-11
and 2026-07-15). Grade a rep against the answer key within ~0.5% on the
denominator before investigating; a matched-semantics rerun that lands inside
that band is a pass. When re-baselining, record the as-of date next to the new
numbers.

## Runnable arms

Ranked by expected wall-clock lever (largest first). Each arm changes exactly one
decision from the full baseline; the headline arm changes all of them.

1. **Framing depth** — full scoping conversation vs express (take the answer-key
   defaults, ask at most one clarifying question, go straight to the brief).
2. **Deliverable** — inline answer only vs committed `.py` script vs full
   notebook (which also triggers the `build_*_nb.py` headless-build step).
3. **Review** — both reviewers plus the results gate vs skip review entirely.
   (One-vs-two barely moves wall-clock: reviewers run in parallel and the
   profiler scores review as max-span, and reviewer-internal tokens are not in
   the transcript. The meaningful cut is review vs no-review.)
4. **Calibration** — full vs skip. (Smallest lever.)
5. **Headline** — full pipeline vs fully-express (express framing + inline
   deliverable + no review + no calibration), end to end, for the total saving.

## How to run an arm

Each arm is a full interactive run via `analytics/runbook/run-product-analysis.md`,
explicitly forcing the arm's branch (for example "produce a notebook" vs "answer
inline, do not write an artifact"; "dispatch both reviewers" vs "skip review").
Note each run's session id (its transcript filename under
`~/.claude/projects/-Users-tristan-Documents-0-goodparty-0-repos-gp-data-platform/`).
For signal above run-to-run latency noise, do about two reps per arm.

## How to profile

```bash
cd analytics
uv run python diagnostics/pipeline_profiler.py \
  ~/.claude/projects/-Users-tristan-Documents-0-goodparty-0-repos-gp-data-platform/<session-id>*.jsonl \
  [<more session ids> ...]
```

Read the per-stage table (`model_active_min`, `human_idle_min`, `wall_clock_min`,
`%_of_active`) and the `**Decisions:**` line. Use the decisions line to confirm
each run actually took its intended branch (e.g. notebook arm shows `notebook ×N`
or `nb-build ×N`; review-skipped arm shows `reviewers — none`). A run flagged
`confidence: low` had out-of-order or missing markers (e.g. a contaminated /
cross-session run); treat its numbers with caution.

Every run also auto-saves its full output (stage tables + the decisions line) to a
timestamped `analytics/diagnostics/logs/profile_<YYYYMMDD-HHMMSS>.md` (gitignored),
so you can review and compare past arms later without re-running.
