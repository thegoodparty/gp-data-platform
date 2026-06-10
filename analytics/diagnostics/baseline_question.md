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
  of that month? (resolved by first full run: _____)
- **"activated users":** the governed activation definition. (resolved: _____)
- **"completed onboarding":** spans the May-2026 onboarding-flow cutover
  (`onboarding_complete` vs `Onboarding - Candidate Pledge Completed`); needs the
  unified definition. (resolved: _____)
- **"viewed dashboard":** the dashboard-view event. (resolved: _____)

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
