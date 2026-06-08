---
name: win-analytics-process
description: The senior-analyst workflow for running a Win-product analysis — clarify and scope the question, find the right sources via the win-analytics-knowledge skill, build the working set once and slice it, then route through the adversarial reviewers and close with a calibration pass. Use when planning, executing, or reviewing a Win analysis (framer, executor, or reviewer). Carries the reusable analysis patterns, the brief contract, and the pipeline topology.
---

# Win analytics process

How a Win-product analysis is run, from a fuzzy question to a reviewed result. For the data
facts themselves (which table, which metric, which join), use the **win-analytics-knowledge**
skill — this skill is about *how to work*, not *what is true about the data*.

## The senior-analyst loop

1. **Clarify and scope (framing).** Sharpen the question to a one-sentence hypothesis with a decision attached, and resolve scoping forks before writing any code. Run the framing routine in [`references/framing.md`](references/framing.md) in your own context (ask the user, iterate); see [`references/methodology.md`](references/methodology.md) for the scoping checklist, resolved defaults, and cohort selection. Framing ends at the human-approval gate; no execution code until the brief is approved.
2. **Find sources.** Resolve every concept through the **win-analytics-knowledge** skill so each maps to its one governed metric/table. Verify named tables/columns/events against the live catalog — docs drift.
3. **Produce the brief.** The output of the framing step (step 1) is a structured brief — not a separate invocation; the format is the framing→execution contract in [`references/brief-schema.md`](references/brief-schema.md).
4. **Execute.** Build the working set once with `analytics/lib/win_analysis.py`, then slice every cut from it in pandas (build-once-slice-many; see `methodology.md`). Anchor features point-in-time so post-anchor activity doesn't leak. Note the helper's `onboarded` column is the new-flow-blind `onboarding_complete` flag, **not** the canonical Onboarded cohort — see the column caveat in `methodology.md` before using it.
5. **Review.** Route the executed analysis through the adversarial reviewers — `product-data-scientist` (methodology + interpretation) and `product-manager` (usefulness). Keep the reviewers on the strong model.
6. **Close the loop.** Run the calibration pass ([`references/calibration.md`](references/calibration.md)): triage findings into the file that owns them, or record that none were needed. Required closing step.

The ordered stages, who does what, and what artifact passes between them are in
[`references/pipeline.md`](references/pipeline.md) — the single description of the pipeline flow.

## Reusable analysis patterns

Common work that should not be reinvented each run:

- **Build-once-slice-many.** One consolidated per-user `cohort × engagement` working set via `analytics/lib/win_analysis.py`, carrying the standard slice dimensions so re-cuts are free pandas `groupby`s. See `methodology.md`.
- **Funnel analysis.** Aggregate the raw event stream to `user_id × funnel step`; report "engaged beyond account creation" (≥2 distinct events) alongside raw any-evidence. Event families come from the knowledge skill's `engagement.md`.
- **Rate decomposition.** Don't pool heterogeneous funnel stages — name the cohort (onboarded / activated / full registered) so a rate isn't dominated by never-active registrants. See the default-cohorts table in `methodology.md`.
- **Retention / reverse-retention curves.** Pair any closed-cohort curve with an open fixed-denominator view, then split open into new vs returning. The closed curve decays by construction; the open/returning view answers whether the base is more engaged. Full mechanism in the knowledge skill's `gotchas.md`.
- **Wilson intervals + binning.** Report Wilson 95% CIs alongside point estimates; flag N<30 bins; pre-register bins in the brief. See `methodology.md`.

## Reference docs

- [`references/framing.md`](references/framing.md) — the framing routine (persona, question set, pre-brief verification) the orchestrator runs in step 1.
- [`references/pipeline.md`](references/pipeline.md) — stages, agents, handoff contracts (descriptive, not active).
- [`references/methodology.md`](references/methodology.md) — scoping, default cohorts, query patterns, verification protocol, source pointers.
- [`references/brief-schema.md`](references/brief-schema.md) — the analysis-brief YAML contract + amend-vs-reframe rule.
- [`references/calibration.md`](references/calibration.md) — the calibration-log convention and the over-calibration / bloat cautions.
