---
name: analytics-process
description: Use when planning, executing, reviewing, or calibrating a product analysis, or when asked a product data question that should become one (the entry runbook routes by product — Win and Serve today). For the data facts themselves, use the product's knowledge skill (win-analytics-knowledge or serve-analytics-knowledge).
---

# Analytics process

How a product analysis is run, from a fuzzy question to a reviewed result. For the data
facts themselves (which table, which metric, which join), use the product's knowledge skill
(**win-analytics-knowledge**, **serve-analytics-knowledge**) — this skill is about *how to
work*, not *what is true about the data*.

## The senior-analyst loop

An index, not the specification: each step's operational detail lives in its reference doc,
and the stage/actor/gate topology lives in [`references/pipeline.md`](references/pipeline.md),
the single description of the pipeline flow. Two legibility conventions are always on —
announce each step transition with a one-line stage banner, and each cleared checkpoint with a
gate receipt; formats and stock why-clauses live in `pipeline.md` ("Legibility").

1. **Clarify and scope (framing).** Run the framing routine ([`references/framing.md`](references/framing.md)) in your own context — it converses with the user. No execution code until the human approves the framing.
2. **Find sources.** Resolve every concept through the knowledge skill, and verify named tables/columns/events against the live catalog — docs drift.
3. **Produce the brief.** The framing output is a structured brief per [`references/brief-schema.md`](references/brief-schema.md), the framing→execution contract.
4. **Execute.** Build the working set once, then slice every cut from it in pandas — the discipline is in [`references/methodology.md`](references/methodology.md); the product's defaults, cohort tables, and working-set column caveats are in the product knowledge skill's `methodology_defaults.md`.
5. **Review.** After the results checkpoint, route the executed analysis through the two adversarial reviewer agents; the gate and dispatch rules are in `pipeline.md`.
6. **Close the loop.** Run the calibration pass ([`references/calibration.md`](references/calibration.md)), ending with the candidates-ledger read-back it requires. Pruning happens via **consolidation mode**, detailed in the same doc.

## Reusable analysis patterns

Common work that should not be reinvented each run:

- **Build-once-slice-many.** One consolidated per-user `cohort × engagement` working set via the product's committed builder, carrying the standard slice dimensions so re-cuts are free pandas `groupby`s. Builder and column caveats: the product knowledge skill's `methodology_defaults.md`.
- **Funnel analysis.** Aggregate the raw event stream to `user_id × funnel step`; report "engaged beyond account creation" (≥2 distinct events) alongside raw any-evidence. Event families come from the product knowledge skill (Win: `engagement.md`; Serve: `sources.md`).
- **Rate decomposition.** Don't pool heterogeneous funnel stages — name the cohort (onboarded / activated / full registered) so a rate isn't dominated by never-active registrants. See the default-cohorts table in the product knowledge skill's `methodology_defaults.md`.
- **Retention / reverse-retention curves.** Pair any closed-cohort curve with an open fixed-denominator view, then split open into new vs returning. The closed curve decays by construction; the open/returning view answers whether the base is more engaged. Full mechanism in the knowledge skill's `gotchas.md`.
- **Wilson intervals + binning.** Report Wilson 95% CIs where they inform the read (small N, or a claimed difference) rather than on every cut; flag N<30 bins; pre-register bins in the brief. See `methodology.md`.

## Reference docs

- [`references/framing.md`](references/framing.md) — the framing routine (persona, question set, pre-brief verification) the orchestrator runs in step 1.
- [`references/pipeline.md`](references/pipeline.md) — stages, agents, handoff contracts (descriptive, not active).
- [`references/methodology.md`](references/methodology.md) — product-agnostic discipline: scoping checklist, folder patterns, query patterns, binning, verification protocol. Product defaults (scoping decisions, cohorts, working-set builder) live in the product knowledge skill's `methodology_defaults.md`.
- [`references/brief-schema.md`](references/brief-schema.md) — the analysis-brief YAML contract + amend-vs-reframe rule.
- [`references/calibration.md`](references/calibration.md) — the calibration-log convention, the candidates ledger, the ledger read-back, consolidation mode, and the over-calibration / bloat cautions.
- [`references/event-lifecycle-assets.md`](references/event-lifecycle-assets.md) — the omni event-lifecycle assets (provenance CSV, health log, gp-meta): cross-product source material consumed by the framing version-continuity check and every product's knowledge skill.
