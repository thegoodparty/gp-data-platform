---
name: win-analytics-knowledge
description: Use when a Win-product data concept — engagement, activation, outcomes/win rate, viability, segmentation, Amplitude events, joins, PMF/satisfaction — needs to resolve to its governed metric, table, column, or definition before a query is written. Not for running analyses (that is the analytics-process skill).
---

# Win analytics knowledge

A router for the data facts behind Win-product analyses. Its job is to take a concept
("engagement," "a win," "viability") and narrow it to the **one** governed definition and the
single doc that owns the detail — before any SQL is written. It does not execute analyses; for
the analyst workflow and patterns, see the **analytics-process** skill.

## How to resolve a concept

1. **Check the canonical registry first.** Open [`references/canonical_metrics.md`](references/canonical_metrics.md). If the concept is there, use that governed one-line definition and follow its "owns detail" link to the single domain doc for caveats, coverage, and query patterns. The registry is the governed layer — do not redefine a metric that already has a row.
2. **If it's not canonical, route by domain** using the table below, then open that one doc.
3. **Verify against the live catalog.** The live `goodparty_data_catalog` (or the model SQL) is ground truth, not these docs — docs drift. Confirm any named table/column/event before relying on it. The verification protocol lives in the process skill's `methodology.md`.

## Domain routing table

| IF the question is about… | Use | Do NOT… |
|---|---|---|
| What a metric *means* / its governed definition | [`references/canonical_metrics.md`](references/canonical_metrics.md) | invent a new definition for a concept that has a row |
| Which table to start from, the data domains, the civics mart structure | [`references/sources.md`](references/sources.md) | |
| When an event was added/retired in code, its lifecycle status, what superseded it | the omni event-lifecycle assets — [`event-lifecycle-assets.md`](../analytics-process/references/event-lifecycle-assets.md) (process skill; cross-product) | infer an event's existence era from data-observed first-seen dates |
| IDs, join keys, join recipes (user→outcome, survey 2-hop, viability) | [`references/joins.md`](references/joins.md) | join `hs_contact_id` to `users_win_candidacy.hubspot_id` (company id) |
| Win/loss outcome, vote share, self-reported outcome, PMF / satisfaction | [`references/outcomes.md`](references/outcomes.md) | use `candidacy_result` for binary win/loss |
| Engagement / activation metrics, the Amplitude event landscape, onboarding flow versions, UTM | [`references/engagement.md`](references/engagement.md) | use `is_onboarded` / `has_completed_onboarding_flow` across the 2026-05-07 cutover |
| Viability Score 2.0 — definition, bands, coverage, calibration | [`references/viability.md`](references/viability.md) | bucket into deciles (the score is bimodal) |
| Slicing dimensions (office, level, state, party, Pro, ICP) | [`references/segmentation.md`](references/segmentation.md) | **filter** on `icp_office_win` (slice instead) |
| Win's analysis defaults (resolved scoping, default cohorts, the working-set builder + its column caveats, reviewer doc pointers) | [`references/methodology_defaults.md`](references/methodology_defaults.md) | use the builder's `onboarded` column as the canonical Onboarded cohort |
| A symptom you're seeing / a recurring trap | [`references/gotchas.md`](references/gotchas.md) | restate a fact the symptom table points elsewhere for |

## Governance

The canonical definitions are human-owned. Claude may draft prose, column descriptions, and
gotchas in the domain docs, but must not invent or silently change a metric definition in
`canonical_metrics.md` — flag a proposed change for the metric owner instead.

Each data fact lives in exactly one file. When a domain doc fully owns a fact, other docs (and
the gotchas symptom table) point to it rather than restating it. Keep it that way.
