---
name: serve-analytics-knowledge
description: Use when a Serve-product data concept — EO activation, Serve engagement/MAU, People Served, the onboarding funnel, Serve Amplitude events, hygiene filters — needs to resolve to its governed metric, table, column, or definition before a query is written. Not for running analyses (that is the analytics-process skill).
---

# Serve analytics knowledge

A router for the data facts behind Serve-product analyses. Its job is to take a concept
("active EO," "people served," "Serve engagement") and narrow it to the **one** governed
definition and the single doc that owns the detail — before any SQL is written. It does not
execute analyses; for the analyst workflow and patterns, see the **analytics-process** skill.

This skill is a **skeleton by design** (DATA-2116, seeded 2026-07-14 from the DATA-2115
scoping decisions). Expect early Serve runs to hit gaps; the calibration loop fills them run
by run. When a fact is missing here, verify against the live catalog and propose the doc
addition through the calibration pass.

## How to resolve a concept

1. **Check the canonical registry first.** Open [`references/canonical_metrics.md`](references/canonical_metrics.md). If the concept is there, use that governed one-line definition and follow its "owns detail" link. The registry is the governed layer — do not redefine a metric that already has a row.
2. **If it's not canonical, route by domain** using the table below, then open that one doc.
3. **Verify against the live catalog.** The live `goodparty_data_catalog` (or the model SQL) is ground truth, not these docs — docs drift, and this skill is young. Confirm any named table/column/event before relying on it. The verification protocol lives in the process skill's `methodology.md`.

## Domain routing table

| IF the question is about… | Use | Do NOT… |
|---|---|---|
| What a metric *means* / its governed definition | [`references/canonical_metrics.md`](references/canonical_metrics.md) | invent a new definition for a concept that has a row |
| Which table to start from, the Serve data domains, the event landscape | [`references/sources.md`](references/sources.md) | |
| When an event was added/retired in code, its lifecycle status, what superseded it | the omni event-lifecycle assets — [`event-lifecycle-assets.md`](../analytics-process/references/event-lifecycle-assets.md) (process skill; cross-product) | infer an event's existence era from data-observed first-seen dates |
| Engagement / activity measurement, the broad-engagement family, the MAU collapse | [`references/methodology_defaults.md`](references/methodology_defaults.md) | headline any poll-anchored series without its collapse caveat |
| Slicing dimensions (office, level, state, funnel stage) | [`references/segmentation.md`](references/segmentation.md) | assume office/level/state segmentation exists (pending officeholder ingestion) |
| Serve's analysis defaults (resolved scoping, default cohorts, the working-set builder, reviewer doc pointers) | [`references/methodology_defaults.md`](references/methodology_defaults.md) | count server-emitted events (`session_id = -1`) as engagement |
| A symptom you're seeing / a recurring trap | [`references/gotchas.md`](references/gotchas.md) | restate a fact the symptom table points elsewhere for |

## Governance

The canonical definitions are human-owned. Claude may draft prose, column descriptions, and
gotchas in the domain docs, but must not invent or silently change a metric definition in
`canonical_metrics.md` — flag a proposed change for the metric owner instead.

Each data fact lives in exactly one file. When a domain doc fully owns a fact, other docs (and
the gotchas symptom table) point to it rather than restating it. Keep it that way.
