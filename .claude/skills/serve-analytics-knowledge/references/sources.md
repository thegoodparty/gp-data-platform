# Data sources reference

Part of the **serve-analytics-knowledge** skill. Where Serve-product data lives and which
table to start from.

## Quick reference

- **Business context:** Serve is GoodParty's product for elected officials (EOs) — polls,
  constituent briefings, community issues, AI chief-of-staff. Nearly every Serve user came
  through the Win funnel: **1,860 of 1,869 Serve users are also Win candidates** (verified
  2026-07-14), so a user's event stream mixes campaigning and serving. Time-scope on
  `eo_activated_at` to separate them.
- **Entity grain:** one row per `user_id` on the primary working table.
- **Standard hygiene filter:** `is_serve_user`, exclude `@goodparty.org` emails, exclude
  impersonation-tainted sessions, count in-session events only — see
  [methodology_defaults.md](methodology_defaults.md).

## Key tables

For most Serve analyses, start with one of these:

- **`mart_analytics.users_serve_base`** — user-grain view over civics `users`: Serve flags
  (`is_serve_user`, `eo_activated_at`), the 7-step onboarding funnel (timestamps + boolean
  flags + `serve_onboarding_steps_completed`), `is_active_eo`, `has_pledged`,
  `is_active_serve_user`, registration cohort fields. Includes ALL users (not just Serve) so
  funnels have correct denominators — always filter `is_serve_user` for the canonical
  population.
- **`dbt.int__serve_active_user`** — the single source of truth for the behavioral half of the
  People Served cohort (`has_sent_sms_poll AND has_pledged`).
- **`mart_analytics.people_served`** — the North Star mart: census population covered by the
  cohort's districts. Headline read = cohort `active`, count-once, office_type `all`
  (~13.5M as of 2026-07).
- **`mart_analytics.users_serve_activity`** (wraps `dbt.int__amplitude_serve_activity`) —
  user × month poll-dashboard views. **Poll-anchored, continuity only** — see the collapse
  caveat in [methodology_defaults.md](methodology_defaults.md) before using it.
- **`dbt.stg_airbyte_source__amplitude_api_events`** — raw event grain; the source for the
  broad-engagement working set built by `analytics/lib/serve_analysis.py`.

## The Serve event landscape (two generations)

Serve's Amplitude events come in two generations, and the warehouse taxonomy only knows the
first (verified 2026-07-14):

| Generation | Event families | Taxonomy classification |
|---|---|---|
| 2025 original | `Serve Onboarding -%`, `Poll -%` / `Polls -%` / `Polls:%`, `Payment -%` | `family = 'serve'` in `dbt.int__amplitude_event_catalog` |
| 2026-05/06 new | `Briefing Assistant -%` (instrumented 2026-05-28), `Community Issues -%` (2026-06-19), `Org Switcher -%` (2026-06-15), plus a new wave of `Serve Onboarding` steps (2026-06) | **falls through to `family = 'other'`** — the `amplitude_event_family` macro predates them |

Two consequences:

1. Any Serve event work that relies on `family = 'serve'` **misses the new generation** —
   use the event-prefix list in `analytics/lib/serve_analysis.py`, which covers both.
2. Several new-generation events are **server-emitted, not user actions** (`Agenda Created`,
   `Agenda Not Created`, `Dispatch Skipped`, `Initial Issues Generated`, `High Priority
   Trending Issue Created` — 100% `session_id = -1`, verified 2026-07-14). Counting them as
   engagement counts the product's own dispatches. See [gotchas.md](gotchas.md).

Extending `amplitude_event_family` with the new Serve prefixes (and an `is_serve` flag) is an
open dbt follow-up — until it lands, the lib's prefix list is the working classification.

## Event-lifecycle assets (omni repo)

For when an event was added or retired in code, its current lifecycle status, or what
superseded it, use the **omni event-lifecycle assets** (provenance CSV, event-health log,
gp-meta metadata). They are cross-product and live with the process skill:
[event-lifecycle-assets.md](../../analytics-process/references/event-lifecycle-assets.md)
owns the full description — what each asset answers, freshness contracts, and the
stay-in-omni design constraint. They are the tool that dated the two Serve event
generations above.

## Cross-references

- [methodology_defaults.md](methodology_defaults.md) — the broad-engagement definition, default cohorts, working-set builder.
- [segmentation.md](segmentation.md) — what you can and cannot slice by today.
- [gotchas.md](gotchas.md) — the symptom table.
