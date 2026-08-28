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

- **`mart_analytics.users_serve_base`** — user-grain view over civics `users` (grain enforced
  by a dbt `unique` + `not_null` test on `user_id`): Serve flags
  (`is_serve_user`, `eo_activated_at`), the 7-step onboarding funnel (timestamps + boolean
  flags + `serve_onboarding_steps_completed`), `is_active_eo`, `has_pledged`,
  `is_active_serve_user`, registration cohort fields. Includes ALL users (not just Serve) so
  funnels have correct denominators — always filter `is_serve_user` for the canonical
  population.
- **Activation anchor derivation:** `eo_activated_at` is computed in the civics `users` mart
  from product-DB tables (least of min completed-poll `created_at` via `elected_offices` and
  first serve-org `created_at`), not from any Amplitude event, so instrumentation changes
  cannot shift it. Recomputed retroactively each run; definition broadened 2026-03-31 (#291),
  so a future definition change restates all history. (state · 2026-07)
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

## Pledge instruments (two, asymmetric)

- **Campaign pledge** — `details:pledged` on gp_api campaigns, the governed `has_pledged`
  source (via `int__serve_active_user`). **Current-state only: no timestamp exists anywhere**
  (`pledgedAt`/`pledgeDate`/`pledged_at` all absent across 62,685 campaigns, verified
  2026-07-23), so point-in-time pledge reconstruction is impossible from it. Amplitude
  onboarding-pledge events (2025-05-28 →) can bound *recent* pledge dates — **when joining
  these events to a user table, cast the product-side key: `cast(u.user_id as string) =
  e.user_id`; casting the Amplitude side to bigint throws `CAST_INVALID_INPUT` on legacy
  Firebase-style IDs (see [gotchas.md](gotchas.md) — Amplitude user_id casts)**; Segment pledge
  tables cover only 2026-02-23 → 2026-03-24.
- **EO pledge** — `gp_api_db_elected_office.pledged_at`, a timestamped EO-level pledge from
  the new Serve onboarding (live 2026-06-23; 17 rows as of 2026-07-23). **Not read by
  `int__serve_active_user`** — a pledge-share analysis needs the union explicitly (state ·
  2026-07: the union added no users beyond campaign pledge on the June-30 population).

## The Serve event landscape (two generations)

Serve's Amplitude events come in two generations. As of DATA-2119 the taxonomy classifies both
as `family = 'serve'` (verified 2026-07-15):

| Generation | Event families | Taxonomy classification |
|---|---|---|
| 2025 original | `Serve Onboarding -%`, `Poll -%` / `Polls -%` / `Polls:%`, `Payment -%` | `family = 'serve'` in `dbt.int__amplitude_event_catalog` |
| 2026-05/06 new | `Briefing Assistant -%` (2026-05-28), `Org Switcher -%` (2026-06-15), `Community Issues -%` (2026-06-19, not yet flowing to the warehouse) | `family = 'serve'` (added in DATA-2119; `Community Issues -%` classifies automatically once its events land) |

**Not Serve:** the 2026-06 `Onboarding V2 -%` events read like a "new Serve onboarding wave" but are
the **Win** onboarding redesign (steps: Ballot Status, Office, Party Designation, Votes Needed,
Voter Insights), classified `win_onboarding`. Do not count them as Serve.

Two things to know:

1. `family = 'serve'` now covers both generations, so `analytics/lib/serve_analysis.py` sources
   Serve membership from the catalog (`family = 'serve'`) with no event-name prefix list.
2. Several new-generation events are **server-emitted, not user actions** (`Briefing Assistant -
   Agenda Created`, `Agenda Not Created`, `Agenda Submitted` — 100% `session_id = -1`, verified
   2026-07-15). They are correctly `family = 'serve'`, but engagement must exclude them via the
   in-session condition (`session_id != -1`), not by family. See [gotchas.md](gotchas.md).

## Event-lifecycle assets (omni repo)

For when an event was added or retired in code, its current lifecycle status, or what
superseded it, use the **omni event-lifecycle assets** (provenance CSV, event-health log,
gp-meta metadata). They are cross-product and live with the process skill:
`event-lifecycle-assets.md` in the analytics-process skill (when installed)
owns the full description — what each asset answers, freshness contracts, and the
stay-in-omni design constraint. They are the tool that dated the two Serve event
generations above.

## Cross-references

- [methodology_defaults.md](methodology_defaults.md) — the broad-engagement definition, default cohorts, working-set builder.
- [segmentation.md](segmentation.md) — what you can and cannot slice by today.
- [gotchas.md](gotchas.md) — the symptom table.
