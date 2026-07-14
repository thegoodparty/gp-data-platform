# Serve methodology defaults

Part of the **serve-analytics-knowledge** skill. The Serve-product defaults consumed by the
process skill's [methodology.md](../../analytics-process/references/methodology.md): that doc
owns the product-agnostic discipline (scoping checklist, folder patterns, binning,
verification protocol); this one owns what is true *for Serve* — the resolved scoping
decisions, the default cohorts, and the working-set builder.

## Resolved scoping decisions (DATA-2115, 2026-07-14)

These apply by default to Serve-product analyses. Document deviations in your project's `SESSION_NOTES.md`.

| Decision | Default |
|---|---|
| Unit of analysis | User (`user_id`); Serve has no candidacy-version grain |
| Canonical population | `users_serve_base` filtered `is_serve_user` (1,869 as of 2026-07; currently 1:1 with `eo_activated_at IS NOT NULL`) |
| Engagement definition | **Broad Serve engagement** (below) is the analysis default; poll-anchored series (`users_serve_activity` MAU, `is_active_eo`) are continuity-only |
| Outcome | People Served (decision level); retention on broad engagement (per user); re-election parked until officeholder data lands |
| Engagement time-scope | Events at `event_time >= eo_activated_at` — pre-activation activity is campaigning (Win), not serving; ~all Serve users are also Win candidates |
| Hygiene filter | `is_serve_user` + exclude `@goodparty.org` + exclude impersonation-tainted sessions + in-session events only (`session_id != -1`) |
| Backfilled EOs | Registrations reach back to 2020 (pre-product EOs); whether they need an engagement floor is deferred to the first analysis that hits it |

## The broad Serve engagement definition (analysis default)

An event counts as broad Serve engagement when **all** of these hold:

1. The user is in the canonical population (`is_serve_user`).
2. `event_time >= eo_activated_at` (time-scope; see above).
3. The event is **in-session** (`session_id != -1`). Server-emitted dispatches
   (`Agenda Created`, `Dispatch Skipped`, …) are 100% out-of-session and are the product
   acting, not the user — see [gotchas.md](gotchas.md).
4. The event is on a Serve surface, meaning any of:
   - taxonomy `family = 'serve'` (the 2025 generation: Serve Onboarding, Polls, Payment);
   - event_type prefix `Briefing Assistant -` / `Community Issues -` / `Org Switcher -`
     (the 2026 generation, unclassified in the taxonomy — see
     [sources.md](sources.md) § The Serve event landscape);
   - a `Viewed` on a Serve-surface path: `/dashboard/polls%`, `/dashboard/briefings%`,
     `/dashboard/contacts%`, `/dashboard/chief-of-staff%`, `/dashboard/outreach%`,
     `/dashboard/community%`, `/serve%`, `/polls%`.
5. Standard hygiene: event-level email not `@goodparty.org`, and the event's
   `(user_id, session_id)` is not impersonation-tainted (the session contains a `Viewed` on
   `/impersonate` or `/admin%`).

The committed predicate is `serve_engagement_predicate()` /
`build_serve_working_set()` in `analytics/lib/serve_analysis.py` — use the lib, don't
re-derive the filter by hand. Shared-surface interaction events fired by EOs under Win names
(`Briefings -`, `Contacts -`, `Dashboard -` …) are deliberately excluded — event names can't
separate serving from campaigning on shared surfaces, so the definition trades some recall for
attribution safety. Broadening to "any post-activation product event" is the standard
sensitivity check.

**Reference series (clean broad-engagement MAU, verified 2026-07-14):** 2026-01: 208,
02: 346, 03: 294, 04: 61, 05: 48, 06: 60, 07 (through 07-14): 30.

**The collapse caveat (required on any Serve activity headline).** Serve monthly activity fell
~5× in April 2026 **under the broad definition too** (294 → 61), so the collapse is not just
poll-anchoring: the earlier "measurement stayed on a surface users left" explanation covers the
poll-vs-briefings mix *within* the shrunken base, not the cliff itself. Feb–Mar levels coincide
with the post-election EO activation wave; whether the cliff is novelty wear-off, a lost
re-engagement driver, or something else is an **open question, not a settled fact** — any
trend headline must carry this. Poll-anchored series additionally undercount the remaining
base ~2–3× (May: 17 poll-anchored vs 48 broad).

## Default cohorts by analysis type

| Analysis type | Default cohort | Source |
|---|---|---|
| Engagement / retention | Serve users, post-activation, broad engagement | `build_serve_working_set` (the lib) |
| Onboarding funnel / dropoff | All users who started Serve onboarding | `users_serve_base.has_started_serve_onboarding` (funnel flags carry the steps) |
| People Served / OKR reporting | People Served cohort | `int__serve_district_resolution.in_people_served_cohort` via `people_served` |
| Continuity comparison vs old dashboards | Poll-anchored MAU / `is_active_eo` | `users_serve_activity`, `users_serve_base` — carry the collapse caveat |

Always name the cohort in the analysis title and headline. "Among EO-activated Serve users…"
not "Among Serve users…"

## The working-set builder

The committed package `analytics/lib/serve_analysis.py` holds the broad-engagement predicate
(`serve_engagement_predicate`), the Serve-surface path and event-prefix constants, and
`build_serve_working_set(run_query, cohorts, ...)`, which returns one consolidated per-user
`cohort × engagement` DataFrame (post-activation, hygiene-filtered, impersonation-excluded)
carrying the slicing columns from [segmentation.md](segmentation.md). Build that one working
set first, then slice every cut from it in pandas (the build-once-slice-many rule in the
process skill's methodology.md). `wilson` is shared from `win_analysis.py`.

## Reviewer doc pointers (used by the dispatch template)

When the reviewer dispatch template in the process skill's pipeline.md asks for the product's
reviewer doc pointers, Serve's are:

- `product-data-scientist`: this skill's [gotchas.md](gotchas.md) and this file (the
  broad-engagement definition and its exclusions), plus the process skill's methodology.md.
- `product-manager`: this skill's [segmentation.md](segmentation.md) and
  [canonical_metrics.md](canonical_metrics.md), plus the process skill's methodology.md.

## Source pointers

- `analytics/planning/2026-07-14-serve-scoping-prep.md` (git-excluded) — the DATA-2115
  scoping profile, forks, and decisions this file is seeded from.
- `dbt/project/models/marts/analytics/users_serve_base.sql` and
  `dbt/project/models/intermediate/civics/int__serve_active_user.sql` — the governed
  definitions, with single-source-of-truth comments.
