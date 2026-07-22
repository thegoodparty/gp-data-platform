# Known gotchas reference

Part of the **serve-analytics-knowledge** skill. Recurring traps, as a quick-reference symptom
table. Near-empty by design — this skill is a skeleton (2026-07-14) and the calibration loop
fills this table run by run.

## Quick reference

- **What this is:** a symptom → mitigation index. When a trap is fully explained by its home doc, this table carries the **symptom** and points there rather than restating the fact — so a fact lives in exactly one place.
- **How to use it:** scan the Symptom column for what you're seeing, then follow the mitigation's link for the full explanation.
- **Maintenance:** when you hit a new trap, add a one-liner here; put the full explanation in the owning domain doc.

## Reading the Status column

Same vocabulary as the Win skill's gotchas: **`invariant`** = a structural truth that does not
drift; **`state · as-of YYYY-MM`** = a data- or code-state fact with a shelf life — re-verify
against the live catalog, the older the as-of date the more suspect.

## Gotchas

| Gotcha | Symptom | Mitigation | Status |
|---|---|---|---|
| **Server-emitted events masquerade as engagement** | `Briefing Assistant - Agenda Created` alone shows ~430 "users" in May–Jun 2026 while surface MAU is ~50; dispatch-style events (e.g. `Briefing Assistant - Agenda Created`; list in [sources.md](sources.md)) are 100% `session_id = -1`. | Count in-session events only (`session_id != -1`); the lib's predicate does this. Full event landscape in [sources.md](sources.md). | state · 2026-07 |
| **Staff impersonation leaks into EO engagement** | `/impersonate` and `/admin%` page views appear under EO `user_id`s (101 "Serve users" viewed `/impersonate` since 2026-04); by 2026-07, tainted sessions were ~1/3 of raw broad-engagement MAU (45 raw vs 30 clean). No user-property marker exists. | Exclude impersonation-tainted sessions — the taint rule (and why it is broader than `Viewed`-only) is part of the broad-engagement definition in [methodology_defaults.md](methodology_defaults.md); the lib applies it. Still a lower bound — sessions that never touch those paths under the EO's id escape the filter. | state · 2026-07 |
| **Poll-anchored activity measures a surface users left** | Serve MAU (`users_serve_activity`) reads ~15–27/mo from 2026-04 while broad engagement reads ~50–60; since 2026-05 `/dashboard/briefings` out-viewers `/dashboard/polls`. | Use broad engagement as the analysis default; keep poll-anchored series for continuity only, always with the collapse caveat in [methodology_defaults.md](methodology_defaults.md). | state · 2026-07 |
| **The April 2026 cliff is real, not a definition artifact** | Even broad-engagement MAU fell 294 → 61 from Mar to Apr 2026; "usage moved surfaces" does not explain it. | Do not headline a Serve activity trend across 2026-04 without the open-question caveat ([methodology_defaults.md](methodology_defaults.md)); Feb–Mar coincides with the post-election activation wave. | state · 2026-07 |
| **Serving vs campaigning attribution** | ~All Serve users are also Win candidates (figure and verification date in [sources.md](sources.md)); their event streams mix products, and shared surfaces fire Win-named events for EOs. | Time-scope to `event_time >= eo_activated_at` and use the surface-scoped broad definition; do not classify by event name on shared surfaces. | state · 2026-07 |
| **All-product pledge fields** | `users_serve_base.has_pledged`-style reads look huge (pledges span all products, 39k+ users). | Always pair with `is_serve_user`; the Serve-relevant pledge is pledge AND serve user. | state · 2026-07 |
