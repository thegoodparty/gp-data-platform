# Canonical metrics (governed)

The single governed answer for each Serve-product concept. **Resolve a concept here first**;
follow the "owns detail" link for the full definition's caveats, coverage, and query patterns.

**Governance.** These definitions are human-owned. Claude may draft the surrounding prose,
column descriptions, and gotchas in the linked docs, but must **not invent or silently change
a definition in this table**. The **Ratified** column records per-row sign-off: a date plus the
owner's initials once the metric owner has confirmed the definition. Treat a `pending` row as
lifted from the DATA-2115 scoping decisions (2026-07-14) and not yet settled — confirm with the
metric owner before building a headline on it. Ratifying a row is a human edit, never Claude's.

Keep this file thin: one row per genuinely-canonical concept, a one-line definition, the
owning `table.column` (or event), and the doc that owns the detail. Caveats and usage notes
live in the owning doc, not here, so this stays small enough to load on every resolution.

| Concept | Governed definition (one line) | Source (`table.column` / event) | Owns detail | Ratified |
|---|---|---|---|---|
| **Serve user / EO activated (canonical population)** | `is_serve_user` flag; currently identical to `eo_activated_at IS NOT NULL` (a data-state fact that may diverge) | `mart_analytics.users_serve_base.is_serve_user` | [sources.md](sources.md) | pending |
| **Active serve user (behavioral)** | Sent ≥1 SMS poll AND has pledged; the behavioral half of the People Served cohort | `dbt.int__serve_active_user.is_active_serve_user` | [sources.md](sources.md) | pending |
| **People Served cohort** | Active serve user AND Serve-ICP office AND not internal | `dbt.int__serve_district_resolution.in_people_served_cohort` | [sources.md](sources.md) | pending |
| **People Served (North Star)** | Census population covered by the cohort's districts; headline = cohort `active`, count-once, office_type `all` | `mart_analytics.people_served` | [sources.md](sources.md) | pending |
| **Onboarding completed (Serve)** | Sent first SMS poll (`first_sms_poll_sent_at IS NOT NULL`) | `users_serve_base.has_completed_serve_onboarding` | [segmentation.md](segmentation.md) | pending |
| **Serve broad engagement (analysis default)** | In-session Serve-surface engagement by a Serve user after EO activation, under the standard hygiene filter: serve-family or new-generation Serve events, or a `Viewed` on a Serve-surface path | predicate in `analytics/lib/serve_analysis.py` | [methodology_defaults.md](methodology_defaults.md) | pending |
| **Serve MAU (poll-anchored, continuity only)** | Distinct users with `Viewed` on `/dashboard/polls` + `Serve Activated`, monthly, excl. @goodparty.org | `mart_analytics.users_serve_activity` | [methodology_defaults.md](methodology_defaults.md) | pending |
| **Active EO 30d (poll-anchored, continuity only)** | Poll-dashboard view in trailing 30 days (AD-08) | `users_serve_base.is_active_eo` | [methodology_defaults.md](methodology_defaults.md) | pending |

When a question names a concept not in this table, fall through to the per-domain routing
in [`../SKILL.md`](../SKILL.md), and treat any definition you assemble as provisional — name it
explicitly in the analysis and flag it as a candidate registry row in the calibration pass.

**Outcome note (from the DATA-2115 decisions):** People Served is the decision-level outcome;
**retention on broad engagement** is the per-user analysis outcome. Re-election / continuation
in office is parked until officeholder data lands in civics.
