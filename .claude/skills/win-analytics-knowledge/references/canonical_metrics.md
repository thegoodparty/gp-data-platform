# Canonical metrics (governed)

The single governed answer for each Win-product concept. **Resolve a concept here first**;
follow the "owns detail" link for the full definition's caveats, coverage, and query patterns.

**Governance.** These definitions are human-owned. Claude may draft the surrounding prose,
column descriptions, and gotchas in the linked docs, but must **not invent or silently change
a definition in this table**. The **Ratified** column records per-row sign-off: a date plus the
owner's initials once the metric owner has confirmed the definition. Treat a `pending` row as
lifted from the prior runbook and not yet settled — confirm with the metric owner before
building a headline on it. Ratifying a row is a human edit, never Claude's.

Keep this file thin: one row per genuinely-canonical concept, a one-line definition, the
owning `table.column` (or event), and the doc that owns the detail. Caveats and usage notes
live in the owning doc, not here, so this stays small enough to load on every resolution.

| Concept | Governed definition (one line) | Source (`table.column` / event) | Owns detail | Ratified |
|---|---|---|---|---|
| **Active Candidates (OKR)** | Viewed the candidate dashboard in the trailing 30 days | `users_win_base.is_active_candidate_30d` | [engagement.md](engagement.md) | pending |
| **Activated Candidates (OKR)** | Has sent ≥1 voter outreach campaign | `users_win_base.is_activated` | [engagement.md](engagement.md) | pending |
| **Onboarded (canonical cohort)** | Viewed the candidate dashboard within 14 days of account creation; recomputed from the 2-event dashboard-view union (`Dashboard - Candidate Dashboard Viewed` ∪ `Dashboard - Campaign Plan Viewed` — the legacy event died in-data 2026-06-13; era-resolved across the onboarding rebuilds and the 2026-05/06 dashboard-surface migration) | recomputed from the dashboard-view union vs `users_win_candidacy.user_created_at` | [engagement.md](engagement.md) | pending |
| **Onboarding completed (pledge)** | Fired any era-resolved pledge-completion event (`Onboarding - Candidate Pledge Completed` / `Onboarding - Pledge Completed` / `Onboarding V2 - Pledge Completed`) within 14 days of account creation (strict funnel completion) | recomputed from the era-resolved pledge union | [engagement.md](engagement.md) | pending |
| **Outreach intensity** | Count of `Voter Outreach - Campaign Completed` events | `users_win_base.total_campaigns_sent` | [engagement.md](engagement.md) | pending |
| **Amplitude coverage** | Has ≥1 milestone event (lower bound; materially undercounts true coverage) | `users_win_base.has_amplitude_data` | [engagement.md](engagement.md) | pending |
| **Win/loss outcome (preferred)** | Deepest stage reached + result there: `latest_stage_reached` + `latest_stage_result` | `mart_civics.candidacy.latest_stage_result` | [outcomes.md](outcomes.md) | pending |
| **Viability Score 2.0** | `round(5 × P(win))`, range 0.0–5.0, mapped to a 5-band label (`No Chance` … `Frontrunner`) | `int__techspeed_viability_scoring.viability_rating_2_0` / `score_viability_automated` | [viability.md](viability.md) | pending |
| **PMF (KR2)** | Share of ICP-activated users answering "very disappointed" (Option 1) on the Win PMF survey if they could no longer use Win; target 40% | `stg_airbyte_source__hubspot_api_feedback_submissions` (`survey_name LIKE 'Win PMF%'`, `pmf_response`) | [outcomes.md](outcomes.md) | pending |
| **Upcoming/live election base** | Distinct `is_latest_version AND NOT is_demo` users whose per-user `MAX(election_date)`, bounded to `[2020-01-01, 2050-01-01]`, is `>= the as-of date` (open-ended; includes future cycles) | `users_win_candidacy.election_date` | [segmentation.md](segmentation.md) | pending |

When a question names a concept not in this table, fall through to the per-domain routing
table in the knowledge skill's `SKILL.md`. When you define a genuinely new canonical metric,
add a row here (and have the owner ratify it) rather than letting the definition live only in
a domain doc.
