# Canonical metrics (governed)

The single governed answer for each Win-product concept. **Resolve a concept here first**;
follow the "owns detail" link for the full definition's caveats, coverage, and query patterns.

**Governance.** These definitions are human-owned. Claude may draft the surrounding prose,
column descriptions, and gotchas in the linked docs, but must **not invent or silently change
a definition in this table**. Rows below were lifted from the prior runbook and are **pending
human ratification** — confirm with the metric owner before treating a definition as settled.

Keep this file thin: one row per genuinely-canonical concept, a one-line definition, the
owning `table.column` (or event), and the doc that owns the detail. Caveats and usage notes
live in the owning doc, not here, so this stays small enough to load on every resolution.

| Concept | Governed definition (one line) | Source (`table.column` / event) | Owns detail |
|---|---|---|---|
| **Active Candidates (OKR)** | Viewed the candidate dashboard in the trailing 30 days | `users_win_base.is_active_candidate_30d` | [engagement.md](engagement.md) |
| **Activated Candidates (OKR)** | Has sent ≥1 voter outreach campaign | `users_win_base.is_activated` | [engagement.md](engagement.md) |
| **Onboarded (canonical cohort)** | Viewed `Dashboard - Candidate Dashboard Viewed` within 14 days of account creation; recomputed from the raw event (version-agnostic across the onboarding-flow cutover) | recomputed from `Dashboard - Candidate Dashboard Viewed` vs `users_win_candidacy.user_created_at` | [engagement.md](engagement.md) |
| **Onboarding completed (pledge)** | Fired `Onboarding - Candidate Pledge Completed` within 14 days of account creation (strict funnel completion) | recomputed from `Onboarding - Candidate Pledge Completed` | [engagement.md](engagement.md) |
| **Outreach intensity** | Count of `Voter Outreach - Campaign Completed` events | `users_win_base.total_campaigns_sent` | [engagement.md](engagement.md) |
| **Amplitude coverage** | Has ≥1 milestone event (lower bound; materially undercounts true coverage) | `users_win_base.has_amplitude_data` | [engagement.md](engagement.md) |
| **Win/loss outcome (preferred)** | Deepest stage reached + result there: `latest_stage_reached` + `latest_stage_result` | `mart_civics.candidacy.latest_stage_result` | [outcomes.md](outcomes.md) |
| **Viability Score 2.0** | `round(5 × P(win))`, range 0.0–5.0, mapped to a 5-band label (`No Chance` … `Frontrunner`) | `int__techspeed_viability_scoring.viability_rating_2_0` / `score_viability_automated` | [viability.md](viability.md) |
| **PMF (KR2)** | Share of ICP-activated users answering "very disappointed" (Option 1) on the Win PMF survey if they could no longer use Win; target 40% | `stg_airbyte_source__hubspot_api_feedback_submissions` (`survey_name LIKE 'Win PMF%'`, `pmf_response`) | [outcomes.md](outcomes.md) |

When a question names a concept not in this table, fall through to the per-domain routing
table in the knowledge skill's `SKILL.md`. When you define a genuinely new canonical metric,
add a row here (and have the owner ratify it) rather than letting the definition live only in
a domain doc.
