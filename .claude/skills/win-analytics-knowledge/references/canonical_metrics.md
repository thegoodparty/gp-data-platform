# Canonical metrics (governed)

> **⚠ HIGH PRIORITY — the semantic layer supersedes this table.** Where a concept exists in the
> dbt semantic layer (`dbt/project/models/**/sem_*.yml`), that model's `config.meta` is the source
> of truth, including its `ratified:` date, and **overrides this projected table, which can lag the
> yml**. Check `sem_*.yml` as a first-class step *before* trusting a `Ratified` value here. This
> projection is regenerated from the yml and can be stale between regenerations (observed 2026-08 on
> the Serve catalog).

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

The table below holds the concepts **not yet encoded** in the semantic layer. Once a concept is
encoded, its row is deleted here and it renders in the generated region at the bottom of this
file instead (row-by-row takeover). Read both tables; nothing lives in both.

| Concept | Governed definition (one line) | Source (`table.column` / event) | Owns detail | Ratified |
|---|---|---|---|---|
| **Onboarded (canonical cohort)** | Viewed the candidate dashboard within 14 days of account creation; recomputed from the 2-event dashboard-view union (`Dashboard - Candidate Dashboard Viewed` ∪ `Dashboard - Campaign Plan Viewed` — the legacy event died in-data 2026-06-13; era-resolved across the onboarding rebuilds and the 2026-05/06 dashboard-surface migration) | recomputed from the dashboard-view union vs `users_win_candidacy.user_created_at` | [engagement.md](engagement.md) | pending |
| **Onboarding completed (pledge)** | Fired any era-resolved pledge-completion event (`Onboarding - Candidate Pledge Completed` / `Onboarding - Pledge Completed` / `Onboarding V2 - Pledge Completed`) within 14 days of account creation (strict funnel completion) | recomputed from the era-resolved pledge union | [engagement.md](engagement.md) | pending |
| **Outreach intensity** | Count of `Voter Outreach - Campaign Completed` events | `users_win_base.total_campaigns_sent` | [engagement.md](engagement.md) | pending |
| **Amplitude coverage** | Has ≥1 milestone event (lower bound; materially undercounts true coverage) | `users_win_base.has_amplitude_data` | [engagement.md](engagement.md) | pending |
| **Win/loss outcome (preferred)** | Deepest stage reached + result there: `latest_stage_reached` + `latest_stage_result` | `mart_civics.candidacy.latest_stage_result` | [outcomes.md](outcomes.md) | pending |
| **Viability Score 2.0** | `round(5 × P(win))`, range 0.0–5.0, mapped to a 5-band label (`No Chance` … `Frontrunner`) | `int__techspeed_viability_scoring.viability_rating_2_0` / `score_viability_automated` | [viability.md](viability.md) | pending |
| **PMF (KR2)** | Share of ICP-activated users answering "very disappointed" (Option 1) on the Win PMF survey if they could no longer use Win; target 40% | `stg_airbyte_source__hubspot_api_feedback_submissions` (`survey_name LIKE 'Win PMF%'`, `pmf_response`) | [outcomes.md](outcomes.md) | pending |
| **Upcoming/live election base** | Distinct `is_latest_version AND NOT is_demo` users whose per-user `MAX(election_date)`, bounded to `[2020-01-01, 2050-01-01]`, is `>= the as-of date` (open-ended; includes future cycles) | `users_win_candidacy.election_date` | [segmentation.md](segmentation.md) | pending |
| **Corroborated candidate (retrospective)** | Candidacy with a BallotReady, TechSpeed, or DDHQ candidacy-stage record AND (general election already happened OR general filing deadline passed); reliable only on completed elections, and "not corroborated" is an upper bound on "not real" (BR under-covers local races) | `candidacy_stage.br_candidacy_id` / `ts_source_candidate_id` / `ddhq_candidate_id` rolled to `gp_candidacy_id`; time-gate uses `candidacy.general_election_date` bounded to `[2020-01-01, 2050-01-01]` (corrupt out-of-range values present — see gotchas.md) (all eras) and `election_stage.filing_period_end_on` via `candidacy_stage.gp_election_stage_id` (2026+ only; NULL for <=2025 — use election-date gate only for archive) | [sources.md](sources.md) | pending |
| **Win W+1 Retention (OKR O2 KR1)** | Share of Win-ICP users returning to the dashboard within ~a week or later (**rolling**) of their first voter outreach send; conditions on activated ICP users by construction; target 60% (rolling ≈ 64.5%; the saved chart is N-day ≈ 12.6% — ~5x method gap, see [engagement.md](engagement.md)) | Amplitude chart `owc6mfnp` (start `ce:Voter Outreach - All`, return `Viewed` path=/dashboard, segment Win ICP) | [engagement.md](engagement.md) | pending |

When a question names a concept not in this table, fall through to the per-domain routing
table in the knowledge skill's `SKILL.md`. When you define a genuinely new canonical metric,
add a row here (and have the owner ratify it) rather than letting the definition live only in
a domain doc.

<!-- semantic-catalog:begin -->
| Concept | Governed definition (one line) | Source | Owns detail | Ratified |
|---|---|---|---|---|
| **GoodParty Cumulative Wins** | Running total of gp_api-sourced candidacy stages flagged as won for 2026 elections whose stage date has already passed. Accumulates all time, so each period shows the cumulative win count to date. | ref('candidacy_stage') | [outcomes.md](outcomes.md) | pending |
| **GoodParty Win Rate** | Count of gp_api-sourced candidacy stages flagged as won for 2026 elections whose stage date has already passed. | ref('candidacy_stage') | [outcomes.md](outcomes.md) | pending |
| **Win Activated Users** | Count of Win-product users who have activated: sent at least one voter outreach campaign (first_campaign_sent_at is not null). The activated slice of win_users; the Activated Candidates OKR. | ref('users_win_base') | [engagement.md](engagement.md) | 2026-07-28 |
| **Win Active Candidates 30d** | Count of Win-product users who viewed the candidate dashboard in the trailing 30 days: the Active Candidates OKR. Definition owned by users_win_base.is_active_candidate_30d, which is anchored on the 2-event dashboard-view union; the legacy single-event anchor died in-data 2026-06-13 and made the flag read FALSE for every user until the union repair landed. The window is evaluated against current_date() in the mart, so this metric is as-of-run-time and cannot be sliced historically by registered_at. | ref('users_win_base') | [engagement.md](engagement.md) | pending |
| **Win Users** | Count of Win-product users. Slice or filter by the engagement dimensions (has_viewed_dashboard, is_active_candidate_30d, etc.) at query time. | ref('users_win_base') | [engagement.md](engagement.md) | 2026-08-03 |
<!-- semantic-catalog:end -->
