# Win product analytics runbook

Evergreen reference for data sources, joins, outcomes, and methodology used in Win-product analyses. Project-specific scout outputs (e.g. `analytics/win_outcomes_scout/INVENTORY.md`) feed insights up into this runbook as they crystallize into reusable knowledge.

A separate Serve runbook will live alongside this file at `analytics/runbook/serve.md` (forthcoming).

**Audience:** Data team, analytics contractors, and future AI assistants doing product analyses against this codebase.

**Maintenance:** This is a living document. When a project scout (or analysis) discovers something reusable — a new join path, a documented gotcha, a coverage anomaly that holds across cycles — add it here. Project scouts answer "what did I find for THIS analysis"; the runbook answers "what's true in general." Cite source (project scout, dbt model, or commit) when updating so the doc stays auditable.

---

## Table of contents

1. [Data sources](#1-data-sources) — where the data lives
2. [Join keys](#2-join-keys) — how tables connect
3. [Outcome variables](#3-outcome-variables) — measuring "did they win?"
3.5 [Canonical engagement metrics](#35-canonical-engagement-metrics) — team OKR metrics for engagement/activation
4. [Amplitude event landscape](#4-amplitude-event-landscape) — engagement + funnel analyses
5. [Viability Score 2.0](#5-viability-score-20) — race-difficulty stratification
6. [Segmentation dimensions](#6-segmentation-dimensions) — slicing the population
7. [Methodology](#7-methodology) — scoping + verifying an analysis
8. [Known gotchas](#8-known-gotchas) — recurring traps
9. [References](#9-references) — source files + tickets

---

## 1. Data sources

Win-product analyses draw on six overlapping data domains:

| Domain | Where it lives | Grain | Use when |
|---|---|---|---|
| **Product DB (gp_api)** | `goodparty_data_catalog.dbt_staging.stg_airbyte_source__gp_api_db_*` | varies (user, campaign, position, path_to_victory, outreach, ...) | You need raw product state (registration, Pro flag, etc.) or to inspect product-side tables (e.g., `path_to_victory`) for funnel reconstruction. |
| **Analytics mart** | `goodparty_data_catalog.mart_analytics.*` | mostly user×campaign | Keystone tables for product analyses. **`users_win_candidacy` is the primary working table** — joins product user → campaign → candidacy → outcomes → viability → segmentation in one denormalized row. |
| **Civics mart** | `goodparty_data_catalog.mart_civics.*` | candidate / candidacy / candidacy_stage / election / election_stage | Outcome variables, viability, opponent counts, vote shares. Authoritative for electoral results across BR/TS/DDHQ/HubSpot providers. |
| **Amplitude product events** | staging: `goodparty_data_catalog.dbt_staging.stg_airbyte_source__amplitude_api_events`<br>intermediates: `goodparty_data_catalog.dbt.int__amplitude_*`<br>mart passthrough: `goodparty_data_catalog.mart_analytics.amplitude_events` | event-grain (raw); user×month or user-grain (aggregates) | Engagement, funnel completion, time-to-action analyses. See §4 for the full landscape. |
| **HubSpot survey responses** | `goodparty_data_catalog.dbt_staging.stg_airbyte_source__hubspot_api_feedback_submissions` | one row per submission | Self-reported PMF (Sean Ellis "would you be very disappointed if...") and CSAT/stars. Surveys include "Win PMF - Web survey", "Win User satisfaction", and "Win - User research" (recruitment). Powers the third success signal alongside outcomes and engagement — see §3. |
| **L2 voter data** | `goodparty_data_catalog.dbt.int__l2_*` | district-grain (aggregations); voter-grain (uniform/Haystaq) | Electorate context (voter counts, demographics). **Voter-grain is PII-adjacent — restricted to lawful use cases. Default to district-grain.** Already surfaced into `users_win_candidacy` as `voter_count`, `l2_district_name`, `l2_district_type`. |

### Keystone working tables

For most Win analyses, start with one of these:

- **`mart_analytics.users_win_candidacy`** — Win users joined to their candidacy, with outcome, viability, and segmentation columns. Grain: one row per `campaign_version_id`. Filter to `is_latest_version AND NOT is_demo` for the canonical working set. Joined upstream via `campaign_id ↔ product_campaign_id` to the civics mart.
- **`mart_analytics.users_win_base`** — Win users with engagement aggregates from `int__amplitude_user_milestones`. Grain: one row per user. Use for user-level analyses (e.g., onboarding CVR, time-to-activation).
- **`mart_analytics.users_win_activity`** — Win user × month engagement, wrapping `int__amplitude_win_activity`. Use for time-series engagement.
- **`mart_civics.candidacy`** — All candidacies (not just Win-product users). Use when you need a broader candidate universe or fields not surfaced in `users_win_candidacy` (vote counts, per-stage match metadata).

### Civics mart structure (5-table model)

Per `dbt/project/CLAUDE.md`:

1. **`candidate`** — one row per unique person.
2. **`candidacy`** — one row per (candidate × position × election year). Use this as the primary outcomes table.
3. **`candidacy_stage`** — one row per candidacy stage (primary, general, runoffs). Carries vendor-specific IDs and per-stage results.
4. **`election`** — one row per full election cycle (all stages combined).
5. **`election_stage`** — one row per individual stage of an election.

The `candidacy` mart is itself built as a UNION ALL of two structurally different parts:
- **2025 archive** (HubSpot-only, from `int__civics_candidacy_2025`)
- **2026+ FOJ** (a four-way full outer join over BR / TS / DDHQ / gp_api providers, from `int__civics_candidacy_{ballotready,techspeed,ddhq,gp_api}`)

Field availability differs across these halves — see [§8 Known gotchas](#8-known-gotchas).

---

## 2. Join keys

### The ID landscape

| Concept | Field | Where it lives | Notes |
|---|---|---|---|
| **Win user (product)** | `user_id` (BIGINT) | `users.user_id`, `users_win_*.user_id`, `int__amplitude_*.user_id` | Cast from Amplitude string `user_id` at the staging→intermediate boundary; non-numeric or empty IDs excluded. |
| **Win campaign (product)** | `campaign_id` (string) | `campaigns.campaign_id`, `users_win_candidacy.campaign_id`, `candidacy.product_campaign_id` | A campaign can have multiple historical versions (`campaign_version_id`) if the user reused it across cycles. |
| **Campaign version** | `campaign_version_id` (string) | `users_win_candidacy.campaign_version_id` | Grain of `users_win_candidacy`. Pre-filter to `is_latest_version` for canonical rows. |
| **Candidacy (GP-internal)** | `gp_candidacy_id` (uuid) | `candidacy.gp_candidacy_id`, `candidacy_stage.gp_candidacy_id` | Hashed UUID. Stable across providers via ER crosswalk in `int__civics_er_canonical_ids`. |
| **Candidate (GP-internal)** | `gp_candidate_id` (uuid) | `candidate.gp_candidate_id`, `candidacy.gp_candidate_id` | A `gp_candidate_id` can have many `gp_candidacy_id` rows (one per cycle). |
| **Election (GP-internal)** | `gp_election_id` (uuid) | `election.gp_election_id`, `candidacy.gp_election_id` | The election year × position. |
| **Election stage** | `gp_election_stage_id` (uuid) | `election_stage.gp_election_stage_id`, `candidacy_stage.gp_election_stage_id` | One per stage. |
| **TechSpeed candidate code** | `techspeed_candidate_code` (string) | `int__civics_candidacy_techspeed.candidate_code`, `int__techspeed_viability_scoring.techspeed_candidate_code` | Hashed first/last/state/office/city. Use to join TS-side viability directly. |
| **HubSpot contact** | `hubspot_contact_id` (int) on candidate side; `hs_contact_id` (string, cast to BIGINT) on survey staging | `candidate.hubspot_contact_id`, `stg_airbyte_source__hubspot_api_feedback_submissions.hs_contact_id` | Same logical key, two column names by source. Use this to attach survey responses to candidates. |
| **HubSpot company** | `hubspot_company_id` (int) | `candidacy.hubspot_company_ids` (array), `users_win_candidacy.hubspot_id` | Used in 2025-archive viability join: `tbl_companies.company_id ↔ stg_model_predictions__viability_scores.id`. **Note `users_win_candidacy.hubspot_id` is the COMPANY id, not the contact id** — see §8 gotchas. |
| **BR position** | `br_position_database_id` (int) | `candidacy.br_position_database_id` | Join key to `int__icp_offices` (district-grain L2 context, ICP flags). |

### Canonical join recipes

**Win user → outcomes:**
```
users_win_candidacy.campaign_id = candidacy.product_campaign_id
```
Use `users_win_candidacy` if you only need outcomes + segmentation that's already denormalized. Join out to `candidacy` for `latest_stage_reached`, `latest_stage_result`, `is_incumbent`, `is_open_seat`, `is_partisan`.

**Win user → engagement (Amplitude weekly):**
```
users_win_candidacy.user_id = int__amplitude_win_activity.user_id  (both BIGINT)
-- A weekly variant (int__amplitude_win_activity_weekly) is in development; framers must verify availability per the framer agent's "Pre-brief verification checklist" before relying on it.
```

**Win user → lifecycle milestones:**
```
users_win_candidacy.user_id = int__amplitude_user_milestones.user_id
```

**Candidacy → per-stage results:**
```
candidacy.gp_candidacy_id = candidacy_stage.gp_candidacy_id  (one-to-many)
```

**Candidacy → viability (TS-side, forward-stable):**
```
candidacy.candidate_code = int__techspeed_viability_scoring.techspeed_candidate_code
```
Use this rather than `candidacy.viability_score` if you need a path that survives a future mart rebuild (see [§5 Viability](#5-viability-score-20) and [§8 Gotchas](#8-known-gotchas)).

**Candidacy → ICP / L2 district context:**
```
candidacy.br_position_database_id = int__icp_offices.br_database_position_id
```

**Win user → HubSpot survey response (PMF / CSAT), 2-hop:**
```
CAST(stg_airbyte_source__hubspot_api_feedback_submissions.hs_contact_id AS BIGINT)
    = mart_civics.candidate.hubspot_contact_id
candidate.prod_db_user_id = users_win_candidacy.user_id
```
Then filter `users_win_candidacy` to `is_latest_version AND NOT is_demo`, and dedupe at `submission_id` grain (one user may have multiple candidacies → fan-out). For ICP attribution at submission grain, use `MAX(CASE WHEN icp_office_win THEN 1 ELSE 0 END)`. **Do NOT join `submissions.hs_contact_id` directly to `users_win_candidacy.hubspot_id`** — that returns zero matches because `hubspot_id` on the mart is the company id, not the contact id. See §8 gotchas.

### Version-aware join: campaign × candidacy

`users_win_candidacy.sql:113-118` joins via `campaign_id AND election_date = general_election_date` (with `election_date IS NULL` fallback). This means a campaign-version that predates BR coverage will surface as a row with NULL candidacy fields rather than a dropped row.

### Multi-cycle wrinkle

- One `user_id` can produce multiple `campaign_version_id` rows across cycles.
- One `gp_candidate_id` can have many `gp_candidacy_id` rows (one per cycle).
- The product DB overwrites campaign rows in place; Airbyte's insert-only stream preserves history; `campaigns.sql` rebuilds historical versions keyed by `campaign_version_id`.

For user-level analyses, collapse with explicit handling of "which candidacy represents the user". For cycle-level analyses, restrict on `general_election_date` and use `is_latest_version`.

---

## 3. Outcome variables

### The outcome columns in order of preference

| Column | Grain | When to use | Caveat |
|---|---|---|---|
| **`candidacy.latest_stage_result`** + `latest_stage_reached` | candidacy | **Preferred.** Captures the deepest stage reached + result there. Candidates eliminated at primary still get a labeled outcome. | Resolved scope for analyses going forward (DATA-1935, 2026-05-27). |
| `candidacy.general_election_result` | candidacy | Strict general-stage outcome only. Use when the analytical question is "did they win the general?" specifically. | Excludes candidates who never reached the general. ~28% fewer labels than `latest_stage_result`. |
| `candidacy_stage.election_result` | stage | Per-stage analyses. Use when comparing primary vs general dynamics. | Funnel-aware. `accepted_values` test enforces a constrained set. |
| `candidacy_stage.is_winner` | stage | Boolean form of the above. DDHQ-authoritative when present. | Boolean. Same domain as `election_result`. |
| `candidacy_stage.votes_received` / `vote_percentage` | stage | Vote-share analyses. | `votes_received` is STRING — may contain literal "uncontested". Cast/clean before aggregating. |
| `candidacy.candidacy_result` | candidacy | **AVOID** for binary win/loss. Rolled-up with cross-stage fallback (general_runoff > general > primary_runoff > primary). | Contamination hazard: a primary winner who lost the general can show `candidacy_result = 'Won'`. Documented at `m_civics.yaml:516-545`. |

### Domain values (per `m_civics.yaml:543-602`)

`Won`, `Lost`, `Runoff`, `Withdrew`, `Not on Ballot`, `Cannot Determine`

The `accepted_values` test on per-stage `election_result` excludes `Cannot Determine`. For binary outcomes, filter to `latest_stage_result IN ('Won', 'Lost')`.

### Amplitude self-reported outcome supplement

`Candidacy - Did You Win Modal Completed` carries a self-reported outcome in `event_properties`:

```json
{"impersonation": <bool | null>, "status": "won" | "lost"}
```

- `status` is the candidate's self-reported outcome.
- `impersonation` flags staff-impersonation. Added 2026-03; events before then carry NULL (treat as not-impersonated).

Filter: `COALESCE(impersonation, false) = false` to drop staff-triggered events while keeping the legacy NULL cohort.

**Net new labels vs civics mart**: roughly +970 users have a self-reported outcome that `general_election_result` does NOT carry (most are primary-stage losses or 2025 candidacies the mart hasn't fully ingested). Layer this BENEATH civics-mart outcomes (mart authority on ties; Amplitude fills NULL).

**⚠ Response selection bias is severe.** Self-reported win rate is ~80% vs ~64% raw win rate in the broader cohort — winners are more motivated to respond. Use the labels but NOT the rate as a population estimate.

See `analytics/win_outcomes_scout/INVENTORY.md` Source 3.5 for the verified reconciliation table.

### Self-reported success signal (PMF / satisfaction)

A parallel "did the product work?" measure that lives next to electoral outcomes rather than inside them. Currently feeding **KR2: 40% of ICP activated users say they would be very disappointed if they could no longer use Win**. Reading as of 2026-05-28: 52% Option 1 (Very disappointed) on n=50 ICP respondents — exceeds target, with small-sample caveat.

**Source table:** `goodparty_data_catalog.dbt_staging.stg_airbyte_source__hubspot_api_feedback_submissions`.

Filter by `survey_name`:
- `LIKE 'Win PMF%'` — Sean Ellis 4-option survey (started 2026-04-14; n=78 as of 2026-05-28).
- `LIKE 'Win User satisfaction%'` — CSAT/stars survey (n=12, sparse).

**Response columns:**
- `pmf_response` — the 4-option answer. **Option 1 = Very disappointed, Option 2 = Somewhat disappointed**; "Not disappointed" and "N/A - I no longer use" are explicit. HubSpot drops the original labels for Option 1/2 in the export; mapping verified by inspecting `pmf_additional_feedback` (Option 1 respondents express strong attachment). See §8 gotchas.
- `pmf_additional_feedback` — free-text follow-up.
- `satisfaction_stars` (1–5 int) and `satisfaction_rating` (string) on the CSAT survey.

**Joining to Win users:** 2-hop via candidate. Recipe in §2 canonical join recipes.

**Selection bias:** Survey-response self-selected. Like the Did-You-Win modal, the cohort overrepresents engaged / satisfied users. Use the labels as a signal, but read rates as directional rather than population estimates until volume grows.

**Reading the KR (KR-aligned recipe):**

```sql
WITH pmf AS (
  SELECT submission_id, CAST(hs_contact_id AS BIGINT) AS hs_contact_id, pmf_response
  FROM goodparty_data_catalog.dbt_staging.stg_airbyte_source__hubspot_api_feedback_submissions
  WHERE survey_name LIKE 'Win PMF%' AND pmf_response IS NOT NULL
),
attributed AS (
  SELECT
    p.submission_id, p.pmf_response,
    MAX(CASE WHEN u.icp_office_win THEN 1 ELSE 0 END) AS any_icp_win
  FROM pmf p
  LEFT JOIN goodparty_data_catalog.mart_civics.candidate c
    ON p.hs_contact_id = c.hubspot_contact_id
  LEFT JOIN goodparty_data_catalog.mart_analytics.users_win_candidacy u
    ON c.prod_db_user_id = u.user_id AND u.is_latest_version AND NOT u.is_demo
  GROUP BY 1, 2
)
SELECT
  COUNT(*) AS icp_respondents,
  SUM(CASE WHEN pmf_response = 'Option 1' THEN 1 ELSE 0 END) AS very_disappointed,
  ROUND(100.0 * SUM(CASE WHEN pmf_response = 'Option 1' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_very_disappointed
FROM attributed WHERE any_icp_win = 1;
```

See `analytics/win_outcomes_scout/INVENTORY.md` Source 7 for the full 7-facet inventory and the cross-bucket response table.

---

## 3.5 Canonical engagement metrics

The team has documented engagement / activation metrics in `mart_analytics.users_win_base` (per `m_analytics.yaml:152-247`). Default to these before defining new ones.

| Metric | Source column | Definition | When to use |
|---|---|---|---|
| `has_amplitude_data` | bool | True if user has ≥1 milestone event (registration, dashboard view, campaign, onboarding complete, pro upgrade, or SMS poll). Note: misses the broader onboarding-progress event family (`Onboarding -%` steps). | Coverage indicator; lower bound on "user appears in Amplitude milestone tables." |
| `is_post_amplitude_registration` | bool | True if `user_created_at >= 2023-12-10` (the documented Amplitude tracking start). **Caveat:** Win-product event instrumentation actually started ~2025-05-28; this flag is too loose for engagement analyses. |
| `has_completed_onboarding_flow` | bool | True if `onboarding_completed_at IS NOT NULL` (event = `onboarding_complete`). | **Default cohort filter for engagement-outcome analyses** — see §7. |
| `is_onboarded` | bool | True if US user viewed candidate dashboard within 14 days of Amplitude registration. Stricter than `has_completed_onboarding_flow`. | Onboarding CVR analyses. |
| `is_active_candidate_7d` / `_30d` / `_90d` | bool | True if user viewed dashboard in trailing N days. Computed against `current_date`. `_30d` is the canonical Active Candidates OKR. | Active-candidate OKR reporting; recompute against an anchor date for retrospective analyses. |
| `is_activated` | bool | True if user has sent ≥1 voter outreach campaign. **Canonical Activated Candidates OKR.** | Deeper-funnel engagement analyses; do not conflate with the colloquial "activated." |
| `total_campaigns_sent` | int | Count of `Voter Outreach - Campaign Completed` events. | Intensity-of-outreach analyses. |

### Terminology

The team's **`is_activated`** is specifically *sent ≥1 outreach campaign* — narrow, OKR-aligned. When referring colloquially to "users who engaged with the product at all," prefer the term **"onboarded"** (`has_completed_onboarding_flow = TRUE`), which is broader (~21% of registered cohort) and is the default cohort filter for engagement-outcome analyses.

---

## 4. Amplitude event landscape

### The modeling layers

| Layer | Model | What it covers |
|---|---|---|
| Staging (raw) | `stg_airbyte_source__amplitude_api_events` | **Full universe.** ~300+ distinct `event_type` values in any reasonable window. |
| Staging (catalog) | `stg_airbyte_source__amplitude_api_events_list` | Amplitude's own event catalog (name, totals, hidden flags). |
| Intermediate (Win) | `int__amplitude_win_activity` (monthly grain). A weekly variant (`_weekly`) is in development — verify availability before use. | **Only 2 event types**: `Voter Outreach - Campaign Completed`, `Dashboard - Candidate Dashboard Viewed`. |
| Intermediate (lifecycle) | `int__amplitude_user_milestones` | ~12 lifecycle milestones (registration, dashboard, onboarding complete, campaign sent, pro upgrade, Serve onboarding). One row per user. |
| Intermediate (Serve) | `int__amplitude_serve_activity` | Filters `Viewed` event to `event_properties:path = '/dashboard/polls'` + `user_properties:Serve Activated = true`. |
| Mart passthrough | `mart_analytics.amplitude_events` | Thin alias of the staging events for Sigma BI consumption. No transformation. |

### Universe vs modeled — the headline finding

The dbt intermediates aggregate only **~4% of distinct event types** (12 of ~300). Most "bulk" event volume is auto-instrumented anonymous traffic (`[Amplitude] *`, `Scroll Depth`, `session_*`) with no `user_id`. After excluding anonymous noise, the modeled fraction of **candidate-attributed** events is ~21%.

**Implication:** rich product instrumentation exists across the Win product (Onboarding, Outreach, Content Builder, Pro Upgrade, Profile, AI Assistant, etc.) and is NOT aggregated. New funnel / engagement analyses can build their own aggregates over the raw stream — this is in-scope analytics work, not a new instrumentation ask.

### Event family taxonomy

Use this SQL CASE-WHEN pattern (originally from `analytics/win_outcomes_scout/notebooks/inventory_queries.ipynb` NB-9) to bucket raw `event_type` values into product feature areas:

| Family | Pattern (LIKE / IN) | Example events |
|---|---|---|
| `win_onboarding` | `'Onboarding -%'` or `'Onboarding:%'` or `IN ('onboarding_complete', 'Invalid Party', 'Sign Up Clicked')` | `Onboarding - Candidate Office Searched`, `Onboarding - Registration Completed` |
| `win_dashboard` | `'Dashboard -%'` | `Dashboard - Candidate Dashboard Viewed`, `Dashboard - Campaign Plan Viewed` |
| `win_voter_outreach` | `'Voter Outreach -%'` | `Voter Outreach - Campaign Completed` |
| `win_outreach_planning` | `'Outreach -%'` | `Outreach - View Accessed`, `Outreach - Click Create` |
| `win_outreach_scheduling` | `'Schedule Text Campaign%'` or `'schedule_campaign%'` | `Schedule Text Campaign: Exit`, `Schedule Text Campaign - Audience: ...` |
| `win_content_builder` | `'Content Builder%'` or `'ai_content_%'` or `'campaign_assistant%'` | `Content Builder: Generation Started`, `ai_content_generation_start` |
| `win_voter_data` | `'Voter Data -%'` or `'Voter Data:%'` or `'Download Voter%'` or `'Custom Voter%'` | `Voter Data: Click Detail View`, `Download Voter File attempt` |
| `win_candidate_profile` | `'Profile -%'` | `Profile - Running Against: Click Save` |
| `win_pro_upgrade` | `'Pro Upgrade -%'` or `'Pro Upgrade:%'` or `= 'pro_upgrade_complete'` | `Pro Upgrade - Modal: Modal Shown`, `pro_upgrade_complete` |
| `win_p2p_upgrade` | `'P2P Upgrade -%'` | `P2P Upgrade - Modal: Modal Shown` |
| `win_candidate_website` | `'Candidate Website%'` | `Candidate Website - Started`, `Candidate Website - Published` |
| `win_candidacy_self_report` | `'Candidacy -%'` | `Candidacy - Did You Win Modal Completed` (see §3 for outcomes use) |
| `win_compliance_or_planning` | `'Campaign Verify%'` or `'Campaign Plan%'` or `'10 DLC Compliance%'` | `Campaign Plan - Weekly Tasks Digest` |
| `win_ai_assistant` | `'AI Assistant%'` or `= 'question_complete'` | `AI Assistant: Ask a question` |
| `win_briefings` | `'Briefings -%'` | New since 2026-04. |
| `win_contacts` | `'Contacts -%'` | Contacts CRM features. |
| `win_resources` | `'Resources -%'` | Resource library clicks. |
| `serve` | `'Serve Onboarding%'` or `'Poll - %'` or `'Polls -%'` or `'Polls:%'` | Serve product (covered in the Serve runbook). |
| `auth_or_settings` | `'Sign In:%'`, `'Sign Up:%'`, `'Set Password:%'`, `'Account -%'`, `'Settings -%'` | Cross-product. |
| `navigation` | `'Navigation -%'` or `'Navigation Top -%'` | Cross-product. |
| `viewed_generic` | `= 'Viewed'` | Generic event partially used by `int__amplitude_serve_activity` via property filter. |
| `amplitude_autotrack` | `'[Amplitude]%'` | Anonymous. Skip for candidate-attributed analyses. |
| `experiment_assignment` | `'[Experiment]%'` or `= 'Experiment Viewed'` | A/B test exposure. Usable as a stratification covariate. |
| `session_or_browser` | `IN ('Scroll Depth', 'session_start', 'session_end', 'page_view', 'Page Viewed', 'Page', 'usersnap_submission')` or `'Segment Consent%'` | Mostly anonymous. Skip. |

### Instrumentation start date

Most product-event families have first-seen dates clustered around **2025-05-28** (a major instrumentation push). Anything pre-2025-05-28 was tracked sparsely. Pre-2023-12-10 candidates have NO Amplitude history at all.

Newer families:
- `Candidacy - Did You Win Modal` (since 2025-10-31)
- `Dashboard - Campaign Plan Viewed` (since 2026-04-09)
- `Briefings - *` (since 2026-04-10)

### Building new funnel aggregates

The pattern: read raw events from `stg_airbyte_source__amplitude_api_events`, filter to the relevant family + window, aggregate to `user_id × time bucket` or `user_id × funnel step`. Mirror the structure of `int__amplitude_user_milestones` (user-grain milestones) or `int__amplitude_win_activity` (per-month aggregates).

Reusable Python pull-script template at `analytics/win_outcomes_scout/notebooks/_pull_amplitude_universe.py` (uses `databricks-sql-connector` via the global env vars — see user-level CLAUDE.md).

---

## 5. Viability Score 2.0

### What it is

**Viability Score 2.0 is the politics-team electoral-viability-for-winning score** — produced internally by `int__techspeed_viability_scoring.py` (`dbt/project/models/intermediate/techspeed_to_hubspot/`) using an MLflow-registered sklearn classifier:

```
goodparty_data_catalog.model_predictions.ViabilityWithOpponentData
```

NOT a BR-sourced field. NOT a lead-routing scorer. The model wraps `predict_proba` and surfaces two columns:

- **`viability_rating_2_0`** — `round(5 * probability_of_winning, 2)`. Range 0.0 to 5.0.
- **`score_viability_automated`** — 5-band categorical label:

| Band | Threshold |
|---|---|
| `No Chance` | `< 1.0` |
| `Unlikely to Win` | `1.0 – < 2.0` |
| `Has a Chance` | `2.0 – < 3.0` |
| `Likely to Win` | `3.0 – < 4.0` |
| `Frontrunner` | `≥ 4.0` |

### Features (model inputs)

- `is_incumbent` (bool)
- `open_seat` (bool)
- `multi_seat` (0/1)
- `partisan_contest` (0/1)
- `is_unexpired` (always False in this pipeline)
- `log_n_losers` — `log(n_candidates - n_seats)` floored at `log(0.001)` for uncontested
- `state_woe`, `level_woe`, `office_type_woe` — Weight-of-Evidence-encoded categoricals (encodings in `goodparty_data_catalog.model_predictions.viability_br_{state,level,office_type}_woe`)

**A row gets a score only if EVERY feature is populated.** Missing one feature → NULL rating. This is why BR-only candidacies are spotty — BR doesn't supply `n_seats`, `is_incumbent`, or `n_candidates` for all rows.

### Where the score lives (3 distinct tables)

| Table | Key | Coverage | Use |
|---|---|---|---|
| `goodparty_data_catalog.dbt.int__techspeed_viability_scoring` | `techspeed_candidate_code` | ~99.7% rated | Model output. **Forward-stable join path.** |
| `goodparty_data_catalog.dbt.stg_model_predictions__viability_scores` | HubSpot company `id` | 100% rated | Separate scoring run (HubSpot-keyed features). Used by `int__civics_candidacy_2025` for the 2025 archive. |
| `goodparty_data_catalog.mart_civics.candidacy.viability_score` | `gp_candidacy_id` | Varies by source bucket (see below) | The mart-level field surfaced into `users_win_candidacy`. |

### Coverage in `mart_civics.candidacy` by source bucket

| source bucket | coverage % |
|---|---:|
| 2026+ TS-only (no BR match) | ~97.8% |
| 2026+ BR-matched (TS+BR) | ~56.3% |
| 2025 archive (HubSpot) | ~4.4% |
| 2026+ gp_api-only | 0% |
| 2026+ ddhq-only | 0% |

For Win-filtered analyses (joining through `users_win_candidacy`), coverage drops further to ~13% — Win users come in via `gp_api` and only get a score if the ER crosswalk lands them on a TS or TS+BR candidacy.

### Score distribution is bimodal

86% of scored candidates land in `Frontrunner` or `No Chance`; only 14% in the three middle bands. Consistent with a model dominated by incumbency + opponent-count features. **Don't bucket into deciles for stratification — use the 5-band label or quintiles aligned to the bimodal shape.**

### ⚠ Code/data discrepancy (flag for the data platform team)

The current SQL files for `int__civics_candidacy_ballotready.sql` and `int__civics_candidacy_techspeed.sql` (both in main) hardcode `cast(null as float) as viability_score`. Yet the prod intermediate tables have scores populated. Two possible explanations:

1. The prod intermediate hasn't been rebuilt since the null hardcode landed (2026-02-09, commit `61120ab1`). A future rebuild would drop ~58k scores from the mart.
2. There's an unobserved code path (notebook, Airflow job, hand-merge) populating the score post-hoc.

**Forward-stable analysis pattern:** join `int__techspeed_viability_scoring` directly via `techspeed_candidate_code` instead of relying on `candidacy.viability_score`.

### Calibration leakage risk

`int__techspeed_viability_scoring` is materialized as a `table` and re-computes on each `dbt run`. No historical snapshot. If the MLflow model is retrained against post-election data, calibration on 2025 outcomes is potentially contaminated. Flag this on any calibration plot.

### Back-scoring NULL rows

Even where `viability_score` is NULL, the underlying features are partially available:
- `is_incumbent` (TS-sourced; sparse on BR-only)
- `is_open_seat` (BR > TS > DDHQ)
- `is_partisan`
- `n_seats` / `n_candidates`: reconstructible from `candidacy_stage` counts per `gp_election_stage_id`
- WoE encodings: stable lookup tables

In principle, the score can be back-scored for additional candidacies by joining feature rows against the existing MLflow model.

---

## 6. Segmentation dimensions

Available on `users_win_candidacy` directly (no join needed):

| Dimension | Coverage notes |
|---|---|
| `election_level` | ~55% non-NULL. Values: `city`, `county`, `state`, `federal`. **Large NULL bucket** — see [§8 Gotchas](#8-known-gotchas). |
| `office_type` | HubSpot-sourced string. Common values: School Board, City Council, Mayor, Judge, State House (`m_civics.yaml:507-514`). |
| `campaign_state` | ~99% populated. |
| `campaign_party` | ~95% populated. |
| `election_date`, `primary_election_date`, `general_election_date`, runoff dates | Per-stage dates. Preserve cycle separation — do NOT use `users_win_base.election_date` for outcome analyses (it's a `coalesce(next, last)` that leaks). |
| `is_pro` | Boolean. Campaign-grain Pro flag at mart-materialization time. Pro upgrade TIMING comes from Amplitude (`int__amplitude_user_milestones.pro_upgrade_completed_at`). |
| `is_verified`, `is_demo`, `is_pledged`, `is_latest_version` | Quality / state flags. Default to `is_latest_version AND NOT is_demo`. |
| `icp_office_win`, `icp_office_serve`, `icp_win_supersize` | ICP flags. **Use as slicing dimensions, NOT filters** (per DATA-1935 resolved scope). |
| `is_judicial`, `is_appointed` | Non-traditional office flags. Often correlated with NULL ICP / NULL `election_level`. |
| `l2_district_name`, `l2_district_type`, `voter_count` | L2 district context — already surfaced from `int__icp_offices`. Use these instead of joining L2 directly. |

Requires a join to `mart_civics.candidacy`:

- `is_incumbent` (TS-sourced; ~70% populated, sparse on BR-only)
- `is_open_seat` (BR > TS > DDHQ; NULL on BR-only)
- `is_partisan` (boolean)

### ICP as dimension, not filter

`icp_office_win` flags candidacies for offices Win supports. Originally proposed as a population filter; resolved scope (DATA-1935, 2026-05-27) says **slice, don't filter** because:

- ICP=true candidacies are competitive races by definition — filtering removes the comparison baseline.
- Reporting unfiltered first characterizes the broader Win population.
- ICP=true cohort tends to show LOWER raw win rate than ICP=false (~53% vs ~63%) — likely because Win supports harder races. This pattern only surfaces with a slice, not a filter.

---

## 7. Methodology

### Resolved scoping decisions (DATA-1935, 2026-05-27)

These apply by default to Win-product analyses. Document deviations in your project's `SESSION_NOTES.md`.

| Decision | Default |
|---|---|
| Unit of analysis | Candidacy (`gp_candidacy_id`) |
| Outcome variable | `latest_stage_reached + latest_stage_result` from `mart_civics.candidacy` (NOT `general_election_result`) |
| Cycle window | `general_election_date BETWEEN '2025-05-01' AND '2026-12-31'` (adjust per project) |
| Engagement window | Same as candidacy window unless analysis specifically wants trailing features |
| Baseline | All registered Win users; engagement-zero is a valid predictor value (not an exclusion) |
| ICP gating | `icp_office_win` is a slicing dimension, not a filter |

### Default cohorts by analysis type

Different analyses call for different cohorts. The full Win-product registered population includes registered-but-never-active candidates, pre-instrumentation registrants, CRM-sync candidates who never logged in, and other heterogeneous funnel stages. Pooling them produces misleading correlations — for example, raw engagement-vs-win-rate looks *inverse* (more engagement → lower win rate) because the 0-engagement bin is dominated by candidates who never used the product at all, not by candidates who tried-and-failed to engage.

| Analysis type | Default cohort | Source flag |
|---|---|---|
| Engagement vs outcome | Onboarded cohort | `has_completed_onboarding_flow = TRUE` (in `users_win_base`) |
| Outreach intensity vs outcome | Activated cohort (sent ≥1 outreach) | `is_activated = TRUE` |
| Funnel / dropoff analyses | Full registered population | `users_win_candidacy` filtered to `is_latest_version AND NOT is_demo` |
| Active candidates OKR reporting | Active candidates (trailing 30d) | `is_active_candidate_30d = TRUE` (or recompute anchored to a target date) |

Always name the cohort in the analysis title and headline so consumers know which population is being characterized. "Among onboarded Win candidates..." not "Among Win candidates..."

### Scoping checklist for a new analysis

Before writing any code:

1. **What's the question?** Phrase it as a one-sentence hypothesis. Identify whether the outcome is binary win/loss, time-to-event, funnel completion, or descriptive.
2. **What's the unit of analysis?** Candidacy / user / campaign-version / event. Mismatching grain is the most common methodological bug.
3. **What's the time window?** Apply at the right grain. Election-cycle vs engagement-feature windows may differ.
4. **What's the comparison baseline?** Cohort A vs cohort B requires both to be in scope.
5. **What confounders matter?** Office level, ICP, Pro, incumbency, opponent count. List explicitly.
6. **What does success look like?** A specific number / chart you want to produce, OR a specific question to answer.

If any of these is ambiguous, surface it to the analytics owner via `AskUserQuestion` (or equivalent) BEFORE writing code. Do not list as "open questions" in the deliverable — that wastes a review cycle.

### Project folder pattern (scout-project flavor)

For multi-week scout projects with reusable inventory, create the full structure:

```
analytics/<project_name>/
  CLAUDE.md          (optional, project-local AI guidance)
  INVENTORY.md       (committed; the scout findings for this project)
  SESSION_NOTES.md   (gitignored, local-only running journal)
  notebooks/
    <project>.ipynb  (the working analysis notebook)
```

The notebook should be set up with a parameterized cycle window at the top so re-running under a different scope is trivial. See `analytics/win_outcomes_scout/notebooks/inventory_queries.ipynb` for the verification-notebook pattern.

### Lightweight analysis pattern (ad-hoc flavor)

For single-notebook ad-hoc analyses (one question, one notebook, no reusable inventory), use the lighter shape:

```
analytics/analyses/
  <YYYY-MM-DD>_<slug>.ipynb
  <YYYY-MM-DD>_<slug>_brief.yaml
```

Date-prefixed filenames are required (chronological sortability). No INVENTORY.md / SESSION_NOTES.md scaffolding. The brief sits alongside the notebook so the framing is retrievable after the fact.

### Notebook sync workflow

Sync local notebooks to Databricks for execution:

```bash
./analytics/scripts/sync.sh <project_folder>     # watcher: local → Databricks
./analytics/scripts/pull.sh <project_folder> <notebook_name>   # reverse: Databricks → local
```

Both honor a `DATABRICKS_WORKSPACE_USER` env var. Default scratchpad: `/Workspace/Users/${USER}/scratchpad/`. Notebook filenames must be unique across all `analytics/*/notebooks/` since they land flat in scratchpad.

See `analytics/win_churn/WORKFLOW.md` for the full local-canonical / Workspace-execution rationale.

### Ad-hoc query pattern

For inspection / scoping queries, use `dbt show --inline` (from `dbt/project/`) directly against prod paths:

```bash
dbt show --inline "SELECT ... FROM goodparty_data_catalog.<schema>.<table> WHERE ..." --limit 50
```

Per project memory: hit `goodparty_data_catalog.*` directly. `ref()` can resolve to stale dev artifacts. Prod schemas: `dbt_staging` (staging), `dbt` (most intermediates), `mart_analytics` / `mart_civics` (marts), `model_predictions` (MLflow outputs).

For larger query results that exceed `dbt show` truncation, use the `databricks-sql-connector` Python client via the global env vars (see user-level CLAUDE.md). Pattern in `analytics/win_outcomes_scout/notebooks/_pull_amplitude_universe.py`.

### Binning conventions

When binning a continuous engagement or outcome metric:

- Prefer pre-registered bins in the brief, with anchors tied to interpretable thresholds (e.g., funnel-stage boundaries: 0 active weeks = didn't return, 1-3 = light user, etc.).
- If bins are chosen after viewing the distribution, document this explicitly in the notebook and report sensitivity to bin choice.
- Always report Wilson 95% CIs alongside point estimates so readers can distinguish real differences from sampling noise.
- Flag any bin with N<30 as small-sample.

### Verification protocol

A scout / analysis is "done" when:

1. Every numeric claim in the deliverable points to a notebook cell (or query) that reproduces it.
2. Cycle / scope parameters are parameterized so the work re-runs under a different scope.
3. Open scoping questions are RESOLVED (not deferred to the reader).
4. The runbook is updated with any reusable insights (joins, gotchas, source-system precedence rules) that emerged.

---

## 8. Known gotchas

These are recurring traps. When you hit one, add a one-liner here so the next person doesn't.

| Gotcha | Symptom | Mitigation |
|---|---|---|
| **`users_win_base.election_date = coalesce(next, last)`** | Leaks future election dates into "current cycle" analyses. | Use per-stage dates from `users_win_candidacy` instead. They are NOT coalesced and preserve cycle separation. |
| **`candidacy_result` cross-stage fallback** | A primary winner who lost the general can show `candidacy_result = 'Won'`. | Use `latest_stage_result` + `latest_stage_reached` for win/loss analyses. Or use `general_election_result` if specifically wanting general-stage outcomes. |
| **Viability score code/data discrepancy** | `candidacy.viability_score` is populated in prod but the SQL files hardcode it to NULL. Future rebuilds could drop ~58k scores. | Join `int__techspeed_viability_scoring` directly via `techspeed_candidate_code` for forward-stable access. |
| **Pre-2025-05-28 candidates have no Win-product Amplitude history** | Product-event instrumentation for the Win product went live ~2025-05-28; the modeled weekly/monthly tables start ~2025-06-01. Earlier `users.created_at` rows exist in the product DB but have no Win-Amplitude events. | For engagement-window floors, use the **actual coverage `MIN(week_start_date)`** of the source table (currently 2025-06-23 for the weekly model), not a fixed date. Document the coverage window in the brief's `data_provenance` field. |
| **45.6% NULL `election_level` in window** | Office-level slicing loses half the population if you filter. | Decide explicitly: (a) restrict, (b) backfill from `office_type` / `int__icp_offices`, or (c) treat NULL as a fifth category. |
| **Pro users have LOWER raw win rate than free** | Naive Pro-lift analysis flips intuition. | Confounded by selection bias (Pro users self-select into harder races) and reverse causation (engagement → Pro). Office-stratify; use pre/post-upgrade frame for timing. |
| **ICP=true cohort has LOWER raw win rate than ICP=false** | Same direction as the Pro finding. | ICP=true offices are by-design competitive. Slice rather than filter. |
| **2025 archive vs 2026+ FOJ structural mismatch** | Field availability differs (e.g., `is_open_seat` absent from 2025 archive). | Slice by `source_systems` and `year(general_election_date)` together. Don't pool blindly. |
| **Outcome data concentrated in 2025** | 2026 cycle has <10% outcome coverage. | "Did they win" analyses are effectively a 2025 cohort study for now. 2026 holdout is limited. |
| **`votes_received` is STRING** | Casting fails on rows containing "uncontested". | Filter or coalesce the literal before casting. |
| **Multi-cycle candidates** | One user can produce multiple `campaign_version_id` rows. | Filter to `is_latest_version` for canonical row. Collapse multiple cycles explicitly. |
| **Amplitude self-reported outcome response bias** | 80% self-report "won" vs 64% raw population win rate. | Use labels, NOT rates. Cohort is not random. |
| **L2 PII boundary** | Voter-grain L2 models carry PII-adjacent records. | Aggregate-grain queries only by default. `voter_count`, `l2_district_type` already surfaced in `users_win_candidacy`. |
| **Mart staleness vs SQL state** | Prod mart may carry data the current SQL doesn't produce (e.g., viability). | When code/data disagree, trust the data for current state but flag the discrepancy. Don't extrapolate forward. |
| **VIRTUAL_ENV breaks pre-commit pytest hook** | `Executable pytest not found` on commit when an analytics venv is active. | Use `env -u VIRTUAL_ENV poetry run git commit ...` from `dbt/`. |
| **`users_win_candidacy.hubspot_id` is the COMPANY id, not the contact id** | Joining `stg_airbyte_source__hubspot_api_feedback_submissions.hs_contact_id` directly to `users_win_candidacy.hubspot_id` returns zero matches. | 2-hop via candidate: `submissions.hs_contact_id` → `candidate.hubspot_contact_id` → `candidate.prod_db_user_id` → `users_win_candidacy.user_id`. Recipe in §2. |
| **HubSpot PMF Option 1/2 labels obscured in export** | `pmf_response` returns the literal strings `"Option 1"` and `"Option 2"` instead of "Very disappointed" / "Somewhat disappointed". | Treat **Option 1 = Very disappointed, Option 2 = Somewhat disappointed**. Verified by sampling `pmf_additional_feedback`: Option 1 respondents express strong attachment; Option 2 are measured. Worth a follow-up with data engineering to either fix the HubSpot export or document the mapping in a `m_hubspot.yaml`. |

---

## 9. References

### Calibration logs

After a substantive analysis run, write findings that should update the runbook or agents into a dated calibration log at `analytics/runbook/CALIBRATION_<YYYY-MM-DD>.md`. Hand-process the log into runbook/agent edits before treating it as resolved.

Calibration logs are personal working documents — they are gitignored (`analytics/runbook/CALIBRATION_*.md`) so each analyst's working tree can carry them without affecting the shared repo. The durable team-shared output is the runbook + agent edits the log drives.

### Project scouts that contributed insights

- **`analytics/win_outcomes_scout/INVENTORY.md`** (DATA-1935) — Win product × electoral outcomes data scout. Seeded most of the content here, especially §3-§5. Source 7 in the inventory carries the full HubSpot survey detail (per-survey submission counts, response distribution by ICP/Win bucket, the verified Option 1/2 mapping evidence).
- **`analytics/win_churn/`** (DATA-1924) — Win churn modeling. Engagement-as-outcome counterpart. Source for some of the multi-cycle and pre-2023-12-10 gotchas.

### Authoritative dbt model docs

- `dbt/project/models/marts/civics/m_civics.yaml` — column descriptions for candidacy / candidate / candidacy_stage / election / election_stage.
- `dbt/project/models/marts/analytics/m_analytics.yaml` — for the analytics mart.
- `dbt/project/models/intermediate/amplitude/int__amplitude.yaml` — Amplitude intermediate columns + tests.

### Conventions

- `CLAUDE.md` (repo root) — multi-venv reality + don't-disable-pre-commit rules.
- `dbt/project/CLAUDE.md` — dbt-development conventions (do not invoke dbt via poetry; use `dbt show --inline`; branch / commit naming; etc.).
- `ai-rules/` (submodule) — broader AI-assisted-development rules.

### External tickets / documentation

- ClickUp tickets are tagged `DATA-XXXX` and surfaced in commit messages + branch names. Search by ticket ID.
- For data platform issues that span multiple subprojects, prefer linking the ticket / PR over re-explaining the issue here.


### Analysis briefs from analytics-question-framer

When the `analytics-question-framer` agent finishes shaping a question, it produces a structured analysis brief. This brief is the handoff artifact to Claude Code (or whoever executes the analysis). The format below is the contract.

Every brief must use this template. Don't omit sections; if a section doesn't apply, write "N/A" with a one-line explanation.

```yaml
brief_id: short-kebab-case-identifier  # e.g., messaging-tool-win-rate-2026q2
created: YYYY-MM-DD
author: analytics-question-framer (refined with <username>)

decision:
  what_action: |
    What will be done differently based on the result.
  who_acts: |
    Which team or role takes that action.

question:
  one_sentence: |
    The sharp version of the question, stated in one sentence.
  type: causal | correlational | descriptive
  underlying_hypothesis: |
    What the user actually believes and wants to test.

population:
  included: |
    Precise definition of who's in scope.
  excluded: |
    Cohorts explicitly removed (demo accounts, internal users, out-of-scope geo, etc.).
  source_model: |
    The dbt model or table this population is drawn from.

data_provenance:
  schema_status: prod | dev | pending_merge
  coverage_start: |
    Actual MIN of the time column in the source table, verified by query
    (not the brief's assumed floor). E.g., MIN(week_start_date) from
    int__amplitude_win_activity_weekly.
  coverage_end: |
    Actual MAX of the time column.
  post_merge_swap: |
    If schema_status = dev or pending_merge: what to change after merge
    (e.g., "swap WIN_ACTIVITY_WEEKLY_SCHEMA from 'private_tristan' to 'dbt'").

eligibility:
  tenure_requirement: |
    Minimum observability window before the reference event.
  reference_event: |
    The anchor date (election date, signup date, feature launch, etc.).
  rationale: |
    Why this eligibility rule exists.

target:
  definition: |
    Precise outcome variable, including units.
  censoring: |
    How incomplete observations are handled.
  absorbing_states: |
    How terminal states (won, lost, dropped, deleted) are handled.

comparison:
  type: treatment_vs_control | correlation | descriptive_cut | other
  comparison_group: |
    What the treated group is being compared to.
  notes: |
    Any caveats about comparability (selection, confounding).

observation_window:
  start: |
    Date or rule for window start.
  end: |
    Date or rule for window end.
  anchored_to: |
    The reference event the window is relative to.

cohorts:
  - dimension: e.g., position_type
    values: |
      Which values to break out (or "all observed").
  - dimension: ...

sample_size:
  expected_n_total: |
    Order of magnitude.
  expected_n_per_cohort: |
    Rough breakdown.
  power_concern: |
    Note if any cohort is likely underpowered for the effect of interest.

falsification:
  what_would_change_belief: |
    The result that would make the user update.

known_concerns:
  - |
    Concerns raised by analytics-question-framer that the user chose to proceed past.
  - |
    Limitations of the data or design.

execution_notes:
  preferred_format: notebook  # default
  output_location: |
    Where the executed analysis should be saved.
  reruns: |
    Should this be a one-off or set up for repeated runs?
```

### Notes on using briefs

- Claude Code should treat the brief as a spec. If something in the brief is ambiguous or unworkable on inspection of the actual data, kick it back to `analytics-question-framer` rather than improvising.
- After execution, `product-data-scientist` reviews the notebook against the brief — both for methodological soundness and to interpret what the results mean.
- Briefs are durable: save them alongside the executed notebook so the framing is retrievable later.
