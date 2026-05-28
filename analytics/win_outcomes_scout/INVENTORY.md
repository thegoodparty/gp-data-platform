# Win product × electoral outcomes — data inventory

## TL;DR

**What this is.** A scouting pass for an upcoming exploratory analysis on whether Win product activity correlates with electoral outcomes. Win is GoodParty's product for independent candidates running for office. The scout characterizes what data is already available to answer that question. It does not build new joins, run the correlation analysis itself, or recommend a specific model.

**Scope.** Candidacies in the 2025-05 through 2026-12 cycle window. Unit of analysis is the candidacy (one row per cycle, so a candidate running in two cycles appears twice). Outcome is `latest_stage_reached` paired with `latest_stage_result`. Baseline is all registered Win users including those with zero product engagement. ICP eligibility (`icp_office_win`) is treated as a slicing dimension, not a filter.

**This artifact.** `INVENTORY.md` (this file) holds findings, gaps, and recommended actions. `notebooks/inventory_queries.ipynb` holds the queries that produce every coverage number cited here, parameterized so the cycle window and other filters can be adjusted to reproduce or extend.

**Findings.** Four data sources are in scope, all in Databricks: Win product activity (Amplitude events), candidacy and election outcomes (the civics mart), candidate and user metadata (the product database), and the segmentation dimensions that link them (office level, state, party, ICP flags). These join cleanly at user grain. The gaps are inside specific fields, not in the joins. The analytical work going forward is in cutting each question correctly, not in stitching the data together.

- **Available.** Win product, civics, and Amplitude data in Databricks all join cleanly at user grain, with 96%+ Amplitude join coverage in the 2025-05 to 2026-12 cycle window. Stable keys link product activity, candidacy, outcomes, and segmentation. Any question that fits inside this universe is reachable.
- **Gaps.** Coverage drops are inside columns, not between tables. Final outcome labels populate only a sub-segment of candidacies (the Did-You-Win modal adds ~970 net new labels but skews ~80% wins via response selection). Viability is sparse on the BR-only `viability_score` and only partially wired in via the better-covered TS-primary `viability_rating_2_0`. Segmentation dimensions like election level, judicial, and appointed thin out at narrow slices. Each modeled Amplitude event has its own instrumentation start date that further bounds who is eligible for any feature-effect analysis.
- **Implication.** Every correlation question is its own narrow cut, not a single global model. The shape is: pick a feature, pin its launch date and a meaningful exposure window, identify the candidacies whose election date sits after that window, intersect with whichever candidacies actually have an outcome label, then correlate. Recent features have small eligible pools. Old features cover everyone but discriminate poorly. The deliberate shaping is the analysis.

## How to read this document

- Sources are ordered by where the work happens: the keystone analytics mart first (most of the work lives here), then the upstream civics mart that feeds it, then Amplitude (engagement), then the product DB layer beyond what the marts surface, then viability score as a dedicated cross-cutting field, then L2 at schema-only granularity.
- Each source section covers the seven facets the user enumerated. Where a facet doesn't apply to a source it is marked "n/a, see [X]".
- The synthesis at the end maps the six analytical questions to supporting tables, fields, and the biggest data-quality risk per question.
- Coverage statistics are written as `[NB-N]` where N is a notebook cell; values are filled in after the notebook is run on Databricks.
- Companion notebook (`notebooks/inventory_queries.ipynb`) carries the queries that produced the coverage statistics cited inline. Each numeric claim points to a notebook cell. Run the notebook on Databricks (see `WORKFLOW.md` pattern in `analytics/win_churn/` for the local-edit / Databricks-execute setup).

---

## Completeness summary (start here)

A consolidated answer to "what fraction of the data is available for the connections we want to make?" The full detail per source is in the sections below; this is the top-of-funnel view.

**Scope** (resolved 2026-05-27):
- Cycle window: `general_election_date BETWEEN '2025-05-01' AND '2026-12-31'`.
- Unit of analysis: candidacy (`gp_candidacy_id`), filtered to Win users via `users_win_candidacy` (non-demo, latest version).
- Outcome variable: `latest_stage_reached + latest_stage_result` from `mart_civics.candidacy`.
- ICP: dimension (slice), not filter.

**Base population in window: 26,072 candidacies** (across 25,982 distinct users).

### Per-field completeness (all rates against the 26,072 in-window base)

| Field family | Field | Populated | % of in-window | Notes / source |
|---|---|---:|---:|---|
| **Outcomes (canonical)** | `latest_stage_result` (any value) | 15,826 | **60.7%** | Source 1.5 / Source 2.5. The preferred outcome variable. |
| | `latest_stage_result` in `('Won', 'Lost')` | ~13,681 | ~52% | Binary win/loss subset. |
| | `general_election_result` (general stage only) | 11,329 | 43.5% | Smaller than `latest_stage_result` because candidates eliminated at primary don't reach the general. |
| | `votes_received` / vote-share | — | per-stage | On `candidacy_stage`, not denormalized in `users_win_candidacy`. Source 2.5. |
| **Outcomes (Amplitude supplement)** | `Candidacy - Did You Win Modal Completed` (self-reported) | 2,136 users | 8.2% | Source 3.5. Of these, ~970 are NEW labels not in the civics mart. Heavy response-selection bias — 80% self-reported win rate. |
| **Viability** | `viability_score` (Viability 2.0, 0-5 scale) | 3,281 | **12.6%** | Source 4. Effectively bound by TS coverage of Win candidacies. |
| | `win_number` | 13,897 | 53.3% | BR-sourced; populates more readily than `viability_score`. |
| **Engagement (modeled)** | Amplitude user_id joinable | ~25k | ~96% | Source 3.3. Pre-2023-12-10 cohort has no event history. |
| | `Dashboard - Candidate Dashboard Viewed` (any) | 11,584 users | 44% | Source 3.1. Big-3 modeled event by users. |
| | `Voter Outreach - Campaign Completed` (any) | 1,238 users | 4.7% | Source 3.1. Power-user signal. |
| | `pro_upgrade_complete` (any) | 1,303 users | 5.0% | Source 3.1. Pro upgrade timing. |
| **Engagement (unmodeled but instrumented)** | Schedule Text Campaign funnel | up to 2,048 users | up to 7.9% | Source 3.1a / 3.8. 15 event types, 0 modeled. Aggregatable in-scope. |
| | Content Builder funnel | up to 2,331 users | up to 8.9% | Source 3.1a. 20 event types, 0 modeled. |
| | Onboarding mid-funnel (`Onboarding - Candidate Office Searched`) | 17,332 users | 66% | Source 3.1a. Largest single candidate-touched event, unmodeled. |
| | Pro Upgrade funnel | up to 3,686 users | up to 14.1% | Source 3.1a. 25 event types, 1 modeled (`pro_upgrade_complete`). |
| | Outreach planning | up to 4,024 users | up to 15.4% | Source 3.1a. 6 event types, 0 modeled. |
| **Segmentation (out of the box)** | `election_level` (non-NULL) | 14,181 | 54.4% | Source 1.4. 45.6% NULL — caveat for office-level slices. |
| | `office_type` | populated for most | ~80% (see NB-4) | Source 1.4. |
| | `campaign_state` | populated for most | ~99% | Source 1.4. |
| | `campaign_party` | populated for most | ~95% | Source 1.4. |
| | `is_pro` | 1,086 true | 4.2% | Source 1.4. Treatment variable for Pro lift analysis. |
| | `icp_office_win = true` | 12,427 | 47.7% | Source 1.4. ICP slice. |
| | `is_incumbent` (TS-sourced) | partial | ~70% | Source 2.4. Higher for TS-side. |
| | `is_open_seat` | partial | varies | Source 2.4. BR > TS > DDHQ; NULL on BR-only. |

### Per-join completeness (the connections matter)

| Connection | Join keys | Success rate in-window | Risk |
|---|---|---:|---|
| Win candidate → engagement | `users_win_candidacy.user_id` ↔ `int__amplitude_*.user_id` | ~96% (Amplitude users mostly match product users) | Pre-2023-12-10 candidates have no events but join; treat as missingness, not "no engagement". |
| Win candidate → outcomes | `users_win_candidacy.campaign_id` ↔ `candidacy.product_campaign_id` | ~98% (most Win users land on a candidacy) | Multi-cycle users → multiple candidacies; handle via `is_latest_version`. |
| Win candidate → viability score | candidacy join → viability via `br.viability_score` (currently) OR via `techspeed_candidate_code` (forward-stable) | ~13% via the mart field | Code/data discrepancy in viability surfacing (Source 4); EDA should be prepared to join `int__techspeed_viability_scoring` directly. |
| Win candidate → ICP context | candidacy join → `int__icp_offices` via `br_position_database_id` | partial (BR-position-bound) | Candidacies without a BR position match have NULL `icp_office_win` / `voter_count` / `l2_district_*`. |
| Win candidate → self-reported outcome | `user_id` ↔ Amplitude `Candidacy - Did You Win Modal Completed` | 8.2% (~2,136 users) | Response selection bias; the cohort overrepresents winners. |

### Headline "what we can answer"

- ✅ **Binary win/loss correlations on 2025 cohort** with ~13.7k labeled candidacies, viability stratification on ~3.3k, Pro/ICP stratification on the full cohort.
- ✅ **Recommended-action funnel** is answerable from raw Amplitude (Source 3.8) — requires building new intermediate aggregates, not new instrumentation.
- ⚠ **2026 forward-prediction** is limited — only 5% of 2026 candidacies have outcomes captured. The window includes most 2026 elections, but they haven't happened yet.
- ⚠ **Office-level analysis** is constrained by the 45.6% NULL `election_level` bucket; either backfill or treat as a fifth category.
- ⚠ **Viability stratification** is thin (12.6% coverage on Win users); the underlying TS-side intermediate has 99.7% rated, so the gap is in the mart-surfacing path, not the model itself.

---

## Source 1: `mart_analytics.users_win_candidacy` — the keystone

**Path:** `dbt/project/models/marts/analytics/users_win_candidacy.sql`
**Materialization:** view (inherits from project default)
**Grain:** one row per `campaign_version_id` (a campaign-version, where a single product `campaign_id` may have multiple historical versions if the user reused the campaign across cycles).

This mart already joins product user → candidacy → outcomes → viability → segmentation in a single denormalized row. **It is the base table for the outcomes analysis.** Most of the user's six analytical questions are answerable directly from this table plus a join to the Amplitude time-series.

### 1.1 Field/event inventory

| Group | Columns |
|---|---|
| Identity | `campaign_version_id` (pk), `campaign_id`, `campaign_slug`, `hubspot_id`, `user_id`, `user_email`, `user_first_name`, `user_last_name`, `user_phone`, `user_zip` |
| Campaign meta | `user_created_at`, `campaign_created_at`, `campaign_updated_at`, `is_verified`, `is_active`, `is_pro`, `is_demo`, `is_pledged`, `is_latest_version`, `is_win_user` |
| Election context | `election_date` (the version-tagged date; equals `general_election_date` where available), `campaign_state`, `campaign_office`, `campaign_party`, `election_level`, `ballotready_position_id` |
| Per-stage dates | `primary_election_date`, `primary_runoff_election_date`, `general_election_date`, `general_runoff_election_date` |
| ICP context | `icp_office_win`, `icp_office_serve`, `icp_win_supersize`, `ballotready_position_name`, `is_judicial`, `is_appointed`, `br_normalized_position_type`, `l2_district_name`, `l2_district_type`, `voter_count` |
| Candidacy meta | `official_office_name`, `office_type`, `candidacy_result`, `viability_score`, `win_number` |
| Per-stage outcomes | `primary_election_result`, `general_election_result`, `primary_runoff_election_result`, `general_runoff_election_result` |
| Serve | `is_serve_user`, `eo_activated_at` |

### 1.2 Data quality and coverage

**Claimed invariants (from dbt tests; see `m_analytics.yaml`):** the `m_analytics.yaml` model description is sparse for this view. The upstream tests live on the constituent tables — `campaigns` (`campaign_version_id` unique + not_null), `users` (`user_id` unique + not_null), `candidacy` (`gp_candidacy_id` unique + not_null, see `m_civics.yaml`).

**Observed coverage (NB-1, NB-2, cycle-windowed to `general_election_date BETWEEN '2025-05-01' AND '2026-12-31'`, populated 2026-05-27):**

- Non-demo latest-version rows total: 59,826
- **Non-demo latest-version rows in window: 26,072** (44% of total — this is the working population for the EDA)
- Distinct `user_id` in window: 25,982
- Distinct `campaign_id` in window: 25,983 (≈1:1 with users)

**Outcome and viability coverage within the cycle window:**

| Field | Populated | % of in-window |
|---|---:|---:|
| `latest_stage_result` (per-candidacy latest stage outcome — preferred outcome variable per scope decision) | 15,826 | **60.7%** |
| `general_election_result` (general-stage outcome only) | 11,329 | 43.5% |
| `viability_score` | 3,281 | **12.6%** |
| `win_number` | 13,897 | 53.3% |

`latest_stage_result` populated nearly 1.4× more often than `general_election_result` — confirms the resolved scoping decision to use the latest-stage pair as the outcome variable. Candidates who lost at primary or earlier stages get a captured outcome that the `general_election_result` projection masks as NULL.

**Quality flags:**
- `is_latest_version` separates the canonical row from historical reuse versions. For "did the user win?" analyses, pre-filter to `is_latest_version` unless explicitly studying campaign reuse. (`users_win_candidacy.sql:43`)
- `is_demo` excludes test campaigns. Apply the demo filter at the candidacy grain.
- The join from `campaigns` → `candidacy` is "version-aware": `campaign_id` AND `election_date = general_election_date` (or `election_date IS NULL` fallback). See `users_win_candidacy.sql:113-118`. This means a campaign-version that pre-dates BR coverage will surface as a row with NULL candidacy fields rather than a dropped row.

### 1.3 Join keys (out from this mart)

- To **Amplitude weekly activity**: `user_id` ↔ `int__amplitude_win_activity_weekly.user_id` (both BIGINT). Cast notes apply: the Amplitude staging model casts the string Amplitude `user_id` to BIGINT and excludes non-numeric IDs (`int__amplitude_win_activity_weekly.sql` references `int__amplitude_win_activity.sql:36-39, 53-56`).
- To **Amplitude milestones**: `user_id` ↔ `int__amplitude_user_milestones.user_id`.
- To **civics-mart candidacy_stage**: `campaign_id` ↔ `candidacy.product_campaign_id` (already done upstream; downstream consumers don't need to redo it).
- To **product DB raw**: `user_id` is the product DB user PK; `campaign_id` is the product campaign PK.

**Multi-cycle wrinkle:** when a user reuses a campaign for a new cycle, the product DB overwrites the row in place; Airbyte's insert-only stream preserves history; `campaigns.sql` rebuilds historical versions keyed by `campaign_version_id`. So one `user_id` may produce multiple `campaign_version_id` rows across cycles. The `users_win_candidacy` mart uses `campaign_version_id` as its grain — collapse to `user_id` only when answering user-level questions, with explicit handling of how to pick "the" candidacy.

### 1.4 Segmentation dimensions

Available out of the box:
- **Office level**: `election_level`. Observed values + counts (NB-4, non-demo latest, cycle-windowed):

| election_level | rows | % of in-window | viability % | `latest_stage_result` % | `general_election_result` % |
|---|---:|---:|---:|---:|---:|
| **NULL** | **11,891** | **45.6%** | 8.5% | 50.1% | 34.6% |
| city | 11,178 | 42.9% | 19.5% | 84.4% | 63.3% |
| state | 1,233 | 4.7% | 1.2% | 10.2% | 0.7% |
| federal | 897 | 3.4% | 0.1% | 1.8% | 0.1% |
| county | 873 | 3.3% | 7.8% | 32.6% | 15.1% |

**⚠ 45.6% of in-window candidacies have NULL `election_level`** (down from 61.5% in the unwindowed view, but still the largest single bucket). The NULL bucket has 50% `latest_stage_result` coverage and 34% `general_election_result` coverage — meaningfully populated, just unclassified by level. Investigating this NULL bucket is a prerequisite for any office-level slice — these 11,891 rows include candidacies where the position couldn't be mapped to a level upstream. Verify in notebook before deciding whether to (a) restrict to non-NULL, (b) drop, or (c) backfill via a different source (e.g. join to `int__icp_offices` or extract from `office_type`).

- **Office type**: `office_type` (string; HubSpot-sourced, common values are documented at `m_civics.yaml:507-514` — School Board, City Council, Mayor, Judge, State House, etc.)
- **Geography**: `campaign_state`, plus the L2 district context from `int__icp_offices` (`l2_district_name`, `l2_district_type`)
- **Party**: `campaign_party`, plus `is_partisan` on the candidacy upstream
- **Incumbency**: not in `users_win_candidacy` directly. Available on `candidacy.is_incumbent` (TS-sourced; `m_civics.yaml` notes "TS: 51k populated, BR: 0"). Join to civics mart if needed.
- **Open seat**: `candidacy.is_open_seat` (BR > TS > DDHQ). Not in `users_win_candidacy` directly.
- **Time-to-election**: derive from `(general_election_date - <asof_date>)` where asof_date is the engagement reference point. No precomputed bucket exists in this mart.
- **ICP gating**: `icp_office_win` to restrict to offices Win supports; `icp_win_supersize` to flag large-electorate offices that are outside the standard Win process.
- **Pro vs free**: `is_pro` (boolean; campaign-grain — true if `campaigns.is_pro = true` for that version)
- **Verified vs unverified**: `is_verified` (note: manual verification was discontinued in 2026; see `m_civics.yaml:617`)

### 1.5 Outcome variables

`users_win_candidacy` already pivots per-stage outcomes. Available:

| Column | Definition | Use |
|---|---|---|
| `primary_election_result` | Latest result on the primary stage | Primary-only or staged analyses |
| `general_election_result` | Latest result on the general stage | "Did they win the general?" (preferred, no contamination from primary fallback) |
| `primary_runoff_election_result` | Primary runoff result | Funnel analysis |
| `general_runoff_election_result` | General runoff result | Funnel analysis |
| `candidacy_result` | Rolled-up outcome with cross-stage fallback | Convenience column, BUT see warning |

**⚠ `candidacy_result` fallback hazard.** Per `m_civics.yaml:516-545`, `candidacy_result` rolls up "general-runoff > general > primary-runoff > primary fallback". This means a candidate who won their primary but lost their general (or has no general yet captured) can show `candidacy_result = 'Won'`. For binary "did they win the election?" analyses, **prefer `general_election_result`**. For funnel analyses, use `latest_stage_reached` + `latest_stage_result` from the upstream `candidacy` mart.

**Vote counts** (votes_received, vote_percentage, is_winner): live on `candidacy_stage`, NOT denormalized into `users_win_candidacy`. Join through `candidacy.gp_candidacy_id` → `candidacy_stage.gp_candidacy_id` if needed. See Source 2 below.

**Domain values (per `m_civics.yaml:543-602`):** `Won`, `Lost`, `Runoff`, `Withdrew`, `Not on Ballot`, `Cannot Determine`. The `accepted_values` test on `candidacy_result` excludes `Cannot Determine` from `general_election_result`, `latest_stage_result`, and the per-stage `election_result` (per-stage values don't carry the "cannot determine" bucket).

### 1.6 Viability score

`users_win_candidacy` surfaces `viability_score` and `win_number` (pulled through from `candidacy`). See Source 4 below for full characterization — it's a cross-cutting field that deserves its own section.

### 1.7 Mapping to questions

Answered directly from this mart, optionally joined to Amplitude (covered in Source 3):
- "Which product actions correlate with winning, controlling for office type and level?" → outcome from `general_election_result`; engagement from Amplitude weekly rollup joined on `user_id`; controls from `election_level` + `office_type` + `is_incumbent` (latter from civics join).
- "Does Pro-tier show outcome lift?" → outcome from `general_election_result`; treatment from `is_pro`; controls as above. Caveat: `is_pro` reflects the campaign-grain Pro flag at the time the mart materializes; upgrade timing requires the milestones model (Source 3).
- "Are there meaningful user segments with distinct outcome patterns?" → segment on the segmentation dimensions enumerated in 1.4, outcome from `general_election_result`.

---

## Source 2: `mart_civics.candidacy` + `mart_civics.candidacy_stage` — upstream outcomes

**Paths:**
- `dbt/project/models/marts/civics/candidacy.sql`
- `dbt/project/models/marts/civics/candidacy_stage.sql`

Use these when `users_win_candidacy` doesn't carry the field you need (vote counts, per-stage match metadata, incumbency, open-seat flags). Also use when answering questions about the broader candidate universe — including candidates with no Win product user (the universe outside the `is_win_user` filter).

### 2.1 Field/event inventory

**Candidacy mart** carries the candidacy-grain rollup. Key columns beyond what `users_win_candidacy` already surfaces:
- Identity: `gp_candidacy_id` (pk), `gp_candidate_id`, `gp_election_id`, `product_campaign_id`, `hubspot_contact_id`, `hubspot_company_ids`
- Per-source diagnostic: `candidate_id_source`, `source_systems` (array — values: `hubspot`, `ballotready`, `techspeed`, `ddhq`, `gp_api`)
- Candidacy meta: `is_incumbent`, `is_open_seat`, `is_partisan`, `office_level`, `candidate_office`, `official_office_name`, `office_type`, `br_position_database_id`
- ICP context: `is_win_icp`, `is_serve_icp`, `is_win_supersize_icp` (all NULL when no BR position match)

**Candidacy-stage mart** carries the stage-grain results. Key columns (`m_civics.yaml:839-1000+`):
- `gp_candidacy_stage_id`, `gp_candidacy_id`, `gp_election_stage_id`
- `candidate_name`, `source_candidate_id`, `source_race_id`, `candidate_party`
- **Outcomes:** `is_winner` (boolean), `election_result` (string), `election_result_source` (`ddhq` / `ballotready` / `techspeed` / `hubspot`), `votes_received` (STRING — may contain literal "uncontested" alongside numeric counts), `is_uncontested`
- LLM match metadata (2025 archive only): `match_confidence`, `match_reasoning`, `match_top_candidates`, `has_match`
- `election_stage_date`, `election_stage`

### 2.2 Data quality and coverage

**The 2025 archive vs 2026+ cycle shape mismatch is the biggest source-level risk.** Per `candidacy.sql:36-72`, 2025 rows come from `int__civics_candidacy_2025` (a HubSpot-only archive). 2026+ rows come from a 4-way FOJ over `int__civics_candidacy_ballotready` / `_techspeed` / `_ddhq` / `_gp_api`. Coverage of fields differs across cycles:
- `is_open_seat`: BR > TS > DDHQ in 2026+; absent from 2025 archive (`m_civics.yaml` would show coverage by cycle if queried).
- `is_incumbent`: TS-leading, BR-fallback in 2026+. The yaml note "TS: 51k populated, BR: 0" suggests TS does the heavy lifting; check 2025 separately.
- `viability_score`, `win_number`: BR-only in 2026+. 2025 may carry through HubSpot — verify in notebook.
- `source_systems`: `hubspot` value only appears for 2025 archive rows. For 2026+ rows the array is some subset of `{ballotready, techspeed, ddhq, gp_api}`.

**Observed coverage in the cycle window (NB-5, non-demo latest, `users_win_candidacy` joined to `mart_civics.candidacy` for `latest_stage_result`):**

| cycle | rows | viability % | `latest_stage_result` % | `general_election_result` % | won (latest) | lost (latest) |
|---|---:|---:|---:|---:|---:|---:|
| 2025 | 16,280 | 16.9% | 91.8% | 66.4% | 7,541 | 5,293 |
| 2026 | 9,792 | 5.3% | 9.1% | 5.4% | 462 | 385 |

**Interpretation:**
- **2025 is the operative cohort for outcomes**: 92% have a `latest_stage_result` and 66% have a `general_election_result`. Most 2025 races have concluded.
- **2026 is almost entirely unconcluded**: only 9.1% have any `latest_stage_result` captured. The few cases that do are likely early primaries that have already happened.
- The 2025 cohort dominates win/loss labels — **12,834 of the 13,681 in-window labeled outcomes (94%) are 2025 races**. Any cross-cycle analysis is effectively a 2025-anchored analysis with limited 2026 holdout.

(The `latest_stage_result` populated rate is much higher than `general_election_result` for both cycles, confirming the resolved scope decision.)

Distinct `source_systems` combinations: see NB-5 second pane.

**Tests (from `m_civics.yaml`):**
- `gp_candidacy_id`: unique, not_null
- `source_systems`: not_null
- `candidacy_result`, `general_election_result`, `latest_stage_result` have `accepted_values` constraints
- `br_position_database_id` has a relationships test to the BR position API (with a warn-only threshold because S3 data can be ahead of API loads)

### 2.3 Join keys

- `candidacy.gp_candidacy_id` ↔ `candidacy_stage.gp_candidacy_id` (one-to-many, one per stage)
- `candidacy.gp_candidate_id` ↔ `candidate.gp_candidate_id`
- `candidacy.product_campaign_id` ↔ `campaigns.campaign_id` (the product join — confirmed at `users_win_candidacy.sql:113-118`)
- `candidacy.gp_election_id` ↔ `election.gp_election_id`
- `candidacy_stage.gp_election_stage_id` ↔ `election_stage.gp_election_stage_id`

**Multi-cycle wrinkle:** one `gp_candidate_id` can have many `gp_candidacy_id` rows (one per cycle). The `users_win_candidacy.sql:113-118` join uses `campaign_id + election_date` to pick the right cycle, but a campaign-version without an `election_date` falls back to `campaign_id` alone and could attach to any cycle for that user.

### 2.4 Segmentation dimensions

In addition to what's surfaced in `users_win_candidacy`:
- `is_incumbent` (TS-sourced, sparse on BR side)
- `is_open_seat` (BR > TS > DDHQ)
- `is_partisan` (boolean)
- `office_level`, `office_type`, `candidate_office`, `official_office_name`

### 2.5 Outcome variables

| Column | Grain | Notes |
|---|---|---|
| `candidacy.candidacy_result` | candidacy | Rolled-up; has fallback hazard (see Source 1) |
| `candidacy.general_election_result` | candidacy | Derived from latest-stage logic; null if no general |
| `candidacy.latest_stage_reached` + `latest_stage_result` | candidacy | Funnel-aware pair |
| `candidacy_stage.is_winner` | stage | Boolean; DDHQ-authoritative |
| `candidacy_stage.election_result` | stage | String with `accepted_values` constraint |
| `candidacy_stage.votes_received` | stage | STRING — handle "uncontested" before casting |
| `candidacy_stage.is_uncontested` | stage | NULL for BR/TS-only rows (DDHQ-only field) |
| `candidacy_stage.election_result_source` | stage | Diagnostic — track which provider claimed this result |

**Vote-share** (votes_received / district_total_votes) is not precomputed. The denominator (total votes cast in the stage) is not in the marts I've reviewed — confirm in notebook whether `election_stage` or `candidacy_stage` carry it.

### 2.6 Viability score → see Source 4

### 2.7 Mapping to questions

- Vote share, race competitiveness, opponent count → only answerable via `candidacy_stage` queries that aggregate across candidates per stage (`gp_election_stage_id` → count of candidacies, sum of votes). Build these in notebook.
- Open-seat / incumbency / partisan-vs-not segmentation → requires joining `users_win_candidacy` to `candidacy` for the fields not already pivoted across.
- 2025-vs-2026+ cycle structural analysis → use `source_systems` and `created_at` year to slice.

---

## Source 3: Amplitude product events — engagement

**Paths:**
- `dbt/project/models/intermediate/amplitude/int__amplitude_win_activity.sql` (monthly, 2 event types)
- `dbt/project/models/intermediate/amplitude/int__amplitude_win_activity_weekly.sql` (weekly, same 2 event types)
- `dbt/project/models/intermediate/amplitude/int__amplitude_user_milestones.sql` (user-grain, lifecycle milestones)
- `dbt/project/models/intermediate/amplitude/int__amplitude.yaml`
- `dbt/project/models/staging/airbyte_source/amplitude_api/stg_airbyte_source__amplitude_api_events.sql`
- `dbt/project/models/marts/analytics/users_win_activity.sql` (user-month mart)

### 3.1 Field/event inventory

**`int__amplitude_win_activity` and `int__amplitude_win_activity_weekly`** track only these two event types (`int__amplitude_win_activity.sql:57-60`):

| Amplitude event_type | Columns it populates |
|---|---|
| `Voter Outreach - Campaign Completed` | `campaigns_sent`, `recipient_count` (sum, capped at 100k), `first_campaign_sent_at`, `last_campaign_sent_at` |
| `Dashboard - Candidate Dashboard Viewed` | `dashboard_views`, `dashboard_view_days` (distinct days), `first_dashboard_viewed_at`, `last_dashboard_viewed_at` |

Combined: `total_events` (sum of the two), `activity_days` (distinct days with any of the two events).

**`int__amplitude_user_milestones`** (user-grain, one row per user) carries lifecycle milestones — first/total/last timestamps for these events (`int__amplitude_user_milestones.sql:18-47`):

Win-relevant:
- `Onboarding - Registration Completed` → `amplitude_registration_completed_at`, `registration_country`
- `Dashboard - Candidate Dashboard Viewed` → `first_dashboard_viewed_at`, `last_dashboard_viewed_at`, `dashboard_view_count`
- `onboarding_complete` → `onboarding_completed_at`
- `Voter Outreach - Campaign Completed` → `first_campaign_sent_at`, `total_campaigns_sent`, `total_recipient_count`
- `pro_upgrade_complete` → `pro_upgrade_completed_at`

Serve-side (not directly relevant to Win-only analyses but present): `Serve Onboarding - Getting Started Viewed`, `Constituency Profile Viewed`, `Poll Value Props Viewed`, `Poll Strategy Viewed`, `Add Image Viewed`, `Poll Preview Viewed`, `SMS Poll Sent`.

### 3.1a Universe vs modeled subset

The intermediate Amplitude models cover only a small slice of what is actually being tracked. Counting distinct `event_type` values within the analysis window (2025-05-01 to 2026-12-31, per NB-9):

- **Total distinct event types in window:** 314
- **Aggregated in at least one intermediate model:** 12 (3.8% of event types)
- **Events on modeled types:** 291,478 of 12,363,089 total events (2.4%)

Most bulk volume is auto-instrumented Amplitude system events (`[Amplitude] Page Viewed`, `Scroll Depth`, `session_start`) with no `user_id` attached. Restricting to candidate-attributed product events (excluding `amplitude_autotrack`, `experiment_assignment`, `gtm_bootstrap`, `session_or_browser`, `viewed_generic`, `marketing_or_misc`, `cta_clicks`):

- **Candidate-attributed events in window:** ~1.4M
- **Modeled candidate-attributed events:** 291k (~21%)

Coverage stat ran via `notebooks/_pull_amplitude_universe.py` against `goodparty_data_catalog.dbt_staging.stg_airbyte_source__amplitude_api_events`. Per-event-type breakdown lives in `notebooks/amplitude_event_universe.csv` (gitignored, regenerable from NB-9).

**Event families in window, sorted by total events:**

| event_family | n_types | events | max_users_one_event | n_modeled | Notes |
|---|---:|---:|---:|---:|---|
| `viewed_generic` | 1 | 3,531,322 | 23,699 | 0* | `Viewed` event. `int__amplitude_serve_activity` filters this on `event_properties:path = '/dashboard/polls'` — partial use. |
| `amplitude_autotrack` | 12 | 3,039,947 | 0 | 0 | `[Amplitude] *` autoevents. Anonymous. Skip. |
| `session_or_browser` | 8 | 2,443,054 | 455 | 0 | `Scroll Depth`, `session_*`. Mostly anonymous. Skip. |
| `experiment_assignment` | 4 | 1,891,990 | 12,152 | 0 | `[Experiment] Assignment/Exposure/Impression`. Usable as a stratification covariate. |
| **`win_onboarding`** | 26 | 292,245 | 17,332 | 2 | We model registration + `onboarding_complete`. 24 mid-funnel events unmodeled. Largest single candidate-touched event: `Onboarding - Candidate Office Searched` (17,332u). |
| **`win_dashboard`** | 8 | 253,821 | 11,589 | 1 | We model `Dashboard - Candidate Dashboard Viewed`. New events: `Dashboard - Campaign Plan Viewed` (started 2026-04, AI weekly plan). |
| **`win_outreach_scheduling`** | 15 | 252,368 | 2,048 | 0 | Schedule Text Campaign funnel — 15 steps from script generation to send. Entirely unmodeled. |
| `navigation` | 22 | 251,831 | 5,811 | 0 | Site navigation. Useful for path analysis if needed. |
| **`win_content_builder`** | 20 | 63,924 | 2,331 | 0 | Content / AI authoring funnel — `Content Builder: Generation Started/Completed`, `ai_content_generation_*`. |
| **`win_voter_data`** | 24 | 53,734 | 1,515 | 0 | Voter file viewing, downloading, customizing. |
| **`win_outreach_planning`** | 6 | 45,881 | 4,024 | 0 | `Outreach - View Accessed` (4,024u), `Outreach - Click Create` (2,605u), door-knock + social-media actions. |
| **`win_pro_upgrade`** | 25 | 44,612 | 3,686 | 1 | We model `pro_upgrade_complete` only. 24 mid-funnel events unmodeled (Modal Shown / Exit / Splash / Confirm / Service Agreement). |
| `marketing_or_misc` | 7 | 40,300 | 1 | 0 | Anonymous marketing events. Skip. |
| **`win_compliance_or_planning`** | 4 | 28,271 | 619 | 0 | 10DLC compliance, campaign verify, campaign plan weekly digest. |
| **`win_candidate_profile`** | 20 | 26,895 | 2,161 | 0 | Profile setup funnel — running against / top issues / campaign details / fun facts. |
| **`serve`** | 33 | 25,873 | 1,829 | 7 | Serve product. 7 onboarding events modeled. Unmodeled: `Serve Onboarding - Meet Your Constituents Viewed` (1,713u), Sworn In events, polls authoring. |
| `auth_or_settings` | 20 | 23,703 | 1,295 | 0 | Sign in / sign up / settings / account. |
| **`win_candidate_website`** | 8 | 10,986 | 1,516 | 0 | Public candidate-website builder — Started (1,516u) / Published (1,267u). |
| **`win_voter_outreach`** | 8 | 10,751 | 1,240 | 1 | We model `Voter Outreach - Campaign Completed`. Unmodeled: 10DLC compliance funnel for outreach. |
| **`win_ai_assistant`** | 10 | 10,435 | 1,482 | 0 | Campaign assistant — `question_complete` (1,482u), `AI Assistant: Ask a question` (985u). |
| **`win_candidacy_self_report`** | 3 | 5,951 | 2,281 | 0 | **`Candidacy - Did You Win Modal` events — see "Outcome variables" subsection below; this is a previously-unsurfaced outcome stream.** |
| **`win_contacts`** | 6 | 4,966 | 624 | 0 | Contacts CRM features. |
| `cta_clicks` | 6 | 4,432 | 8 | 0 | Marketing CTAs. Mostly anonymous. Skip. |
| **`win_p2p_upgrade`** | 3 | 3,161 | 923 | 0 | Peer-to-peer texting upgrade. Separate from Pro. |
| **`win_resources`** | 1 | 2,328 | 807 | 0 | Resource library click-throughs. |
| **`win_briefings`** | 6 | 242 | 8 | 0 | Brand-new feature (started 2026-04-10). Tiny user base. |
| `payment` | 3 | 60 | 18 | 0 | Payment flow. Tiny. |
| `gtm_bootstrap` | 4 | 4 | 0 | 0 | GTM init. Skip. |
| `other` | 1 | 2 | 0 | 0 | Unclassified noise. Skip. |

\* `viewed_generic` n_modeled = 0 strictly, but `int__amplitude_serve_activity` uses a property-filtered slice of `Viewed`.

**Top unmodeled events per Win family, ranked by `distinct_users`** (the events we'd most want to know about for a "what actions correlate with outcomes" analysis):

- **`win_onboarding`**: `Onboarding - Candidate Office Searched` (17,332u, 129k events), `Onboarding - Office Step: Office Selected` (12,758u), `Onboarding - Office Step: Click Next` (12,204u), `Onboarding - Party Step: Click Submit` (11,902u), `Onboarding - Candidate Office Completed` (11,794u)
- **`win_outreach_scheduling`**: `Schedule Text Campaign: Exit` (2,048u), `Schedule Text Campaign: Next` (1,851u), `Schedule Text Campaign - Audience: Check Audience` (1,332u)
- **`win_pro_upgrade`**: `Pro Upgrade - Modal: Modal Shown` (3,686u — denominator for upsell impressions), `Pro Upgrade - Modal: Exit` (2,768u), `Pro Upgrade: Confirm office` (2,329u), `Pro Upgrade - Splash Page: Click upgrade` (2,161u)
- **`win_content_builder`**: `Content Builder: Generation Started` (2,331u), `Content Builder: Generation Completed` (2,317u), `Content Builder: Click Generate` (2,178u)
- **`win_outreach_planning`**: `Outreach - View Accessed` (4,024u), `Outreach - Click Create` (2,605u)
- **`win_candidate_profile`**: `Profile - Running Against: Click Save` (2,161u), `Profile - Top Issues: Click Finish Entering Issues` (1,595u)
- **`win_ai_assistant`**: `question_complete` (1,482u), `AI Assistant: Ask a question` (985u)
- **`win_voter_data`**: `Voter Data: Click Detail View` (1,515u), `Download Voter File attempt` (1,212u)
- **`win_candidate_website`**: `Candidate Website - Started` (1,516u), `Candidate Website - Published` (1,267u)
- **`win_candidacy_self_report`**: `Candidacy - Did You Win Modal Viewed` (2,281u, started 2025-10-31), `Candidacy - Did You Win Modal Completed` (2,159u)

**Instrumentation start date pattern:** most product-event families have first-seen dates clustered around 2025-05-28 (suggesting a major instrumentation push around that date). Briefings (2026-04-10), Candidacy self-report (2025-10-31), and Dashboard Campaign Plan (2026-04-09) are newer. Anything before 2025-05-28 was tracked sparsely.

**Implications for the analysis:**

1. **The "what fraction of recommended actions do users complete" question is answerable** — see updated subsection 3.8 below. Rich instrumentation exists across the onboarding / outreach / content / Pro funnels; the work is to build new intermediate aggregates from the staging table, not to add new instrumentation.
2. **The current Amplitude intermediate models vastly undersample available signal.** Adding event-family aggregates is a tractable in-scope build using the FAMILY_CASE_SQL pattern in NB-9.
3. **`Candidacy - Did You Win` is a previously-unsurfaced outcome stream.** See updated subsection 3.5 below.

### 3.2 Data quality and coverage

- **Source**: `stg_airbyte_source__amplitude_api_events` → ultimately Amplitude API via Airbyte.
- **Date floor**: Amplitude tracking for Win began **2023-12-10**. Users registered before that date have no Amplitude history. This is implicit (no explicit filter; just no events before that date) — `users_win_base.is_post_amplitude_registration = (u.created_at >= '2023-12-10')` (`users_win_base.sql:124`).
- **Volume (from PR #399 SESSION_NOTES.md, the weekly rollup author's own verification)**: 24,311 user-weeks across 11,529 distinct users; 248,680 total events; 6,389 campaigns_sent; 242,291 dashboard_views; 35,670 activity_days. Validated parity vs. the monthly mart at per-user grain.
- **`recipient_count` is mostly null**: only populated for `Voter Outreach - Campaign Completed` events; 9.8% of user-weeks. SESSION_NOTES.md from win_churn confirms.
- **`recipient_count` data-quality cap**: values > 100k or < 0 are nulled out (`int__amplitude_win_activity.sql:46-52`). Tracks Amplitude instrumentation errors observed in production.
- **User ID coercion**: `try_cast(user_id as bigint)`; non-numeric or empty IDs excluded (`int__amplitude_win_activity.sql:38-39, 55-56`). This loses anonymous/pre-auth events — flag if "first session" pre-signup needs to be in scope.

### 3.3 Join keys

- `user_id` (BIGINT) ↔ product DB `user.id` (BIGINT).
- Amplitude raw `user_id` is a STRING; the cast happens at the staging→intermediate boundary.
- `int__amplitude_user_milestones.user_id` has a `relationships` test to `mart_civics.users.user_id` with `severity: warn` (`int__amplitude.yaml:14-22`). Some Amplitude user IDs may not map to a product user (test/staging events; deleted users).

### 3.4 Segmentation dimensions

None native to the Amplitude models — they're event aggregates. Segmentation comes from joining `user_id` to `users_win_candidacy` (for office/state/party/Pro) or `users_win_base` (for registration cohort, onboarding state).

### 3.5 Outcome variables

Mostly engagement, **but with one important exception verified in NB-9c**: the `win_candidacy_self_report` family carries self-reported electoral outcomes captured in product.

| event_type | distinct_users | events | First seen |
|---|---:|---:|---|
| `Candidacy - Did You Win Modal Viewed` | 2,281 | 3,350 | 2025-10-31 |
| `Candidacy - Did You Win Modal Completed` | 2,159 | 2,434 | 2025-10-31 |
| `Candidacy - Campaign Completed` | 155 | 167 | 2025-09-18 |

**Verified payload schema** (NB-9c, against `event_properties` of `Candidacy - Did You Win Modal Completed`):

```json
{"impersonation": <bool | null>, "status": "won" | "lost"}
```

- `status` is the user's self-reported outcome — `"won"` or `"lost"`. No other values observed.
- `impersonation` indicates whether a staff member triggered the event while impersonating a user. The field was **added 2026-03** (events before that have `impersonation = null`; events after carry an explicit `true` or `false`). For analytical use, treat `null` as not-impersonated (the legacy cohort is the bulk of the data — 2,007 of 2,160 users).

**User-grain summary (latest event per `user_id`, dropping impersonated):**

| | users |
|---|---:|
| Total users with completed modal | 2,159 |
| With non-impersonated latest response | 2,136 |
| ↳ self-reported "won" | 1,707 (80%) |
| ↳ self-reported "lost" | 429 (20%) |

**Response selection bias is severe.** The 80% self-reported win rate is far above the 64% raw win rate among all candidacies with a `general_election_result` (Source 1.4). Two plausible drivers: (a) winners are more motivated to come back to the product and respond; (b) the modal may be shown more aggressively to winners. **The cohort is NOT a random sample of Win candidates.** Use it cautiously.

**Reconciliation with civics-mart outcomes (NB-9c):** of the 2,136 users with a non-impersonated self-reported response:

| Civics-mart state | Users | Notes |
|---|---:|---|
| `general_election_result` populated AND matches Amplitude | ~1,201 | Reconciled both ways |
| `general_election_result` populated, Amplitude disagrees | ~17 | Disagreement (<1%) — manual investigation needed; likely user error, multi-cycle confusion, or modal interpretation issue |
| `general_election_result` NULL, but `latest_stage_result` populated | ~708 | **Mart partial — Amplitude adds full outcome.** Most are primary-stage losses or primary-stage wins reported with no general stage ingested yet. |
| `general_election_result` NULL AND `latest_stage_result` NULL (no civics-mart row) | ~262 | **Amplitude-only outcome.** Users without a civics-mart candidacy join (could be edge-case `campaign_id` mismatch or candidacy not ingested). |

**Net new outcome labels addressable from Amplitude (beyond `general_election_result`): ~970 users.** This nearly doubles the labeled-outcome population (from 14,279 civics-mart general results to ~15,200 with Amplitude added).

**Caveat on combining sources**: the modal asks the candidate about the general outcome but no formal authority order has been defined. The 17-user disagreement set should be inspected before adopting an authoritative-mart-then-Amplitude-fallback rule.

**Implications for the EDA**:
- Source 1.5's outcome columns plus the Amplitude self-reported `status` together give a richer label set, especially for 2025 cycle wins/losses that the civics mart hasn't ingested.
- The response selection bias means we can't use the self-reported `won` rate as a population estimate — but we can use the labels themselves to study correlates of winning *within the responding cohort*.
- The 2,136 responding users are 3.6% of the 59,693 non-demo latest-version candidacies — small but non-trivial.

### 3.6 Viability score → n/a

### 3.7 Mapping to questions

- "Which product actions correlate with winning?" → only `Voter Outreach - Campaign Completed` and `Dashboard - Candidate Dashboard Viewed` are individually addressable. Other actions are lumped — or are missing from instrumentation entirely. **This is a meaningful gap.**
- "What fraction of recommended actions do users complete, and where's the drop-off?" → **Likely not answerable with current Amplitude instrumentation.** The closest proxy is the milestones model: Registration → Dashboard → Campaign Sent → Pro Upgrade is a coarse 4-stage funnel. If the user wants a recommended-actions funnel proper, look at the product DB (Source 4) for any `campaign_planner`-style step table, or flag a new instrumentation ask.
- "How does behavior change as a function of days-to-election?" → join weekly activity to per-stage dates on `users_win_candidacy`, compute `general_election_date - week_start_date`, bucket. Watch the 2023-12-10 floor (pre-floor users are missing).
- "Time-to-first-meaningful-action" → use `min(first_dashboard_viewed_at, first_campaign_sent_at)` from milestones; anchor to `users_win_base.registered_at`.

### 3.8 Recommended-action funnel — revised after NB-9 universe scan

The original framing ("instrumentation doesn't directly support this") was wrong. After NB-9 surfaced the full 314-event-type universe, the picture is:

**Rich funnel instrumentation already exists in the raw Amplitude stream.** Specifically:

- **Schedule Text Campaign funnel** (15 events, 252k events in window, 2,048u max per step): script generation → audience selection → preview → send.
- **Content Builder funnel** (20 events, 64k events in window, 2,331u max per step): generation started → completed → publish.
- **Pro Upgrade funnel** (25 events, 45k events in window, 3,686u max per step): modal shown → splash page → confirm office → service agreement → complete.
- **Profile setup funnel** (20 events, 27k events in window, 2,161u max per step): running against / top issues / campaign details / fun facts.
- **Onboarding funnel** (26 events, 292k events in window, 17,332u max per step): registration → office search → office selected → party → pledge → complete.
- **Outreach planning funnel** (6 events, 46k events in window, 4,024u max per step): view → click create → action clicked → method-specific completion (door knock / social / phone).
- **Candidate Website funnel** (8 events): started → domain selection → published.

**What's not modeled is the aggregation, not the events.** The work to answer "what fraction of recommended actions do users complete, and where's the drop-off" is:

1. Define which funnels count as "recommended actions" with the product team (most likely the Onboarding + Outreach + Profile setup funnels).
2. Add intermediate-layer aggregation models on top of `stg_airbyte_source__amplitude_api_events` that compute per-user funnel-step reach. Pattern follows `int__amplitude_user_milestones`.
3. The aggregation can be funnel-step-grain (user × step → reached_at), which then collapses to user-grain via pivot.

This is **in-scope analytics work**, not a new instrumentation ask. Drop-off rates per step are derivable directly from the existing raw events.

**Caveat — funnel definition is a product decision, not a data decision.** Which Win product actions actually count as "recommended" in the recommended-action sense (the dashboard widget the user has in mind?) needs confirmation from product. The data supports any reasonable definition we land on.

---

## Source 4: Viability Score 2.0 — dedicated characterization

**Important correction from the previous draft:** Viability Score 2.0 is the politics-team electoral-viability-for-winning score (not, as previously stated, a BR-only field or a lead-routing scorer). It is produced internally from TechSpeed-primary candidate data by the MLflow model `goodparty_data_catalog.model_predictions.ViabilityWithOpponentData`, joined into the candidacy mart via several paths described below.

### 4.1 What it is

`int__techspeed_viability_scoring.py` (`dbt/project/models/intermediate/techspeed_to_hubspot/`) wraps the MLflow model registered as `ViabilityWithOpponentData`. The model is a sklearn classifier that outputs `predict_proba`; the wrapper transforms the probability into:

- **`viability_rating_2_0`** — `round(5 * probability_of_winning, 2)`. Range 0.0 to 5.0. This is the column the user referenced as "viability 2.0".
- **`score_viability_automated`** — 5-band categorical label:
  - `< 1.0` → `No Chance`
  - `1.0 – < 2.0` → `Unlikely to Win`
  - `2.0 – < 3.0` → `Has a Chance`
  - `3.0 – < 4.0` → `Likely to Win`
  - `≥ 4.0` → `Frontrunner`

**Features (model inputs):** `is_incumbent`, `open_seat`, `multi_seat`, `partisan_contest`, `is_unexpired` (always False in this pipeline), `log_n_losers` (log of opponents-minus-seats, floored at log(0.001) for uncontested), plus three Weight-of-Evidence-encoded categorical features `state_woe`, `level_woe`, `office_type_woe` (encodings live in `goodparty_data_catalog.model_predictions.viability_br_{state,level,office_type}_woe`).

**A row gets a score only if every feature is populated** (`int__techspeed_viability_scoring.py:53` — `valid_rows = df[model.feature_names_in_].notnull().all(axis=1)`). Missing one feature → NULL rating. This is why BR-only candidacies are "spotty" per the user — BR doesn't supply `n_seats`, `is_incumbent`, or `n_candidates` for all rows.

### 4.2 Where the score lives (3 distinct tables, distinct join paths)

| Table | Key | Rows | Rated | Purpose |
|---|---|---:|---:|---|
| `goodparty_data_catalog.dbt.int__techspeed_viability_scoring` | `techspeed_candidate_code` | 63,223 | 63,010 (99.7%) | The model output, keyed to TS-side candidates. Used downstream by `m_techspeed__candidate_to_hubspot` to push scores to HubSpot. |
| `goodparty_data_catalog.dbt.stg_model_predictions__viability_scores` | HubSpot company `id` | 10,509 | 10,509 (100%) | A separate scoring run keyed to HubSpot company IDs. Used by `int__civics_candidacy_2025.sql:89,113` to enrich the 2025 archive via `tbl_companies.company_id = viability_scores.id`. |
| `goodparty_data_catalog.mart_civics.candidacy.viability_score` | `gp_candidacy_id` | varies by source bucket | varies (see 4.3) | The mart-level field surfaced into `users_win_candidacy`. Different join paths depending on whether the candidacy is BR-matched, TS-only, gp_api-only, ddhq-only, or 2025-archive. |

### 4.3 Coverage in `mart_civics.candidacy` by source bucket (NB-7a, 2025-05-01 to 2026-12-31 window)

| source_bucket | rows in window | viability populated | coverage % |
|---|---:|---:|---:|
| 2025 archive (HubSpot) | 65,284 | 2,854 | **4.4%** |
| 2026+ TS-only (no BR match) | 36,395 | 35,611 | **97.8%** |
| 2026+ BR-matched (TS+BR) | 14,475 | 8,148 | **56.3%** |
| 2026+ gp_api-only | 8,808 | 0 | **0%** |
| 2026+ ddhq-only | 2,990 | 0 | **0%** |

**This breakdown corrects the earlier "BR-only / 17% overall" framing.** Coverage is highest where TechSpeed has provided full feature data (97.8%), middling where BR has been matched to a TS candidacy but BR's feature contribution leaves gaps (56.3%), and absent for candidacies sourced solely from `gp_api` or DDHQ (because those sources don't feed `int__techspeed_viability_scoring`).

**For the Win-product analysis specifically:** `users_win_candidacy` filters to Win-product users (i.e. candidacies linked through `gp_api.product_campaign_id`). Win users get attached to a candidacy via ER crosswalk; whichever side the canonical match landed on determines whether the score surfaces. The empirical Win-user viability coverage from Source 1.4 (17.1% overall, city=37%, county=28%, state=5.7%, federal=1.1%, NULL-level=7.6%) reflects the practical mix. **These numbers need to be re-run with the 2025-05-01 to 2026-12-31 window applied** — currently they are unwindowed.

**⚠ Code/data discrepancy worth flagging upstream:** the current SQL files for `int__civics_candidacy_ballotready.sql` and `int__civics_candidacy_techspeed.sql` (both in main) hardcode `cast(null as float) as viability_score`. Yet the prod intermediate tables show: TS intermediate has 58,580 / 59,870 = 97.8% populated, BR intermediate has 0 populated. Two possibilities:
1. The prod TS intermediate hasn't been rebuilt since the `null` hardcode landed (2026-02-09 in commit `61120ab1`), and a future rebuild will drop ~58k scores from the mart.
2. There is an unobserved code path (a separate notebook, an Airflow job, a hand-merge) that populates `viability_score` post-hoc on the TS intermediate.

**Either way the analysis should not assume the current mart coverage is stable.** If the EDA depends on `viability_score`, build a forward-compatible join path that goes directly to `int__techspeed_viability_scoring` keyed by `techspeed_candidate_code`. (Open: how does `users_win_candidacy` get from `user_id` to a TS candidate code? Likely via the BR position → TS candidacy crosswalk, but verify.)

### 4.4 Stability — is the score recomputed?

- `int__techspeed_viability_scoring` is materialized as a `table` (not a view). The model recomputes scores on each `dbt run` via the MLflow client — the latest registered version is fetched and applied. **No historical snapshot is preserved.**
- A given candidate's score can shift between materializations if the MLflow model is retrained, OR if their input features (incumbency, opponent count, etc.) are updated upstream.
- **Calibration implication:** for a "did candidates at score X win at rate Y" plot, you can only use the score as-of-mart-read, not as-of-prediction. If the model has been retrained against post-2025-election data, calibration on 2025 outcomes is potentially contaminated by knowledge of those outcomes (a leakage risk worth flagging).

### 4.5 Score distribution (NB-7a, from `int__techspeed_viability_scoring`)

| band | rows |
|---|---:|
| Frontrunner (≥4.0) | 33,373 |
| No Chance (<1.0) | 20,653 |
| Likely to Win (3.0–4.0) | 3,605 |
| Unlikely to Win (1.0–2.0) | 3,368 |
| Has a Chance (2.0–3.0) | 2,011 |
| (NULL) | 213 |

**Mean rating: 3.07; range 0.0 to 5.0.** The distribution is heavily bimodal — 86% of scored candidates land in either Frontrunner or No Chance, with only 14% spread across the three middle bands. This is consistent with a model that uses incumbency + open-seat + opponent-count as primary features: incumbent or uncontested → Frontrunner; challenger in a crowded field → No Chance. The bimodality has implications for any downstream stratification — "viability decile" buckets won't be evenly populated.

### 4.6 Component availability for candidacies WITHOUT a score

Even where `viability_score` is NULL, the underlying features are partially available:
- `is_incumbent`: on `candidacy` (TS-sourced; sparse on BR-only rows)
- `is_open_seat`: on `candidacy` (BR > TS > DDHQ)
- `is_partisan`: on `candidacy`
- `n_seats` / `n_candidates` / `log_n_losers`: reconstructible from `candidacy_stage` counts grouped by `gp_election_stage_id` (count candidacies per stage)
- WoE encodings for state/level/office_type: stable lookup tables in `model_predictions.viability_br_*_woe`

So the score could be **back-scored** for additional candidacies by joining their feature rows against the existing MLflow model. This is in-scope analytics work if needed, but the scout flags it as out-of-scope for the inventory deliverable.

### 4.7 Calibration on closed 2025 races

Calibration analysis (observed win rate by `viability_score` decile, restricted to candidacies with both a non-null score and a `latest_stage_result ∈ ('Won', 'Lost')`) is pending NB-7b. Expected challenges:

1. **Bimodality** of the score (4.5) means decile buckets will be unevenly populated — switching to quintile or to a coarser scheme aligned with the 5-band `score_viability_automated` may be more interpretable.
2. **Score overwriting** (4.4) means the calibration is against the *current* score, not the *prediction-time* score. Note this when interpreting.
3. **The TS-only 97.8% cohort dominates the rated population** — calibration will be effectively a TS-cohort calibration unless explicitly stratified.

---

## Source 5: Product DB beyond `users_win_base` / `users_win_candidacy`

**Paths:**
- `dbt/project/models/staging/airbyte_source/gp_api_db/` (raw stage of the product DB tables)
- `dbt/project/models/marts/civics/users.sql` (the upstream `users` model that feeds `users_win_base`)
- `dbt/project/models/marts/civics/campaigns.sql` (the upstream `campaigns` model with version tracking)
- `dbt/project/models/marts/analytics/users_win_base.sql`

This source covers the product-DB-side signals that don't make it into `users_win_candidacy`.

### 5.1 Field/event inventory

**Raw product DB tables (staging):** `user`, `campaign`, `campaign_position`, `organization`, `position`, `elected_office`, `election_type`, `poll`, `path_to_victory`, `outreach`, `ecanvasser` (+ contact/house/interaction), `tcr_compliance`, `top_issue`, `website` (+ `website_contact`, `website_view`), `domain`, `poll_individual_message`, `poll_issues`.

**Most relevant for outcomes analysis:**
- `stg_airbyte_source__gp_api_db_user.sql` — product users (raw user table)
- `stg_airbyte_source__gp_api_db_campaign.sql` — product campaigns including `details` JSON
- `stg_airbyte_source__gp_api_db_path_to_victory.sql` — possibly carries recommended-action / strategy data; **verify in notebook**
- `stg_airbyte_source__gp_api_db_outreach.sql` — voter outreach campaign data
- `stg_airbyte_source__gp_api_db_position.sql` — positions/offices the campaign maps to

### 5.2 Data quality and coverage

- `users_win_base.is_demo_only = (u.campaign_count > 0 and u.non_demo_campaign_count = 0)` — applies the demo filter at the user grain.
- `users_win_base` uses `users.is_win_user = true` as the filter (`users_win_base.sql:25`). `is_win_user = (organization_type = 'win')` is the authoritative source per `m_civics.yaml:87-92`.
- Pro identification: `campaigns.is_pro` is a column on the raw campaign table (confirmed at `users.sql:50`: `count(case when is_pro then 1 end) as pro_campaign_count`). `users_win_base.is_pro = (pro_campaign_count > 0)`. The Amplitude `pro_upgrade_complete` event gives the **timing**.
- Pledged: `campaigns.details:pledged::boolean` is parsed out of a JSON field (`users.sql:52`). Boolean.

### 5.3 Join keys

- `user.id` (product) ↔ `mart_civics.users.user_id` ↔ `mart_analytics.users_win_base.user_id` ↔ `int__amplitude_*.user_id`
- `campaign.id` (product) ↔ `mart_civics.campaigns.campaign_id` ↔ `users_win_candidacy.campaign_id` ↔ `candidacy.product_campaign_id`
- `user.id` ↔ `candidate.prod_db_user_id` (the civics-side candidate identity link; confirmed in `candidate.sql:46`)

### 5.4 Segmentation dimensions (product-DB-native)

- Registration cohort: `users_win_base.registered_at`, `registration_month`, `registration_week`, `registration_year`, `registration_quarter`
- Geography: `users.zip`
- Country: `int__amplitude_user_milestones.registration_country` (from the Amplitude event payload)
- **Registration channel / source:** not surfaced in `users_win_base`. Verify whether the product `user` table carries a `referrer` / `signup_source` / `utm_*` column in the raw stage. **Open question for notebook.**
- **Funnel: recommended actions:** the product DB has `path_to_victory` and `outreach` tables. `[NB-8]` characterizes whether either carries a structured step / task / recommendation schema.

### 5.5 Outcome variables → n/a

### 5.6 Viability score → n/a, see Source 4

### 5.7 Mapping to questions

- "Does Pro-tier show outcome lift?" → `is_pro` is identifiable (`users_win_base.is_pro`); timing of upgrade comes from `int__amplitude_user_milestones.pro_upgrade_completed_at`. **Watch reverse causation: highly engaged users are more likely to upgrade.**
- "How does time-to-first-meaningful-action vary?" → use `users_win_base.first_dashboard_viewed_at` minus `registered_at`; OR `first_campaign_sent_at` minus `registered_at` for a stricter "meaningful action" bar.
- "What fraction of recommended actions do users complete?" → depends on Source 5.4 open question; if `path_to_victory` carries structured task data, this question becomes answerable from product DB alone, supplemented by Amplitude for completion events.

---

## Source 6: L2 voter data — schema only

**Paths:**
- `dbt/project/models/intermediate/l2/int__l2_district_aggregations.sql`
- `dbt/project/models/intermediate/l2/int__l2_nationwide_uniform.py`
- `dbt/project/models/intermediate/l2/int__l2_nationwide_uniform_w_haystaq.py`
- `dbt/project/models/intermediate/l2/int__l2_nationwide_haystaq_flags.py`
- `dbt/project/models/intermediate/l2/int__l2_nationwide_haystaq_scores.py`
- `dbt/project/models/intermediate/l2/int__zip_code_to_l2_district.py`

L2 is in scope only at schema-only granularity in this scout. **No row-level queries.**

### 6.1 Field/event inventory (schema only)

- `int__l2_district_aggregations` — district-grain aggregates (voter counts, demographic rollups). Feeds `int__icp_offices.voter_count` via the join chain — the only L2 field currently surfaced into `users_win_candidacy` indirectly.
- `int__l2_nationwide_uniform` — nationwide voter file in uniform schema. **Row grain = individual voter. PII-adjacent (name, address-adjacent identifiers).** Do not pull row-level data without an explicit lawful use case.
- `int__l2_nationwide_uniform_w_haystaq` — same as above, joined with Haystaq partisanship/turnout scores. **Row grain = individual voter. PII-adjacent.**
- `int__zip_code_to_l2_district` — zip → district crosswalk. Aggregate-grain; not PII.

### 6.2 Data quality and coverage

- L2 uniform drift is a known operational concern; there's a dedicated triage skill `l2-uniform-drift-remediator` (referenced in user instructions) for handling schema drift between L2 source and the uniform model. If a longitudinal analysis tries to use L2-derived fields, expect schema-version mismatches across the time window.

### 6.3 Join keys

- District-grain: `int__icp_offices` joins via the BR position → L2 district crosswalk (LLM-matched in `stg_model_predictions__llm_l2_br_match_*`). Already surfaced through `users_win_candidacy.voter_count`, `l2_district_name`, `l2_district_type`.
- Voter-grain: NOT typically joined to candidates by L2 ID. There may be partial joins via address-matching for certain analyses; that's out-of-scope here.

### 6.4-6.6 Segmentation, outcomes, viability → n/a or already surfaced via `int__icp_offices`

### 6.7 Mapping to questions

L2 is **not needed** to answer any of the six questions the user posed in the original prompt. `voter_count` (already in `users_win_candidacy`) and `l2_district_type` are sufficient as district-context segmentation dimensions. If the EDA later needs to characterize the electorate in a candidate's district beyond what `int__icp_offices` surfaces, that's a follow-on inventory.

**PII flag:** the `*_uniform*` models carry voter-level records. Treat as restricted; only aggregate-grain queries appropriate for this analysis.

---

## Synthesis: mapping the analytical questions to data

For each of the six analytical questions the user posed, this table names the supporting tables, fields, joins, and the biggest data-quality risk. Outcome variable across all rows is the **`latest_stage_reached` + `latest_stage_result` pair** from `mart_civics.candidacy` (resolved scoping decision — captures candidates who lost at primary or earlier stages, not just general). Cycle window throughout: `general_election_date BETWEEN '2025-05-01' AND '2026-12-31'`. ICP = dimension (slice), not a filter.

| # | Question | Outcome / target field | Predictor source | Required joins | Biggest data-quality risk |
|---|---|---|---|---|---|
| 1 | Which product actions correlate with winning, controlling for office type and level? | `candidacy.latest_stage_result ∈ ('Won', 'Lost')` joined through `users_win_candidacy` on `campaign_id` → `product_campaign_id`. Optional Amplitude self-reported supplement: ~970 additional labels (Source 3.5). | `int__amplitude_win_activity_weekly` (per-week activity) + `int__amplitude_user_milestones` (lifecycle ts); broader event-family aggregates derivable from raw `stg_airbyte_source__amplitude_api_events` (Source 3.1a) | `user_id` | Only 2 event types are aggregated today; 312 more exist in the raw stream and can be aggregated in-scope (Source 3.8). |
| 2 | What fraction of recommended actions do users complete, and where's the drop-off? | None — funnel question | Raw `stg_airbyte_source__amplitude_api_events` filtered to funnel-relevant event types (Onboarding / Outreach / Content Builder / Profile / Pro Upgrade families — see Source 3.1a). New intermediate aggregates needed. | `user_id` | Funnel definition is a product decision, not a data limitation. Adding intermediate aggregates is in-scope analytics work. |
| 3 | Does Pro-tier show outcome lift over self-serve, holding candidate type roughly constant? | `candidacy.latest_stage_result` | `users_win_base.is_pro` (flag); `int__amplitude_user_milestones.pro_upgrade_completed_at` (timing) | `user_id` | **Raw Pro win-rate is LOWER than free** (39.8% vs 59.3% in-window). Reverse causation: Pro users are pre-selected for engagement. Need explicit pre-/post-upgrade frame + office-stratified analysis. |
| 4 | How does behavior change as a function of days-to-election? | n/a (descriptive); time-to-event predictor | `int__amplitude_win_activity_weekly.week_start_date`; per-stage dates from `users_win_candidacy` (`primary_election_date`, `general_election_date`, runoffs) | `user_id` | Pre-2023-12-10 users have no event history; users with multiple cycles need explicit cycle assignment; in-window 2026 candidates have election dates in the future (engagement to date is partial). |
| 5 | Are there meaningful user segments with distinct outcome patterns? | `candidacy.latest_stage_result` | Segmentation dims (Source 1.4) including `is_pro`, `icp_office_win`, `election_level`, `office_type`, `campaign_state`, `campaign_party`, `is_incumbent`, `is_open_seat`, `is_partisan` | candidacy join for `is_incumbent` / `is_open_seat` (latter NULL for BR-only) | 45.6% NULL `election_level` in window; small-cell counts in state × office_type slices; **ICP=true cohort has lower raw win rate (53.8%) than ICP=false (63.5%)** — slice carefully. |
| 6 | How does time-to-first-meaningful-action vary across users, and does it predict downstream engagement? | n/a (descriptive); for prediction: `int__amplitude_win_activity_weekly` activity at later periods | `int__amplitude_user_milestones.first_dashboard_viewed_at`, `first_campaign_sent_at`; `users_win_base.registered_at` | Definition of "meaningful action" matters — dashboard view is low-bar, campaign send is high-bar; user-week autocorrelation flagged in the churn SESSION_NOTES. |

**Cross-cutting completeness per question (in-window, 26,072 candidacies):**

| Field | Populated | % in-window | Use case |
|---|---:|---:|---|
| `latest_stage_result` (any value) | 15,826 | 60.7% | All outcome questions |
| `latest_stage_result ∈ ('Won', 'Lost')` | ~13,681 | ~52% | Binary win/loss questions |
| `general_election_result` | 11,329 | 43.5% | General-stage-only questions |
| `viability_score` | 3,281 | 12.6% | Viability stratification |
| `icp_office_win = true` (slice value) | 12,427 | 47.7% | Win-ICP slice |
| `is_pro = true` (treatment) | 1,086 | 4.2% | Pro vs free comparison |
| Amplitude `user_id` joined (any activity) | ~25k | ~96% | Engagement features |

### What does "successful use of Win" mean? (unresolved)

The six analytical questions above all assume outcome-only success (the candidate won). Mid-scout (2026-05-28), the framing surfaced as too narrow: product success may be some combination of engagement quality and outcome, and the actual definition has not been agreed with the product team.

The data supports at least four candidate definitions of "successful use":

1. **Outcome-only.** The candidate won. Treats engagement as a means.
2. **Engagement-only.** The candidate ran a "real" campaign per some engagement signature, regardless of outcome. Treats winning as largely outside product control (opponent quality, district lean, money).
3. **Engagement-and-outcome.** The candidate engaged meaningfully AND won. The strictest definition; "won despite zero product use" would not count as a product success.
4. **Engagement quality, outcome-stratified.** Report engagement signatures separately for winners, losers, and no-outcome candidacies, rather than collapsing into a single composite metric.

This is a product-side definitional question, not a data-side one. The data team can enumerate options and characterize what each requires; product needs to pick. Until that decision lands, any outcome-correlation analysis is implicitly choosing definition 1.

Two practical implications follow. First, this expands the recommended-actions section: a top item there is "agree on a definition of 'successful use of Win' with the product team." Second, engagement-side success analysis (definitions 2 or 4) is feasible today using the same Amplitude substrate documented in Sources 3.1 / 3.1a / 3.5, and could run in parallel to (or ahead of) outcome-correlation work.

## Biggest risks and gaps (call-out)

In rough order of impact on what the EDA can deliver:

1. **Existing Amplitude intermediate models cover ~4% of tracked event types.** The raw `stg_airbyte_source__amplitude_api_events` table carries 314 distinct event types in the analysis window; intermediate models aggregate only 12. **The recommended-action funnel question IS answerable** — see Source 3.1a and 3.8. Required work is new aggregation models over existing raw events, not new instrumentation.
2. **45.6% of in-window candidacies have NULL `election_level`** (11,891 rows). Outcome coverage in the NULL bucket is meaningful (50% have `latest_stage_result`), so dropping it loses real signal. Any analysis sliced by office level must decide: restrict to populated rows (slashes sample size to ~14k), backfill from `office_type` / `int__icp_offices`, or treat as a fifth category.
3. **Pro users show counterintuitively lower raw win rate (39.8%) than free users (59.3%) in window.** ICP=true users also show a lower win rate (53.8%) than ICP=false (63.5%). Both gaps narrow vs the unwindowed view but persist. Confounded by selection bias and unobserved race competitiveness; needs office-stratified analysis. **This is the most likely starting hypothesis to test rigorously in the EDA.**
4. **`viability_score` (Viability Score 2.0) is TS-primary with rich coverage, but the mart surfacing path is unstable.** The score is produced internally by `int__techspeed_viability_scoring.py` against the MLflow model `ViabilityWithOpponentData` (not BR-sourced as previously framed). Coverage in the mart by source bucket: 97.8% for TS-only candidacies, 56.3% for BR-matched, 4.4% for 2025 archive, 0% for gp_api-only or ddhq-only. **There is a code/data discrepancy** — the current intermediate SQL files set `viability_score = NULL` but prod tables carry scores; a future rebuild may drop these. For forward stability, the EDA should be prepared to join `int__techspeed_viability_scoring` directly (via `techspeed_candidate_code`). See Source 4.
5. **2025 archive vs 2026+ cycle shape mismatch.** Some fields (incumbency, open-seat) have different coverage by cycle. Cross-cycle longitudinal analyses risk silently averaging over structurally different cohorts.
6. **Outcome data is concentrated in 2025.** Only 5.4% of 2026 candidacies have a `general_election_result` yet (most 2026 elections haven't happened). For the foreseeable future, "did they win the general?" analyses are effectively a 2025 cohort study.
7. **Pre-2023-12-10 candidates have no Amplitude history.** Any engagement-features analysis must restrict to `users.created_at >= 2023-12-10` (or accept structural missingness).
8. **Multi-cycle candidates.** One `user_id` can produce multiple `campaign_version_id` rows across cycles. Unit-of-analysis decisions affect the answer. The mart already provides `is_latest_version` and `campaign_version_id` as the natural seams, but the analyst must pick.
9. **`candidacy_result` cross-stage fallback.** Convenient but contaminated. **For the resolved scope, use `latest_stage_reached + latest_stage_result` from `mart_civics.candidacy`** — it explicitly preserves the deepest stage the candidate reached and the result there. Documented at `m_civics.yaml:516-545` but easy to miss.
10. **Reverse causation on Pro.** Highly engaged users are more likely to upgrade. Pro-vs-free outcome comparisons need an explicit pre/post-upgrade frame to be causally meaningful.
11. **L2 PII boundary.** Voter-grain L2 models (`int__l2_nationwide_uniform*`) carry PII-adjacent records. Aggregate-grain queries only; voter-grain joins out of scope for this analysis.

## Recommended actions

Concrete follow-up work that the findings and gaps imply. Items are grouped by the kind of work and the team that would do it, not by priority. Each names the gap or risk it addresses and the analytical capability it unblocks. Sequencing is for the user and the relevant teams to decide.

### Data-platform actions (close coverage gaps)

| Action | Gap addressed | Unblocks |
|---|---|---|
| Wire `viability_rating_2_0` and `score_viability_automated` into `users_win_candidacy` via `int__techspeed_viability_scoring.techspeed_candidate_code` | Mart-level viability surfacing currently 12.6% on Win candidacies; the underlying TS scoring covers ~99.7% of TS-touched candidacies | Viability stratification on most Win candidacies; viability as input for outcome models |
| Resolve the viability code/data discrepancy in `int__candidacy_*` SQL | Intermediate dbt SQL hardcodes `viability_score = NULL` (commit `61120ab1`, 2026-02-09) while prod tables still carry historical scores; two analysts get different answers depending on layer | Stable forward path; removes the "which table do I trust" friction |
| Backfill or document missing `election_level` (45.6% NULL in window, ~11.9k rows) | Office-level slicing currently loses ~12k rows | All office-level cuts in the EDA |
| Build intermediate aggregates for additional Amplitude event families (Onboarding mid-funnel, Schedule Text Campaign, Content Builder, Pro Upgrade funnel, Outreach planning) | Only 12 of 314 instrumented event types are modeled today; raw events are in `stg_airbyte_source__amplitude_api_events` | Recommended-action funnel analysis (Source 3.8); broader feature-correlation work |
| Land a labeled outcome view fed by the Did-You-Win Modal payload (`event_properties:status`) | ~970 net new outcome labels live in Amplitude only | Outcome coverage on additional candidacies (preserving the response-selection caveat as a column on the view, not a footnote) |

### Methodology actions (cut data deliberately)

These are not data-engineering tasks but conventions that the analysis itself must follow. They belong in the EDA spec, not in dbt.

- **Per-feature exposure-window recipe.** For any feature-effect analysis: pin the feature's launch date, define a meaningful exposure window, restrict to candidacies whose election date sits after that window, then intersect with candidacies that have an outcome label.
- **Outcome-availability filter on top of exposure.** 2026 candidacies whose election has not yet occurred are out, regardless of feature usage. Restate this every time the cohort is described.
- **Quasi-experimental framing for any causal claim.** "Used vs didn't use" is self-selected, so the default output is correlation, not effect. To approach effect: cohort timing around launch dates, propensity weighting, or before-and-after stratification within the same user.
- **Self-report-bias handling for Did-You-Win-sourced labels.** Either restrict outcome correlations to civics-sourced labels, weight, or stratify on label source. The 80% self-reported win rate is a known artifact and should be treated as such in any chart or table.

### Product-team actions (resolve definitional questions)

- **Agree on a definition of "successful use of Win"** using the four-option enumeration in the synthesis section above. Without this decision, any outcome-correlation analysis is implicitly choosing definition 1 (outcome-only success).
- **Characterize Viability 2.0** as a prerequisite to using it for stratification or as an outcome predictor. Specifically: what labels was the `ViabilityWithOpponentData` MLflow model trained against, what features go in, how often is the score refreshed, and is calibration stable across office types? Likely owner is the politics-and-data team that owns the model.

### Followup analyses worth scoping

- **Engagement-side success analysis.** Using the weekly Amplitude substrate plus event-grain milestones, characterize engagement signatures (depth, breadth, recency) on the in-window cohort. Feasible today, runnable in parallel to outcome-correlation work, and serves the "we do not have to wait for product to define success in order to learn things" path.
- **Pre-vs-post Amplitude cohort comparison.** Quantify what we lose by restricting to `users.created_at >= 2023-12-10` (post-Amplitude cohort) versus accepting structural missingness on the pre-cohort rows. Inform the EDA decision on whether to drop pre-2023-12-10 candidacies entirely.

## Scoping decisions (resolved 2026-05-27)

These were originally open and have been resolved in conversation:

- **Unit of analysis**: candidacy (`gp_candidacy_id`).
- **Outcome row selection**: `latest_stage_reached + latest_stage_result` from `mart_civics.candidacy`. Captures candidates who lost at primary or earlier stages, not just the general.
- **Cycle restriction**: `general_election_date BETWEEN '2025-05-01' AND '2026-12-31'`. Applied at the candidacy grain.
- **Engagement window**: same as the candidacy/outcome window (2025-05 to 2026-12).
- **Baseline**: include all registered Win users with engagement-zero treated as a valid predictor value (i.e. engagement-zero is a row, not an exclusion).
- **ICP gating**: `icp_office_win` is a slicing dimension, not a filter. The analysis reports across all candidacies and stratifies by ICP=true / ICP=false / NULL.

## Verification

This inventory is "done" when:

1. ✅ One section per source covering the 7 facets, with file paths and column names cited (not just described).
2. 🟡 Coverage claims backed by either a dbt test (with file path) or a notebook cell (with cell number and result). **Partially complete** — NB-1, NB-2, NB-4 (election_level slice), NB-5 (cycle slice), NB-6 (election_level + is_pro slices) were run via `dbt show` during the scout pass; remaining cells (NB-3 candidacy_result fallback quantification, NB-6 state slice, NB-7 calibration, NB-8 path_to_victory schema, NB-9 broader Amplitude event scan, NB-10 join sanity) still need to be run from the notebook.
3. ✅ Each of the 6 analytical questions has a row in the synthesis section naming the supporting tables, fields, joins, and the biggest data-quality risk.
4. 🟡 Viability score section: coverage characterized; stability + components answered from SQL reads; **calibration analysis pending NB-7**.
5. ✅ Biggest-risks call-out lists at minimum: recommended-action funnel gap, BR-only viability coverage, 2025-archive shape, pre-2023-12-10 cohort, multi-cycle candidate handling. **Added during scout pass:** 61.5% NULL `election_level` finding; Pro users' counterintuitive lower raw win rate.

The remaining work is to run the rest of the notebook on Databricks to fill in NB-7 (calibration), NB-8 (path_to_victory schema), NB-9 (broader Amplitude event scan), NB-10 (join sanity).
