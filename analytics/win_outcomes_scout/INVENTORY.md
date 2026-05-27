# Win product × electoral outcomes — data inventory

A scouting map of the data available to study whether Win product activity correlates with electoral outcomes. Produced before scoping the real EDA, this document characterizes what exists, where it's reliable, how sources join, and what would trip up a longitudinal analysis.

Companion notebook (`notebooks/inventory_queries.ipynb`) carries the queries that produced the coverage statistics cited inline. Each numeric claim points to a notebook cell. Run the notebook on Databricks (see `WORKFLOW.md` pattern in `analytics/win_churn/` for the local-edit / Databricks-execute setup).

## How to read this document

- Sources are ordered by where the work happens: the keystone analytics mart first (most of the work lives here), then the upstream civics mart that feeds it, then Amplitude (engagement), then the product DB layer beyond what the marts surface, then viability score as a dedicated cross-cutting field, then L2 at schema-only granularity.
- Each source section covers the seven facets the user enumerated. Where a facet doesn't apply to a source it is marked "n/a, see [X]".
- The synthesis at the end maps the six analytical questions to supporting tables, fields, and the biggest data-quality risk per question.
- Coverage statistics are written as `[NB-N]` where N is a notebook cell; values are filled in after the notebook is run on Databricks.

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

**Observed coverage (notebook NB-1, NB-2, populated 2026-05-26 via `dbt show`):**
- Total rows: 105,311
- Latest-version rows: 61,905
- Non-demo latest-version rows: **59,693** (this is the working population for outcome analysis — use as default)
- Distinct `user_id`: 61,748
- Distinct `campaign_id`: 61,905 (very close to user count — most users have a single campaign; multi-cycle is the rule, not the exception, in the *campaign version* sense, but multi-campaign-per-user is rare)
- Rows with at least one stage outcome captured: 14,582 (24.4% of non-demo latest)
- `general_election_result` populated: 14,279 (23.9%)
- `viability_score` populated: 10,210 (17.1%)
- `win_number` populated: 25,038 (42.0%) — **note: `win_number` populated more often than `viability_score`. Both are BR-only fields, but BR populates them on different cadences.**

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
- **Office level**: `election_level`. Observed values + counts (NB-4, non-demo latest):

| election_level | rows | viability populated | general result populated |
|---|---:|---:|---:|
| **NULL** | **36,717 (61.5%)** | 7.6% | 13.3% |
| city | 18,179 | 37.3% | 50.7% |
| county | 1,952 | 28.1% | 8.0% |
| state | 1,689 | 5.7% | 0.7% |
| federal | 1,156 | 1.1% | 0.1% |

**⚠ Surprise finding: 61.5% of candidacies have NULL `election_level`.** Investigating this NULL bucket is a prerequisite for any analysis sliced by office level — these 36,717 rows include candidacies where the position couldn't be mapped to a level upstream. They are *not* the candidacies with general election results either (only 13.3% of the NULL bucket has a general result captured), suggesting the NULL bucket is dominated by candidacies that never reached / never had outcome data ingested. Verify in notebook before deciding whether to (a) restrict to non-NULL, (b) drop, or (c) backfill via a different source (e.g. join to `int__icp_offices` or extract from `office_type`).

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

**Observed coverage (NB-5, non-demo latest, slicing on `year(general_election_date)`):**

| cycle | rows | viability populated | general result populated | won | lost |
|---|---:|---:|---:|---:|---:|
| 2025 | 19,831 | 29.0% (5,759) | 69.3% (13,738) | 8,763 | 4,490 |
| 2026 | 9,697 | 5.1% (494) | 5.4% (525) | 325 | 194 |
| 2027 | 969 | 0% | 0% | 0 | 0 |
| 2028 | 75 | 0% | 1 | 0 | 1 |
| 2030 | 1 | 0% | 0% | 0 | 0 |
| (NULL election_date) | balance to 59,693 | — | — | — | — |

**Interpretation:**
- 2025 is the most-complete cycle for outcomes (69.3% have a general result), which makes sense — 2025 elections have largely concluded.
- 2026 has very low outcome coverage (5.4%) because most 2026 elections haven't happened yet.
- 2027+ have effectively no outcomes — they're future races.
- The remaining 29,121 candidacies have NULL `general_election_date` — these are candidacies without a captured general date (could be primary-only, withdrawn, or position-only registrations).
- **2025 viability coverage is higher (29%) than 2026 (5%)** — counterintuitive given the 2025 archive structure. Confirms that the HubSpot archive does carry viability for many 2025 candidacies, contradicting the planning-time worry that the 2025 archive would lose viability. Worth a closer look.

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

n/a — Amplitude carries engagement, not electoral outcomes.

### 3.6 Viability score → n/a

### 3.7 Mapping to questions

- "Which product actions correlate with winning?" → only `Voter Outreach - Campaign Completed` and `Dashboard - Candidate Dashboard Viewed` are individually addressable. Other actions are lumped — or are missing from instrumentation entirely. **This is a meaningful gap.**
- "What fraction of recommended actions do users complete, and where's the drop-off?" → **Likely not answerable with current Amplitude instrumentation.** The closest proxy is the milestones model: Registration → Dashboard → Campaign Sent → Pro Upgrade is a coarse 4-stage funnel. If the user wants a recommended-actions funnel proper, look at the product DB (Source 4) for any `campaign_planner`-style step table, or flag a new instrumentation ask.
- "How does behavior change as a function of days-to-election?" → join weekly activity to per-stage dates on `users_win_candidacy`, compute `general_election_date - week_start_date`, bucket. Watch the 2023-12-10 floor (pre-floor users are missing).
- "Time-to-first-meaningful-action" → use `min(first_dashboard_viewed_at, first_campaign_sent_at)` from milestones; anchor to `users_win_base.registered_at`.

### 3.8 Recommended-action funnel (call-out)

The user's original question included "what fraction of recommended actions do users complete, and where's the drop-off?" — singled out here because **the instrumentation doesn't directly support it**:

- `int__amplitude_win_activity` has 2 event types.
- `int__amplitude_user_milestones` adds 5 more for Win (registration, dashboard, onboarding_complete, campaign sent, pro upgrade) — coarse lifecycle, not recommended actions.
- The product DB may have a structured task/step table (`campaign_planner_step` or similar) — see Source 4 below.

**Recommended action for the user before scoping the EDA:** decide whether the question is (a) redefined as lifecycle-milestone completion (answerable today), (b) instrumented properly with a new Amplitude event family (multi-week ask), or (c) reconstructed from the product DB if a structured task table exists (verify in notebook).

---

## Source 4: Viability score — dedicated characterization

The user asked for this field to be characterized specifically, not used as a primary outcome or control. Treated here as a cross-cutting field rather than a source.

### 4.1 Where the score lives

- **`candidacy.viability_score`** (and `candidacy.win_number`, `candidacy.win_number_model`). Sourced exclusively from `int__civics_candidacy_ballotready` per `candidacy.sql:141`. The columns are documented at `m_civics.yaml:665-672` as "Model-generated viability rating" / "Estimated votes needed to win" / "Model version used for win number calculation".
- Surfaced in `users_win_candidacy.sql:86-87` for Win users.

### 4.2 Is there a non-ML rule-based version?

The user mentioned a politics-team-drafted score with ~5 inputs (incumbency, contested race, open seat, partisan/nonpartisan, seat/opponent ratio). After reading the marts:

- The score column on the candidacy mart is **BR-sourced**. BR computes it externally; we receive it as a number.
- The inputs the user listed map onto fields the candidacy mart already carries: `is_incumbent`, `is_open_seat`, `is_partisan` (the seat/opponent ratio and "contested race" would need to be reconstructed from `candidacy_stage` counts per `gp_election_stage_id`).
- There is no internal model in this repo that recomputes the viability score from the inputs. `int__techspeed_viability_scoring.py` (at `dbt/project/models/intermediate/techspeed_to_hubspot/`) is a **different** model — it scores TechSpeed inbound leads using an MLflow model registered at `goodparty_data_catalog.model_predictions` for HubSpot routing decisions, not the politics-team electoral viability. Do not conflate.

**Implication:** if the user wants to characterize what drives the viability score, that work happens upstream of this repo (in BR's documentation), or by regressing the score against the underlying fields in our own data.

### 4.3 Coverage

**Observed coverage (NB-6):**

Coverage by `election_level` — already shown in Source 1.4 table. Headline: **17.1% overall coverage, with city = 37.3%, county = 28.1%, state = 5.7%, federal = 1.1%, NULL-level = 7.6%.**

Coverage by cycle (NB-5):
- 2025 cycle: 29.0% have a `viability_score`
- 2026 cycle: 5.1%
- 2027+: 0%

Coverage by `is_pro`:

| is_pro | rows | viability populated | win rate among rows with general result |
|---|---:|---:|---:|
| False | 55,951 | 17.4% | 64.3% (8,885 / 13,818) |
| (NULL — no campaign join) | 2,361 | 0.2% | 67% (2 / 3) |
| True | 1,381 | 32.0% | **45.6% (209 / 458)** |

**⚠ Surprise: Pro users show a LOWER raw win rate (46%) than free users (64%).** This is the strongest immediate finding from this scout. Important caveats before reading any signal into it:

1. **Selection bias.** Pro users self-select. They likely run for higher-stakes races (state, federal, contested cities) where the competition is fiercer. Free users may be skewed toward low-competition local races (school board, town council) where the field is small or uncontested.
2. **Reverse causation.** Pro upgrade is downstream of high engagement — but high engagement may also be a proxy for "candidate took the campaign seriously" which is itself correlated with running in a tougher race.
3. **`general_election_result` is the denominator.** The Pro bucket has 458 rows with a general result vs 13,818 for free users — Pro is a small population. Confidence intervals on the 46% will be wide.
4. **Office-level controls flip the sign?** Need to verify by stratifying. The headline number is meaningless without controls.

**This is the kind of finding the EDA exists to interpret carefully.** Flagged here as a starting hypothesis to test, not a conclusion.

**Other coverage cuts (deferred to notebook run):** by state, by candidate type (incumbent / open-seat). See NB-6 for the by-state slice.

**Initial expectation reconciliation:** the plan predicted "coverage is bounded by BR's coverage, federal/state better covered than local". Observed data flips this: **city has highest coverage (37%), federal has lowest (1.1%).** Likely explanation: BR's viability model is trained on the kind of local race where Win operates; federal races may not be in BR's scoring scope at all. Worth confirming with BR's documentation.

### 4.4 Stability — is it recomputed?

- `candidacy.viability_score` is a passthrough column from `int__civics_candidacy_ballotready`. That intermediate model is materialized as the dbt default (likely a view or non-incremental table — confirm in `dbt_project.yml`).
- BR refreshes the score externally; we re-ingest on each `dbt run`. There is **no historical snapshot** of viability score in this repo — once BR updates the score, the previous value is lost from the mart.
- For a calibration analysis ("do candidates at score X win at rate Y?") you can only use the **current** score against the realized outcome, not the score-as-of-a-historical-date. **This is a meaningful limitation.**

### 4.5 Component availability

Even where `viability_score` is NULL, the underlying inputs are partially available:
- `is_incumbent`: on `candidacy` (TS-sourced)
- `is_open_seat`: on `candidacy` (BR > TS > DDHQ)
- `is_partisan`: on `candidacy`
- Seat/opponent ratio: reconstructible from `candidacy_stage` counts grouped by `gp_election_stage_id`
- "Contested race": derive from opponent count > 1

So the score could in principle be reconstructed for additional candidacies — but only if (a) the user has BR's scoring methodology, or (b) the user is willing to fit a regression to learn the scoring rule. Both are out-of-scope for this scout but worth flagging.

### 4.6 Calibration (historical fit)

`[NB-7]` will compute observed win rate by `viability_score` decile, on candidacies where both the score and a `general_election_result ∈ ('Won', 'Lost')` are populated. Expected: calibration is reasonable on 2024 cycle data (where BR has had time to refresh against actual outcomes); 2026 calibration only meaningful post-election.

**Calibration risk:** because the score is overwritten with each BR refresh, the score-at-time-of-prediction may differ from the score-at-time-of-mart-read. If BR refreshes after results come in, the score is contaminated by knowledge of the outcome. Worth flagging when interpreting calibration plots.

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

For each of the six analytical questions the user posed, this table names the supporting tables, fields, joins, and the biggest data-quality risk.

| # | Question | Outcome / target field | Predictor source | Required joins | Biggest data-quality risk |
|---|---|---|---|---|---|
| 1 | Which product actions correlate with winning, controlling for office type and level? | `users_win_candidacy.general_election_result` | `int__amplitude_win_activity_weekly` (per-week activity) + `int__amplitude_user_milestones` (lifecycle ts) | `user_id` | Only 2 event types individually addressable; richer action detail missing from instrumentation |
| 2 | What fraction of recommended actions do users complete, and where's the drop-off? | None — funnel question | Coarse proxy: `int__amplitude_user_milestones` (Registration → Dashboard → Onboarding → Campaign Sent → Pro). True product DB: `stg_airbyte_source__gp_api_db_path_to_victory` (verify) | `user_id` | **Instrumentation gap.** May not be answerable as posed. Decide between coarse-milestone redefinition, new instrumentation, or product-DB reconstruction |
| 3 | Does Pro-tier show outcome lift over self-serve, holding candidate type roughly constant? | `users_win_candidacy.general_election_result` | `users_win_base.is_pro` (flag); `int__amplitude_user_milestones.pro_upgrade_completed_at` (timing) | `user_id` | Reverse causation: Pro users are pre-selected for engagement. Need explicit time-of-upgrade vs time-of-election framing |
| 4 | How does behavior change as a function of days-to-election? | n/a (descriptive) | `int__amplitude_win_activity_weekly.week_start_date`; `users_win_candidacy.general_election_date` | `user_id` | Pre-2023-12-10 users have no event history; election_date NULL for between-cycles users; users with multiple cycles need explicit cycle assignment |
| 5 | Are there meaningful user segments with distinct outcome patterns? | `users_win_candidacy.general_election_result` | Segmentation dims (Source 1.4) | n/a or candidacy join for `is_incumbent` / `is_open_seat` | 2025 archive vs 2026+ cycle shape mismatch; small-cell counts in fine office/state slices |
| 6 | How does time-to-first-meaningful-action vary across users, and does it predict downstream engagement? | n/a (descriptive); for prediction: `int__amplitude_win_activity_weekly` activity at later periods | `int__amplitude_user_milestones.first_dashboard_viewed_at`, `first_campaign_sent_at`; `users_win_base.registered_at` | Definition of "meaningful action" matters — dashboard view is low-bar, campaign send is high-bar; user-week autocorrelation flagged in the churn SESSION_NOTES |

## Biggest risks and gaps (call-out)

In rough order of impact on what the EDA can deliver:

1. **No structured recommended-action funnel in current instrumentation.** Amplitude tracks 2 event types in the activity model + 6 Win lifecycle milestones. There is no per-action completion table. Question #2 may need to be redefined or scoped out.
2. **61.5% of non-demo latest-version candidacies have NULL `election_level`.** Any analysis sliced by office level must decide how to handle this bucket: restrict to populated rows (slashes sample size to ~23k), backfill from `office_type` / `int__icp_offices`, or treat as a fifth category. **Surfaced during the scout pass — verify in notebook before scoping.**
3. **Pro users show counterintuitively lower raw win rate (46%) than free users (64%).** Confounded by selection bias and unobserved race competitiveness; needs office-stratified analysis. **Surfaced during the scout pass — this is the most likely starting hypothesis to test rigorously in the EDA.**
4. **`viability_score` is BallotReady-only, with no historical snapshot.** Coverage bounded by BR's coverage (17% overall, with strongest coverage at city level not federal). Value gets overwritten on each refresh — calibration analyses on closed cycles are still informative but represent the score "at the time the mart materialized", not "at the time of prediction".
5. **2025 archive vs 2026+ cycle shape mismatch.** Some fields (incumbency, open-seat) have different coverage by cycle. Cross-cycle longitudinal analyses risk silently averaging over structurally different cohorts.
6. **Outcome data is concentrated in 2025.** Only 5.4% of 2026 candidacies have a `general_election_result` yet (most 2026 elections haven't happened). For the foreseeable future, "did they win the general?" analyses are effectively a 2025 cohort study.
7. **Pre-2023-12-10 candidates have no Amplitude history.** Any engagement-features analysis must restrict to `users.created_at >= 2023-12-10` (or accept structural missingness).
8. **Multi-cycle candidates.** One `user_id` can produce multiple `campaign_version_id` rows across cycles. Unit-of-analysis decisions affect the answer. The mart already provides `is_latest_version` and `campaign_version_id` as the natural seams, but the analyst must pick.
9. **`candidacy_result` cross-stage fallback.** Convenient but contaminated. **Prefer `general_election_result` for binary win/loss questions.** Documented at `m_civics.yaml:516-545` but easy to miss.
10. **Reverse causation on Pro.** Highly engaged users are more likely to upgrade. Pro-vs-free outcome comparisons need an explicit pre/post-upgrade frame to be causally meaningful.
11. **L2 PII boundary.** Voter-grain L2 models (`int__l2_nationwide_uniform*`) carry PII-adjacent records. Aggregate-grain queries only; voter-grain joins out of scope for this analysis.

## Open scoping questions for the real EDA

These don't block the scout but should be resolved when scoping the EDA:

- **Unit of analysis**: candidacy / candidate / user? Multi-cycle candidates appear at multiple grains.
- **Outcome row selection**: when a candidacy has primary + general + runoff stages, which row defines "the outcome"? `general_election_result` is the proposed default but biases toward candidates who reached the general.
- **Cycle restriction**: 2024 + 2026 only, or include 2025 archive rows?
- **Engagement-zero candidates**: include as "no product use" or exclude?
- **ICP gating**: apply `icp_office_win` at the candidacy level, or report unfiltered first to characterize the broader population?
- **Time-of-engagement window**: trailing N days before election? Trailing N days since registration? Both?

## Verification

This inventory is "done" when:

1. ✅ One section per source covering the 7 facets, with file paths and column names cited (not just described).
2. 🟡 Coverage claims backed by either a dbt test (with file path) or a notebook cell (with cell number and result). **Partially complete** — NB-1, NB-2, NB-4 (election_level slice), NB-5 (cycle slice), NB-6 (election_level + is_pro slices) were run via `dbt show` during the scout pass; remaining cells (NB-3 candidacy_result fallback quantification, NB-6 state slice, NB-7 calibration, NB-8 path_to_victory schema, NB-9 broader Amplitude event scan, NB-10 join sanity) still need to be run from the notebook.
3. ✅ Each of the 6 analytical questions has a row in the synthesis section naming the supporting tables, fields, joins, and the biggest data-quality risk.
4. 🟡 Viability score section: coverage characterized; stability + components answered from SQL reads; **calibration analysis pending NB-7**.
5. ✅ Biggest-risks call-out lists at minimum: recommended-action funnel gap, BR-only viability coverage, 2025-archive shape, pre-2023-12-10 cohort, multi-cycle candidate handling. **Added during scout pass:** 61.5% NULL `election_level` finding; Pro users' counterintuitive lower raw win rate.

The remaining work is to run the rest of the notebook on Databricks to fill in NB-7 (calibration), NB-8 (path_to_victory schema), NB-9 (broader Amplitude event scan), NB-10 (join sanity).
