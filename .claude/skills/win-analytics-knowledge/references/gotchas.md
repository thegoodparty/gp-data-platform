# Known gotchas reference

Part of the **win-analytics-knowledge** skill. Recurring traps, as a quick-reference symptom table.

## Quick reference

- **What this is:** a symptom → mitigation index. When a trap is fully explained by its home doc (joins, outcomes, viability, etc.), this table carries the **symptom** and points there rather than restating the fact — so a fact lives in exactly one place.
- **How to use it:** scan the Symptom column for what you're seeing, then follow the mitigation's link for the full explanation.
- **Maintenance:** when you hit a new trap, add a one-liner here; put the full explanation in the owning domain doc.

## Routing triggers

- IF you see one of the symptoms below → apply the mitigation / follow its link.
- IF the trap is about a metric definition → confirm against [canonical_metrics.md](canonical_metrics.md).
- IF the trap is environment/tooling (pre-commit, venv) → see the repo-root `CLAUDE.md` (the owning doc).

## Gotchas

| Gotcha | Symptom | Mitigation |
|---|---|---|
| **`users_win_base.election_date = coalesce(next, last)`** | Leaks future election dates into "current cycle" analyses. | Use per-stage dates from `users_win_candidacy` instead. They are NOT coalesced and preserve cycle separation. |
| **`candidacy_result` cross-stage fallback** | A primary winner who lost the general can show `candidacy_result = 'Won'`. | Use `latest_stage_result` + `latest_stage_reached` (or `general_election_result`). See [outcomes.md](outcomes.md). |
| **Viability score code/data discrepancy** | `candidacy.viability_score` is populated in prod but the SQL files hardcode it to NULL; future rebuilds could drop ~58k scores. | Join `int__techspeed_viability_scoring` via `techspeed_candidate_code` for forward-stable access. See [viability.md](viability.md). |
| **Pre-2025-05-28 candidates have no Win-product Amplitude history** | Earlier `users.created_at` rows exist in the product DB but have no Win-Amplitude events (instrumentation went live ~2025-05-28; see [engagement.md](engagement.md)). | For engagement-window floors, use the **actual coverage `MIN(week_start_date)`** of the source table (currently 2025-06-23 for the weekly model), not a fixed date. Document the coverage window in the brief's `data_provenance` field. |
| **45.6% NULL `election_level` in window** | Office-level slicing loses half the population if you filter. | Decide explicitly: (a) restrict, (b) backfill from `office_type` / `int__icp_offices`, or (c) treat NULL as a fifth category. See [segmentation.md](segmentation.md). |
| **Pro users have LOWER raw win rate than free** | Naive Pro-lift analysis flips intuition. | Confounded by selection bias (Pro users self-select into harder races) and reverse causation (engagement → Pro). Office-stratify; use pre/post-upgrade frame for timing. |
| **ICP=true cohort has LOWER raw win rate than ICP=false** | Same direction as the Pro finding. | ICP=true offices are by-design competitive. Slice rather than filter. See [segmentation.md](segmentation.md). |
| **2025 archive vs 2026+ FOJ structural mismatch** | Field availability differs (e.g., `is_open_seat` absent from 2025 archive). | Slice by `source_systems` and `year(general_election_date)` together. Don't pool blindly. See [sources.md](sources.md). |
| **Outcome data concentrated in 2025** | 2026 cycle has <10% outcome coverage. | "Did they win" analyses are effectively a 2025 cohort study for now. 2026 holdout is limited. |
| **`votes_received` is STRING** | Casting fails on rows containing "uncontested". | Filter or coalesce the literal before casting. See [outcomes.md](outcomes.md). |
| **Multi-cycle candidates** | One user can produce multiple `campaign_version_id` rows. | Filter to `is_latest_version` for the canonical row; collapse cycles explicitly. See [joins.md](joins.md). |
| **Amplitude self-reported outcome response bias** | 80% self-report "won" vs 64% raw population win rate. | Use labels, NOT rates. See [outcomes.md](outcomes.md). |
| **L2 PII boundary** | Voter-grain L2 models carry PII-adjacent records. | Aggregate-grain queries only by default. See [sources.md](sources.md). |
| **Mart staleness vs SQL state** | Prod mart may carry data the current SQL doesn't produce (e.g., viability). | When code/data disagree, trust the data for current state but flag the discrepancy. Don't extrapolate forward. |
| **VIRTUAL_ENV breaks pre-commit pytest hook** | `Executable pytest not found` on commit when an analytics venv is active. | See the pre-commit section of the repo-root `CLAUDE.md` (the owning doc). |
| **`users_win_candidacy.hubspot_id` is the COMPANY id, not the contact id** | Joining `stg_airbyte_source__hubspot_api_feedback_submissions.hs_contact_id` directly to `users_win_candidacy.hubspot_id` returns zero matches. | Use the 2-hop via candidate. See [joins.md](joins.md). |
| **HubSpot PMF Option 1/2 labels obscured in export** | `pmf_response` returns literal `"Option 1"` / `"Option 2"` instead of the disappointment labels. | Treat **Option 1 = Very disappointed, Option 2 = Somewhat disappointed**. See [outcomes.md](outcomes.md). |
| **Onboarding milestone flags are new-flow-blind** | `onboarding_complete` / `has_completed_onboarding_flow` / `is_onboarded` / the lib `onboarded` flag read FALSE for post-cutover users, so a cross-cutover analysis shows a fake completion collapse. | Recompute **onboarding dashboard viewed** (broad) or `Onboarding - Candidate Pledge Completed` (strict) — both version-agnostic. Mechanism, dates, and the verified 0.0% read are in [engagement.md](engagement.md). |
| **Account-creation column name** | `users_win_base.user_created_at` does NOT exist. | Account creation = `users_win_candidacy.user_created_at` (= `users_win_base.registered_at`, byte-identical). |
| **Civics classification lag on recent signups** | Recent registrants carry NULL `election_level` / `office_type = 'Other'` until the civics mart classifies them, so office/level cuts on a recent cohort collapse. | Treat office/level stratification as a structural finding on the older classified cohort; don't read a recent pre/post office cut. |
| **Amplitude coverage can lead the calendar date** | `MAX(event_time)` may be ahead of "today" (e.g. 2026-06-06 seen on 2026-06-02). | Compute `data_max = MAX(event_time)` live and derive censoring cutoffs from it; never hardcode "today" or a stale max. |
| **Closed-cohort retention declines by construction** | An election-anchored or signup-anchored closed cohort (membership fixed by first-active week) decays toward the anchor the way any product's weekly retention decays, and gets misread as the base disengaging as the election nears. On Win, the same dashboard-view metric falls in the closed-cohort curve but rises on an open view (8.6% to 15.2% for Nov-2025 generals). | When a retention curve could be read as aggregate engagement, pair it with an **open fixed-denominator** view: numerator = users active that week, denominator = the full population held constant. The closed curve answers whether early adopters stay sticky; the open curve answers whether the base is more engaged near the election. For the Win run-up question specifically, the recommended approach is to lead with the open view and then split it into **new** (first-ever active that week) versus **returning** (active this week and active in some earlier week), both on the fixed denominator. The returning line is the cleanest reverse-retention read: it separates real re-engagement from new signups, since `new% + returning%` equals the open active share. On Nov-2025 generals the returning share rose 3.6% to 10.8% while new stayed roughly flat near 5%, so the run-up was repeat engagement, not acquisition. Reference build: `analytics/ad_hoc/2026-06-02_win_retention_curve.ipynb`. |

## Cross-references

- Domain docs own the full explanations: [sources.md](sources.md), [joins.md](joins.md), [outcomes.md](outcomes.md), [engagement.md](engagement.md), [viability.md](viability.md), [segmentation.md](segmentation.md).
- The repo-root `CLAUDE.md` owns the environment/pre-commit traps.
