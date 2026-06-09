# Engagement & Amplitude event landscape reference

Part of the **win-analytics-knowledge** skill. Owns the detail for the engagement/activation
metrics defined in [canonical_metrics.md](canonical_metrics.md), plus the Amplitude event
stream they're computed from.

## Quick reference

- **Business context:** how engaged a Win candidate is — from "appears in Amplitude" through onboarding, dashboard activity, and outreach activation (the OKR metrics).
- **Entity grain:** stored metrics are user-grain (`users_win_base`); the raw stream is event-grain; the Win activity intermediates are user×month (and user×week).
- **Standard hygiene filter:** for candidate-attributed analysis, exclude anonymous/auto-track families (`[Amplitude]%`, `Scroll Depth`, `session_*`) and require a numeric `user_id`.

## Routing triggers

- IF the question asks for a governed metric's definition (Active, Activated, Onboarded, etc.) → [canonical_metrics.md](canonical_metrics.md); the caveats are below.
- IF the analysis spans the **2026-05-07 onboarding cutover** → use version-agnostic anchors (see Onboarding flow versions); do NOT use `is_onboarded` / `has_completed_onboarding_flow`.
- IF you need to classify raw `event_type` values into product families → use the `int__amplitude_event_taxonomy` model / `amplitude_event_family` macro (taxonomy below).
- IF the question is about acquisition channel / UTM → see Channel / UTM below.
- IF the question is about self-reported win/loss from the Did-You-Win modal → that's an outcome; see [outcomes.md](outcomes.md).

## Canonical engagement metrics

Governed definitions live in [canonical_metrics.md](canonical_metrics.md) (Active Candidates,
Activated Candidates, Onboarded cohort, Onboarding pledge completion, Outreach intensity,
Amplitude coverage). They are stored on `mart_analytics.users_win_base` (per
`m_analytics.yaml:152-247`); default to them before defining new ones. The notes below are the
caveats and deprecations the one-line registry definitions don't carry.

- **`has_amplitude_data`** misses the broader onboarding-progress family (`Onboarding -%` steps), so it materially **undercounts** true coverage (Nov-2025 cohort: 36% by this flag vs ~79% with ≥1 raw candidate-attributed event). Don't use it as a funnel top — compute "any Win evidence" from the raw stream instead.
- **`is_post_amplitude_registration`** (`user_created_at >= 2023-12-10`, the documented Amplitude tracking start) is **too loose** for engagement analyses — Win-product event instrumentation actually started ~2025-05-28.
- **`has_completed_onboarding_flow`** (`onboarding_completed_at IS NOT NULL`, event `onboarding_complete`) and **`is_onboarded`** (US user, dashboard viewed within 14d of *Amplitude registration*) are both **⚠ NEW-FLOW-BLIND** across the 2026-05-07 cutover. Deprecated as cohort filters — use the canonical **Onboarded** metric instead.
- **`is_active_candidate_7d/_30d/_90d`** are computed against `current_date`; recompute against an anchor date for retrospective analyses.
- **`is_activated`** is a lifetime flag (true if the user *ever* sent an outreach campaign); anchor it to `first_campaign_sent_at <= election_date` for forward/recent cohorts so post-election sends don't leak. Do not conflate with the colloquial "activated."
- **Onboarding completed (pledge)** is version-agnostic across both eras but first-seen 2025-09, so it under-counts pre-2025-09 cohorts.

### Terminology

Two distinct onboarding concepts — keep them named separately so they stop drifting across analyses:
- **"onboarding dashboard viewed"** (canonical **Onboarded**) = viewed the candidate dashboard within 14 days of account creation (the broad cohort filter).
- **"onboarding completed (pledge)"** = fired `Onboarding - Candidate Pledge Completed` within 14 days (the strict funnel metric).

Both are recomputed from durable, version-agnostic events; do NOT use the stored `is_onboarded` / `has_completed_onboarding_flow` flags across the 2026-05-07 cutover (both new-flow-blind, see Onboarding flow versions below).

### Point-in-time caveat for retrospective cohorts

`has_completed_onboarding_flow` and `is_activated` are lifetime-to-now flags (true if the user *ever* hit the milestone), like `is_active_candidate_*`. For a retrospective cohort analysis anchored to a past election, recompute them from the raw stream restricted to events before the anchor, so post-anchor activity doesn't leak into the funnel. (2026-06-01: the magnitude was immaterial for the Nov-2025 cohort — 21.2% as-of-today vs 20.9% anchored — but the principle holds and matters more for recent cohorts whose anchor is close to today.)

### "Any evidence" vs "engaged beyond account creation"

Raw "any Win-product Amplitude evidence" (≥1 candidate-attributed event) reads high (~79% for Nov-2025) but is heavily padded by the one-off registration event `Onboarding - User Created`: ~45% of that cohort had *only* that single event and nothing else. When characterizing engagement, report **"engaged beyond account creation"** (≥2 distinct candidate-attributed events, ~34% for Nov-2025) alongside raw any-evidence — the former is the meaningful floor, the latter is barely above "registered."

## Amplitude event landscape

### The modeling layers

| Layer | Model | What it covers |
|---|---|---|
| Staging (raw) | `stg_airbyte_source__amplitude_api_events` | **Full universe.** ~300+ distinct `event_type` values in any reasonable window. |
| Staging (catalog) | `stg_airbyte_source__amplitude_api_events_list` | Amplitude's own event catalog (name, totals, hidden flags). |
| Intermediate (Win) | `int__amplitude_win_activity` (monthly grain). A weekly variant (`_weekly`) now exists in prod `dbt` (coverage week_start_date 2025-06-23+); verify the live catalog before use. | **Only 2 event types** (the recurrent-activity allowlist, authoritative in the `amplitude_event_is_recurrent` macro): `Voter Outreach - Campaign Completed`, `Dashboard - Candidate Dashboard Viewed`. One-off lifecycle milestones live in `int__amplitude_user_milestones`. |
| Intermediate (lifecycle) | `int__amplitude_user_milestones` | ~12 lifecycle milestones (registration, dashboard, onboarding complete, campaign sent, pro upgrade, Serve onboarding). One row per user. |
| Intermediate (Serve) | `int__amplitude_serve_activity` | Filters `Viewed` event to `event_properties:path = '/dashboard/polls'` + `user_properties:Serve Activated = true`. |
| Mart passthrough | `mart_analytics.amplitude_events` | Thin alias of the staging events for Sigma BI consumption. No transformation. |

### Universe vs modeled — the headline finding

The dbt intermediates aggregate only **~4% of distinct event types** (12 of ~300). Most "bulk" event volume is auto-instrumented anonymous traffic (`[Amplitude] *`, `Scroll Depth`, `session_*`) with no `user_id`. After excluding anonymous noise, the modeled fraction of **candidate-attributed** events is ~21%.

**Implication:** rich product instrumentation exists across the Win product (Onboarding, Outreach, Content Builder, Pro Upgrade, Profile, AI Assistant, etc.) and is NOT aggregated. New funnel / engagement analyses can build their own aggregates over the raw stream — this is in-scope analytics work, not a new instrumentation ask.

### Event family taxonomy

Membership is **not** maintained here. `is_win` (Win membership) and `first_seen_date` (drift) live in the dbt model `int__amplitude_event_taxonomy`; the LIKE/IN patterns live in the `amplitude_event_family` macro (`dbt/project/macros/amplitude_event_taxonomy.sql`). Read those for the authoritative set — do not infer membership from the index below. This list is a human-readable index of what families exist and how to read them; it deliberately omits the patterns (the macro owns them) and per-family first-seen dates (the model's `first_seen_date` owns them, governed by the analysis `drift_cutoff`, default `2026-01-01`).

**Win families** (`is_win = true`, prefixed `win_`): `win_onboarding`, `win_dashboard`, `win_voter_outreach`, `win_outreach_planning`, `win_outreach_scheduling`, `win_content_builder`, `win_voter_data`, `win_candidate_profile`, `win_pro_upgrade`, `win_p2p_upgrade`, `win_candidate_website`, `win_candidacy_self_report` (the Did-You-Win modal — see [outcomes.md](outcomes.md) for outcomes use), `win_compliance_or_planning`, `win_ai_assistant`, `win_briefings`, `win_contacts`, `win_resources`. Families first seen after the cutoff (e.g. `win_briefings`, and `Dashboard - Campaign Plan Viewed` within `win_dashboard`) fall out as drift unless you widen `drift_cutoff`.

**Non-Win families** (`is_win = false`): `serve` (Serve product, covered in the Serve runbook), `auth_or_settings`, `navigation` (cross-product); `viewed_generic`, `amplitude_autotrack`, `session_or_browser` (noise — skip for candidate-attributed analysis); `experiment_assignment` (noise, but usable as an A/B-exposure stratification covariate). Anything unmatched falls through to `other`.

### Instrumentation start date

Most product-event families have first-seen dates clustered around **2025-05-28** (a major instrumentation push). Anything pre-2025-05-28 was tracked sparsely. Pre-2023-12-10 candidates have NO Amplitude history at all.

Newer families:
- `Candidacy - Did You Win Modal` (since 2025-10-31)
- `Dashboard - Campaign Plan Viewed` (since 2026-04-09)
- `Briefings - *` (since 2026-04-10)

### Onboarding flow versions (2026-05-07 cutover)

The Win onboarding flow was rebuilt in a clean same-night switch ~**2026-05-07 01:54 UTC** (no overlap / holdback observed in-data; DATA-1947). Event names changed across the cutover, so any onboarding analysis spanning it must use **version-agnostic** anchors:

| Role | Old flow (pre) | New flow (post) | Version-agnostic (both eras) |
|---|---|---|---|
| Entry / top | `Onboarding - Registration Completed` (died ~2026-04-20) | `Onboarding - Welcome Completed` (post only) | **product-DB account creation** (`users_win_candidacy.user_created_at`) — every Amplitude entry event changed |
| Completion / bottom | `onboarding_complete`, `Onboarding - Complete Step: Click Go to Dashboard` (died at cutover) | per-step `*Completed` events (pledge is the final step) | **`Onboarding - Candidate Pledge Completed`** (final step, both eras) |
| Party gate | party step + `Invalid Party` block (block died at cutover) | `Onboarding - Party Selection Completed` | No single version-agnostic event — use old-flow `Invalid Party` (pre-cutover) and new-flow `Onboarding - Party Selection Completed` (post-cutover) separately. `Onboarding - Candidate Affiliation Completed` has not been verified in-data (DATA-1947). |

**`onboarding_complete` / `has_completed_onboarding_flow` / `is_onboarded` (and the lib's `onboarded` flag) are NEW-FLOW-BLIND** — FALSE for every post-cutover user — so using them across the cutover fakes a completion collapse to zero. (`is_onboarded` is anchored on `Onboarding - Registration Completed`, which died ~2026-04-20, so it reads 0.0% for 2026-05/06 registrants vs ~54% recomputed; prod-flag fix ticketed.) Recompute via the canonical **Onboarded** metric (broad cohort) or **`Onboarding - Candidate Pledge Completed`** (strict completion) instead — see Canonical engagement metrics above. The party gate is *not* gate-equivalent across the cutover (the `Invalid Party` block was removed), so for a pre/post completion comparison condition on reaching the party step.

### Channel / UTM (acquisition source)

UTM lives only in the `user_properties` JSON on `stg_airbyte_source__amplitude_api_events` (no dedicated column). Extract with `get_json_object(user_properties,'$.utm_medium')` (also `$.initial_utm_medium`, `$.utm_source`, `$.initial_utm_source`); `utm_source` and `initial_utm_source` co-populate. There is **no instrumentation ramp** (UTM has been captured continuously since ~2025-06), and NULL conflates true organic with untracked. So the honest channel split is **3 buckets — paid-tagged (`utm_medium='paid'` OR `initial_utm_medium='paid'`) / other-tagged / untracked**, not a clean "organic." Lead non-paid; keep paid as a reference.

> ⚠ **Point-in-time figures (verified 2026-06-02; these drift — re-verify before citing):** UTM coverage fluctuated ~30-70% month to month; paid share of registrants collapsed from ~27% (Jan 2026) to ~3% (Apr 2026). Source: DATA-1947.

## Common query patterns

### Building new funnel aggregates

The pattern: read raw events from `stg_airbyte_source__amplitude_api_events`, filter to the relevant family + window, aggregate to `user_id × time bucket` or `user_id × funnel step`. Mirror the structure of `int__amplitude_user_milestones` (user-grain milestones) or `int__amplitude_win_activity` (per-month aggregates).

Reusable Python pull-script template at `analytics/projects/win_outcomes_scout/notebooks/_pull_amplitude_universe.py`. Connect via the profile-auth helper `analytics/lib/databricks_conn.py` (`run_query`, authenticates via the `~/.databrickscfg` profile). The reusable build-once-slice-many helper is `analytics/lib/win_analysis.py` (see the process skill's `methodology.md`).

## Cross-references

- [canonical_metrics.md](canonical_metrics.md) — governed definitions of the engagement/activation metrics.
- [outcomes.md](outcomes.md) — the Did-You-Win modal as a self-reported outcome.
- [gotchas.md](gotchas.md) — new-flow-blind flags, pre-2025-05-28 coverage floor, coverage-leads-calendar.
