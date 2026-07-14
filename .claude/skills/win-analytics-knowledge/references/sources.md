# Data sources reference

Part of the **win-analytics-knowledge** skill. Where Win-product data lives and which table to start from.

## Quick reference

- **Business context:** Win analyses draw on six overlapping data domains (product DB, analytics mart, civics mart, Amplitude events, HubSpot surveys, L2 voter data).
- **Entity grain:** varies by domain (see table). The primary working grain is one row per `campaign_version_id`.
- **Standard hygiene filter:** on `users_win_candidacy`, `is_latest_version AND NOT is_demo`.

## Routing triggers

- IF you need raw product state (registration, Pro flag, `path_to_victory`) → product DB staging tables.
- IF the question is about electoral outcomes → civics mart; see [outcomes.md](outcomes.md).
- IF the question is about engagement / funnels / events → Amplitude; see [engagement.md](engagement.md).
- IF the question is about self-reported PMF / satisfaction → HubSpot surveys; see [outcomes.md](outcomes.md).
- IF the question needs voter counts / demographics → L2, **district-grain only by default** (voter-grain is PII-adjacent; see [gotchas.md](gotchas.md)).
- IF you need ID/join recipes between these → see [joins.md](joins.md).
- IF you need when an event was added or retired in code, its current lifecycle status, or what event superseded it → the omni event-lifecycle assets, below.

## Data domains

| Domain | Where it lives | Grain | Use when |
|---|---|---|---|
| **Product DB (gp_api)** | `goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_*` | varies (user, campaign, position, path_to_victory, outreach, ...) | You need raw product state (registration, Pro flag, etc.) or to inspect product-side tables (e.g., `path_to_victory`) for funnel reconstruction. |
| **Analytics mart** | `goodparty_data_catalog.mart_analytics.*` | mostly user×campaign | Keystone tables for product analyses. **`users_win_candidacy` is the primary working table** — joins product user → campaign → candidacy → outcomes → viability → segmentation in one denormalized row. |
| **Civics mart** | `goodparty_data_catalog.mart_civics.*` | candidate / candidacy / candidacy_stage / election / election_stage | Outcome variables, viability, opponent counts, vote shares. Authoritative for electoral results across BR/TS/DDHQ/HubSpot providers. |
| **Amplitude product events** | staging: `goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events`<br>intermediates: `goodparty_data_catalog.dbt.int__amplitude_*`<br>mart passthrough: `goodparty_data_catalog.mart_analytics.amplitude_events` | event-grain (raw); user×month or user-grain (aggregates) | Engagement, funnel completion, time-to-action analyses. See [engagement.md](engagement.md) for the full landscape. |
| **HubSpot survey responses** | `goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_feedback_submissions` | one row per submission | Self-reported PMF (Sean Ellis "would you be very disappointed if...") and CSAT/stars. Surveys include "Win PMF - Web survey", "Win User satisfaction", and "Win - User research" (recruitment). Powers the third success signal alongside outcomes and engagement — see [outcomes.md](outcomes.md). |
| **L2 voter data** | `goodparty_data_catalog.dbt.int__l2_*` | district-grain (aggregations); voter-grain (uniform/Haystaq) | Electorate context (voter counts, demographics). **Voter-grain is PII-adjacent — restricted to lawful use cases. Default to district-grain.** Already surfaced into `users_win_candidacy` as `voter_count`, `l2_district_name`, `l2_district_type`. |

## Key tables

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

Field availability differs across these halves — see [gotchas.md](gotchas.md).

## Event-lifecycle assets (omni repo)

Three assets in the **omni monorepo** answer the event-history questions the warehouse cannot: which era an event covers in code, whether it is healthy to use right now, and what replaced it across product changes. They exist because data-observed dates (`int__amplitude_event_catalog.first_seen_date`) conflate "event did not exist" with "event existed but fired rarely" — for analyses spanning a product change (e.g. the 2026-05 onboarding cutover), consult these instead of inferring from volume.

| Asset | Path / where | Answers | Freshness check |
|---|---|---|---|
| **Provenance CSV** | omni `packages/runbooks/scripts/python/instrumentation_data/amplitude_event_provenance.csv` | **History.** Git-truth per event: `instrumented_date`/`_commit`/`_pr`, `retired_date`, author emails, live `call_site_count`, `call_site_retired_date`. | `updated_at` column. Written per-PR by omni's `instrument-analytics-event` skill; audited by omni's `refresh-event-provenance` runbook. |
| **Event-health log (+ report)** | omni `packages/runbooks/scripts/python/instrumentation_data/analytics-event-health-log.md` (durable, committed). The per-event JSON report (`instrumentation_data/analytics_event_health_report.json`) is a gitignored transient — present and current only after a monitor run. | **Present.** Per-event lifecycle status reconciled across declared intent, code presence, and firing volume (`active`, `dormant`, `retired`, `system`, `orphaned_firing`, ...), plus anomaly flags and metadata coverage. | Log entries are dated; the JSON has a `run_date` field. Regenerated by omni's `monitor-analytics-event-health` runbook. |
| **gp-meta governance metadata** | In Amplitude itself: a structured block in each event's description + a `product:*` tag (written by omni's `event-metadata` skill) | **Transitions.** Human-asserted purpose, status, and **supersession lineage** — which event replaced which. The most direct answer to "which onboarding event do I use for which period." | Coverage tracked in the health report (`metadata_coverage`). Code-provenance (CSV) and gp-meta are allowed to disagree; the health monitor reconciles them. |

Status vocabulary (what `dormant` vs `retired` vs `orphaned_firing` mean) is defined by the **analytics event lifecycle SOP**: ClickUp doc `2ky4jq2q-110533`, page `2ky4jq2q-91453`.

**Design constraint (decided 2026-07-14): these assets stay in omni.** They are curated git history, not product data, so they are deliberately not loaded into the warehouse — do not propose ingestion. The costs to accept and manage: reading them requires a **local omni checkout**, and they are only as fresh as their last run — check `updated_at` / `run_date` before relying on them, and if stale, run the omni refresh runbooks first.

## Cross-references

- [joins.md](joins.md) — ID landscape and join recipes between these domains.
- [engagement.md](engagement.md) — the Amplitude event landscape.
- [outcomes.md](outcomes.md) — civics-mart outcomes and HubSpot PMF.
