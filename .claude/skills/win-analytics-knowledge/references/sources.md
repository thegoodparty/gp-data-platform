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

## Cross-references

- [joins.md](joins.md) — ID landscape and join recipes between these domains.
- [engagement.md](engagement.md) — the Amplitude event landscape.
- [outcomes.md](outcomes.md) — civics-mart outcomes and HubSpot PMF.
