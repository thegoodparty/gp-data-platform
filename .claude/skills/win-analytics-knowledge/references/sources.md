# Data sources reference

Part of the **win-analytics-knowledge** skill. Where Win-product data lives and which table to start from.

## Quick reference

- **Business context:** Win analyses draw on six overlapping data domains (product DB, analytics mart, civics mart, Amplitude events, HubSpot surveys, L2 voter data).
- **Entity grain:** varies by domain (see table). The primary working grain is one row per `campaign_version_id`.
- **Standard hygiene filter:** on `users_win_candidacy`, `is_latest_version AND NOT is_demo`.
- **Internal accounts:** no governed filter exists. The agreed proxy is `email ILIKE '%@goodparty.org'` (user grain). Exclude for external-facing user counts and name the exclusion as an assumption. Verified 2026-07-21 (2026 H1): domain-anchored and bare-substring forms matched identical sets; residual QA-pattern emails among remaining users ≤2/month.

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

### BallotReady timeliness and candidate corroboration (retrospective only)

BallotReady is a **lagging** source: it lists a race's candidates well after the filing deadline.
Measured on 2025 general elections (n=45,854), only ~15% of candidates are in BR by the filing
deadline; median lag ~40 days, p90 ~101 days (measured 2026-08). So **BR presence cannot verify
candidates in-cycle** — "not in BR" near a deadline is uninformative, and any real-time gate built
on it rejects real candidates. Trust BR presence only retrospectively: after the election, or after
the filing deadline plus a ~60-100 day buffer.

A **corroborated candidate** (the reusable "is this a real candidacy" cut, DATA-2202) is a candidacy
that has a BallotReady, TechSpeed, or DDHQ candidacy-stage record AND whose general election has already
happened OR whose general filing deadline has passed. Reliable only on completed elections (per the
lag above). "Not corroborated" is an **upper bound on "not real"** — BR under-covers local races, so
some uncorroborated candidacies are real but unlisted; it is not a fake-account count. The match
flag lives on `candidacy_stage` — see [joins.md](joins.md), not `candidate_id_source`. When gating on the election date, clamp `general_election_date` to `[2020-01-01, 2050-01-01]` — corrupt out-of-range values exist ([gotchas.md](gotchas.md)). For the filing-deadline arm, use `election_stage.filing_period_end_on` joined via `candidacy_stage.gp_election_stage_id`; this column is 2026+ only (NULL for <=2025 — use the election-date arm alone for the archive half).

**Historical filing deadlines (pre-2026).** `election_stage.filing_period_end_on` is 2026+ only, but
the general filing deadline for earlier cycles is recoverable from BR raw staging:
`stg_airbyte_source__ballotready_api_race` (filter `NOT is_primary AND NOT is_runoff AND NOT is_recall`),
explode `filing_periods`, join `int__ballotready_filing_period.end_on`, election via
`race.election.databaseId`, keyed by `race.position.databaseid × election_day`. Coverage at the
general-race grain is partial and cycle-dependent (as of 2026-08: 72% of 2024 races, 50% of 2025, 40% of
2026) — always carry a deadline-missing flag. (Recipe from DATA-2202, promoted via DATA-2225.) These
figures predate DATA-2235 (`int__ballotready_filing_period` ingestion widened, merged 2026-08-12,
+39,797 ids recovered) — re-measure before citing; ~75% is a hard ceiling per that ticket (~7pp of the
gap is a position+election_day join failure, not an ingestion gap).

### Race-structure fields: opponents, seats, districting

**Opponent counts come from BallotReady rosters, continuously.** The column literally named
`number_of_opponents` on the provider election models is TechSpeed-only (BallotReady and DDHQ are
NULL on every row, verified 2026-08-20), but that is the legacy path, not the live one. Two
BR-roster-derived models carry the current counts:

- `int__civics_election_estimated_opponents` — election-grain, a count of active `candidacy_stage`
  records per stage (general preferred). **One-directional:** a roster of <= 1 means BallotReady has
  not loaded the field yet, so it never asserts 0 opponents. Carries no viability inputs, so it is
  safe under the viability-never-in-lead-scoring constraint.
- `int__civics_viability_opponents_fallback` — candidacy-grain, selected by roster **membership**
  (the candidacy's own BR candidacy ids found inside exactly one clean race), never by
  `matched_br_race_id`, which is a position+date min-pick and is not roster-verified. **Fail-closed:**
  membership in more than one clean race, an office-type contradiction against the roster race's
  position, or a roster under two members yields no row rather than a guess. Count and seats come
  from the same race row so the pair never mixes grains.

Because both track BallotReady ingest, opponent counts and the viability scores built on them
**update continuously as races fill in** — a count read today may differ tomorrow. Anchor any
quoted figure to a date rather than treating it as settled.

**TechSpeed's column is late-arriving.** Of scoped 2026 city-council/school-board leads that ever
received a TS count, only ~23% had it in the warehouse before user signup (~14% of all scoped 2026
leads; both lower bounds — `created_at` is Airbyte arrival time). Do not build at-signup features
on it.

**Seats and districting coverage is period-gated:** ~99% seats on 2026 city-council/school-board
candidacies, and **0% on the 2025 archive**, which is HubSpot-only with no election-stage structure.
A 2025-vs-2026 comparison on either field is not available, not merely thin.

## Event-lifecycle assets (omni repo)

For when an event was added or retired in code, its current lifecycle status, or what
superseded it, use the omni event-lifecycle assets (provenance CSV, event-health log, gp-meta
metadata). They are cross-product and live with the process skill:
`event-lifecycle-assets.md` in the analytics-process skill (when installed)
owns the full description — what each asset answers, freshness contracts, and the stay-in-omni
design constraint. Do not infer an event's existence era from data-observed first-seen dates.

## Cross-references

- [joins.md](joins.md) — ID landscape and join recipes between these domains.
- [engagement.md](engagement.md) — the Amplitude event landscape.
- [outcomes.md](outcomes.md) — civics-mart outcomes and HubSpot PMF.
