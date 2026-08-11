# Join keys reference

Part of the **win-analytics-knowledge** skill. The ID landscape and canonical join recipes.

## Quick reference

- **Business context:** Win-product entities (user, campaign, candidacy, candidate, election) are keyed differently across the product DB, civics mart, TechSpeed, and HubSpot. This doc is the crosswalk.
- **Entity grain:** the canonical working grain is `campaign_version_id` (`users_win_candidacy`); candidacy outcomes are at `gp_candidacy_id`.
- **Standard hygiene filter:** `users_win_candidacy` → `is_latest_version AND NOT is_demo`.

## Routing triggers

- IF you need to attach outcomes to a Win user → `users_win_candidacy.campaign_id = candidacy.product_campaign_id` (recipe below).
- IF you need to attach a HubSpot survey response → use the 2-hop via `candidate`, **NOT** `users_win_candidacy.hubspot_id` (that is the company id; see below and [gotchas.md](gotchas.md)).
- IF you need viability → `users_win_candidacy.viability_score`, or `mart_civics.candidacy_scored` via `product_campaign_id` + election-date; see [viability.md](viability.md).
- IF the question is about what a metric *means* rather than how to join → see [canonical_metrics.md](canonical_metrics.md).
- IF you have an external or ad-hoc table (emails, BallotReady race IDs) that needs ICP flags attached → use the **data-matching** skill, which owns the email→campaigns and race-id→position entry paths. This doc covers only the Win-entity ICP path (`candidacy.br_position_database_id`, below).

## The ID landscape

| Concept | Field | Where it lives | Notes |
|---|---|---|---|
| **Win user (product)** | `user_id` (BIGINT) | `users.user_id`, `users_win_*.user_id`, `int__amplitude_*.user_id` | Cast from Amplitude string `user_id` at the staging→intermediate boundary; non-numeric or empty IDs excluded. When querying raw `stg_airbyte_source__amplitude_api_events` directly (ad-hoc, BIGINT marts), use `try_cast(user_id as bigint) is not null` — **not** `where user_id rlike '^[0-9]+$' ... cast(user_id as bigint)`, which Spark can reorder so the cast runs before the regex filter and raises `CAST_INVALID_INPUT` on Firebase-style ids. |
| **Win campaign (product)** | `campaign_id` (string) | `campaigns.campaign_id`, `users_win_candidacy.campaign_id`, `candidacy.product_campaign_id` | A campaign can have multiple historical versions (`campaign_version_id`) if the user reused it across cycles. |
| **Campaign version** | `campaign_version_id` (string) | `users_win_candidacy.campaign_version_id` | Grain of `users_win_candidacy`. Pre-filter to `is_latest_version` for canonical rows. |
| **Candidacy (GP-internal)** | `gp_candidacy_id` (uuid) | `candidacy.gp_candidacy_id`, `candidacy_stage.gp_candidacy_id` | Hashed UUID. Stable across providers via ER crosswalk in `int__civics_er_canonical_ids`. |
| **Candidate (GP-internal)** | `gp_candidate_id` (uuid) | `candidate.gp_candidate_id`, `candidacy.gp_candidate_id` | A `gp_candidate_id` can have many `gp_candidacy_id` rows (one per cycle). |
| **Election (GP-internal)** | `gp_election_id` (uuid) | `election.gp_election_id`, `candidacy.gp_election_id` | The election year × position. |
| **Election stage** | `gp_election_stage_id` (uuid) | `election_stage.gp_election_stage_id`, `candidacy_stage.gp_election_stage_id` | One per stage. |
| **TechSpeed candidate code** | `techspeed_candidate_code` (string) | `int__civics_candidacy_techspeed.candidate_code` | Hashed first/last/state/office/city. |
| **HubSpot contact** | `hubspot_contact_id` (int) on candidate side; `hs_contact_id` (string, cast to BIGINT) on survey staging | `candidate.hubspot_contact_id`, `stg_airbyte_source__hubspot_api_feedback_submissions.hs_contact_id` | Same logical key, two column names by source. Use this to attach survey responses to candidates. |
| **HubSpot company** | `hubspot_company_id` (int) | `candidacy.hubspot_company_ids` (array), `users_win_candidacy.hubspot_id` | Used in 2025-archive viability join: `tbl_companies.company_id ↔ stg_model_predictions__viability_scores.id`. **Note `users_win_candidacy.hubspot_id` is the COMPANY id, not the contact id** — joining survey `hs_contact_id` to it returns zero matches; use the 2-hop recipe below. |
| **BR position** | `br_position_database_id` (int) | `candidacy.br_position_database_id` | Join key to `int__icp_offices` (district-grain L2 context, ICP flags). |

## Common query patterns

**Win user → outcomes:**
```
users_win_candidacy.campaign_id = candidacy.product_campaign_id
```
Use `users_win_candidacy` if you only need outcomes + segmentation that's already denormalized. Join out to `candidacy` for `latest_stage_reached`, `latest_stage_result`, `is_incumbent`, `is_open_seat`, `is_partisan`.

**Win user → engagement (Amplitude weekly):**
```
users_win_candidacy.user_id = int__amplitude_win_activity.user_id  (both BIGINT)
-- Weekly variant: int__amplitude_win_activity_weekly (same join key). Coverage and caveats owned by engagement.md (modeling layers).
```

**Win user → lifecycle milestones:**
```
users_win_candidacy.user_id = int__amplitude_user_milestones.user_id
```

**Candidacy → per-stage results:**
```
candidacy.gp_candidacy_id = candidacy_stage.gp_candidacy_id  (one-to-many)
```

**Candidacy → corroboration (external match) flag:**
```
-- roll up to gp_candidacy_id:
MAX(CASE WHEN candidacy_stage.br_candidacy_id IS NOT NULL
          OR candidacy_stage.ts_source_candidate_id IS NOT NULL
          OR candidacy_stage.ddhq_candidate_id IS NOT NULL THEN 1 ELSE 0 END)
```
"Has a BallotReady, TechSpeed, or DDHQ record" is the corroboration signal. Do **NOT** use
`candidate_id_source` for this — it is identity-mint provenance (the earliest-member ER rule), so a
product-sourced (`gp_api`) candidacy still carries BR/DDHQ matches and outcomes yet reads as
`gp_api`; using it as a match flag undercounts external corroboration massively.

This is the match flag *only*. The **corroborated-candidate** definition also requires the
retrospective time gate: `candidacy.general_election_date` clamped to `[2020-01-01, 2050-01-01]`
and `< CURRENT_DATE` (election happened), OR `election_stage.filing_period_end_on < CURRENT_DATE`
joined via `candidacy_stage.gp_election_stage_id` (deadline passed; 2026+ only). Without the gate
this counts matches, not corroborated candidates. Full definition in [sources.md](sources.md).

**Campaign → viability (canonical):**
```
c.campaign_id = candidacy_scored.product_campaign_id
and (c.election_date = candidacy_scored.general_election_date or c.election_date is null)
```
`users_win_candidacy` already implements this join; prefer reading it directly. See [viability.md](viability.md).

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
Then filter `users_win_candidacy` to `is_latest_version AND NOT is_demo`, and dedupe at `submission_id` grain (one user may have multiple candidacies → fan-out). For ICP attribution at submission grain, use `MAX(CASE WHEN icp_office_win THEN 1 ELSE 0 END)`. **Do NOT join `submissions.hs_contact_id` directly to `users_win_candidacy.hubspot_id`** — that returns zero matches because `hubspot_id` on the mart is the company id, not the contact id.

### Version-aware join: campaign × candidacy

`users_win_candidacy.sql:113-118` joins via `campaign_id AND election_date = general_election_date` (with `election_date IS NULL` fallback). This means a campaign-version that predates BR coverage will surface as a row with NULL candidacy fields rather than a dropped row.

### Multi-cycle wrinkle

- One `user_id` can produce multiple `campaign_version_id` rows across cycles.
- One `gp_candidate_id` can have many `gp_candidacy_id` rows (one per cycle).
- The product DB overwrites campaign rows in place; Airbyte's insert-only stream preserves history; `campaigns.sql` rebuilds historical versions keyed by `campaign_version_id`.

For user-level analyses, collapse with explicit handling of "which candidacy represents the user". For cycle-level analyses, restrict on `general_election_date` and use `is_latest_version`.

## Cross-references

- [sources.md](sources.md) — what each table is and when to start there.
- [viability.md](viability.md) — the canonical viability read and join rationale.
- [outcomes.md](outcomes.md) — the PMF/CSAT survey join in context.
- [gotchas.md](gotchas.md) — the `hubspot_id` company-vs-contact trap and multi-cycle traps.
- the data-matching skill (when installed) — matching external tables to ICP flags by email / race ID / position ID.
