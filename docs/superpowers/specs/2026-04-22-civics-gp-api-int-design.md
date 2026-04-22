# DATA-1719 — Product Database Civics Mart Intermediates

**Status:** Design approved 2026-04-22
**Ticket:** [DATA-1719](https://goodparty.clickup.com/t/90132012119/DATA-1719)
**Branch / PR:** `data-1719-civics-gp-api-intermediates` → [PR #335 (draft)](https://github.com/thegoodparty/gp-data-platform/pull/335)
**Scope of this PR:** Intermediate-layer only. Marts, the `_2025` archive, and any upstream ER source files are out of scope.

## For implementers (read this first)

If you're picking this up in a fresh session, here's the orientation to get productive fast:

**Repo workflow docs (authoritative):**
- `dbt/project/CLAUDE.md` — dbt Cloud CLI conventions, build/test workflow, `dbt show` and `inspect_data` usage, naming conventions, terminology definitions (Candidate / Candidacy / Election / Election-Stage / Candidacy-Stage), and the ID Mappings table that governs which Product DB ID maps to which civics-mart column.
- User memory: commits in this repo must be wrapped with `cd dbt && poetry run git commit ...` (or `poetry -C dbt run bash -c "git commit ..."` from the repo root) so pre-commit hooks find pyspark/airflow.

**Reference implementations to pattern-match (in priority order):**
- `dbt/project/models/intermediate/civics/int__civics_candidacy_ballotready.sql` — full provider candidacy model: staging → enrichment → salted UUID → ER coalesce → referential integrity filter → dedup → final SELECT. This is the closest analog for `_candidacy_gp_api`.
- `dbt/project/models/intermediate/civics/int__civics_candidate_techspeed.sql` — shows how TechSpeed uses the ER crosswalk: window-propagates `canonical_gp_candidate_id` over a partition keyed by the provider's own person hash. This pattern is what `_candidate_gp_api` uses (see §Per-model specifications → candidate).
- `dbt/project/models/intermediate/civics/int__civics_candidacy_stage_techspeed.sql` — candidacy-stage grain, including stage-level ER lookup and the candidacy-grain cascade window. Template for `_candidacy_stage_gp_api`.
- `dbt/project/models/intermediate/civics/int__civics_elected_official_ballotready.sql` — elected_official output schema. Note: no `gp_candidate_id` column — elected_official is its own entity.
- `dbt/project/models/intermediate/civics/int__civics_er_canonical_ids.sql` — current ER crosswalk (TS-only). The extension for `gp_api` UNIONs a symmetric CTE.

**Mart inputs our intermediates consume:**
- `dbt/project/models/marts/civics/users.sql` — has `user_id`, `campaign_count`, `is_serve_user`, `is_win_user`, email/name/phone/zip.
- `dbt/project/models/marts/civics/campaigns.sql` — has `is_latest_version` (filter on this), `user_id`, user fields, `campaign_state`, `campaign_party`, `campaign_office`, `ballotready_position_id`, `normalized_position_name`, `election_date`, `election_level`, `is_demo`, `is_verified`, `is_pledged`, `did_win`, `hubspot_id`.
- `dbt/project/models/marts/civics/organizations.sql` — org × user × position × district joined, with `organization_type` (`serve` | `win`). Grain is awkward for elected_official consumption; spec sources `stg_..._gp_api_db_elected_office` directly instead.

**Macros referenced in the spec** (all in `dbt/project/macros/`):
- `generate_salted_uuid(fields=[...])` — deterministic UUID across providers; field ORDER matters for cross-source convergence.
- `generate_gp_election_id()` — requires `state`, `office_type`, `candidate_office`, `district`, `seats_available`, `general_election_date` in scope.
- `map_office_type(candidate_office)` — normalizes office strings.
- `parse_party_affiliation(json_expr)` — used in BR candidacy.
- `clean_phone_number(phone_expr)` — phone normalization.
- `extract_city_from_office_name(position_name)` — parses city from office name strings.
- `generate_candidate_office_from_position(position_name, normalized_position_name)` — used in BR.
- Similar per-provider ID hashers exist (`generate_ts_gp_candidate_id`, `generate_ts_gp_candidacy_id`, `generate_ts_gp_election_stage_id`). If a Product-DB analog is needed, follow that naming pattern (`generate_gp_api_gp_candidate_id` etc.) — but for Product DB, using native IDs in the salt may remove the need.

**Build / test / commit cycle (from CLAUDE.md):**
1. `dbt run --select "model_name"` — materialize.
2. `dbt run-operation inspect_data --args '{"model": "model_name"}'` — counts, types, null rates, sample rows.
3. Write tests informed by the data in `int__civics.yaml`.
4. `dbt test --select "model_name"` — must pass before committing.
5. Commit via `cd dbt && poetry run git commit ...` (pre-commit hooks need the poetry env).
6. Do not use `+model_name` selectors (upstream prefix) — dbt Cloud defers; upstream selectors pull hundreds of unnecessary models.

**Upstream ER check (do this before writing the crosswalk extension):**
Run `dbt show --inline "select source_name, count(*) from {{ ref('stg_er_source__clustered_candidacy_stages') }} group by source_name"` — confirm `gp_api` rows are still present. As of 2026-04-22 there were 20,325 rows. If this drops to zero, the ER upstream was reverted and the crosswalk extension will produce no rows.

## Context and goals

Product Database (GoodParty.org's own application database, exposed as `gp_api_db` via Airbyte) is one of four providers in the Civics Mart, alongside BallotReady, TechSpeed, and DDHQ. The other three have provider-specific intermediate models (`int__civics_{candidate,candidacy,candidacy_stage,election,election_stage,elected_official}_{ballotready,techspeed,ddhq}`). Product Database does not.

This PR adds intermediate models that transform Product DB source data into the Civics Mart target schema, so the downstream `marts/civics/*` union layer can consume Product DB as a first-class provider.

**Key product-level insight:** Product DB users create candidacies before BallotReady or TechSpeed sees them. Surfacing these candidacies early and linking them by `product_campaign_id` lets the Civics Mart represent users before they appear in external data feeds.

## Locked design decisions

These were resolved through brainstorming; the rationale is captured in `docs/superpowers/specs/2026-04-22-civics-gp-api-int-design.md` (this file).

| Decision | Value |
|---|---|
| Entity scope | `candidate`, `candidacy`, `candidacy_stage`, `elected_official` (no standalone `election`, no standalone `election_stage` for Product DB — BR/TS own those grains) |
| File suffix | `_gp_api` (matches source naming) |
| `candidate_id_source` literal | `'gp_api'` |
| Source approach | Source from `marts/civics/{users,campaigns,organizations}.sql` and staging `stg_airbyte_source__gp_api_db_elected_office` — no custom prep "base" tables |
| Demo campaigns | Included with `is_demo` preserved (no pre-filter) |
| Orphan elected_office rows (9 rows where `eo.campaign_id` doesn't resolve) | Filtered out of `elected_official_gp_api` |
| Candidate scope filter | `users.campaign_count > 0` only (Serve-only users with no campaign are NOT candidates) |
| Elected-official signal | `gp_api_db_elected_office` table (authoritative — `did_win` is stale: only 165 of 1,811 elected_offices have it set) |
| ER crosswalk | Extend `int__civics_er_canonical_ids` in-place with nullable Product DB keys (unified wide-table structure); ER upstream already emits `source_name = 'gp_api'` rows with `source_id = '{campaign_id}__{stage}'` |
| `candidacy_result` derivation | `case when did_win then 'Won' else null end` — under-populated but honest |
| Candidacy-stage grain | Cross-join campaign × BR election_stage rows for same position+year. Campaigns without BR position link produce 0 rows. |

## Data findings that informed the design

Run against Databricks on 2026-04-22:

- `gp_api_db_elected_office`: 1,811 rows, 100% populated on `campaign_id`, `user_id`, `organization_slug`. 1,811 distinct users (1:1), 1,811 distinct campaigns (1:1), 1,804 distinct orgs.
- `eo.user_id` matches the linked `campaign.user_id` in 1,802 cases; **0 mismatches**, 9 orphans (broken `campaign_id` FK).
- Campaigns: 61,548 total, 1,802 with an elected_office, 165 with `did_win = true`, 18 with both. `did_win` is effectively stale for most winning campaigns.
- ER crosswalk source (`stg_er_source__clustered_candidacy_stages`): already emits `source_name = 'gp_api'` for 20,325 rows / 19,438 clusters. `source_id` format is `{campaign_id}__{primary|general|runoff}`.
- Exactly the 9 orphan eo rows have null state; everything else resolves cleanly.

## Architecture

```
marts/civics/users.sql ────────────────────────────┐
                                                   ├─→ int__civics_candidate_gp_api
marts/civics/campaigns.sql ──────────────┬─────────┤
                                         │         │
                                         ├─→ int__civics_candidacy_gp_api ←──┐
                                         │                                   │
                                         ├─→ int__civics_candidacy_stage_gp_api ←── int__civics_candidacy_gp_api
                                         │                                   ↑
int__civics_election_stage_ballotready ──┘    (stage FK target) ─────────────┘

stg_..._gp_api_db_elected_office ─┐
marts/civics/users.sql ───────────┼─→ int__civics_elected_official_gp_api
marts/civics/campaigns.sql ───────┘

int__civics_er_canonical_ids  (extended with nullable gp_api_* columns)
  ↳ consumed (left-joined) by all four gp_api intermediates
```

All four new intermediates: `materialized = "table"`, tagged `["civics", "gp_api"]`. ER crosswalk inherits existing config.

Intentional note on dbt layering: the four intermediates reference mart models (users, campaigns, organizations). This inverts the standard staging → int → mart flow. Precedent exists (`int__civics_candidacy_2025` references `ref("campaigns")`), and the benefit is reuse of mart-level versioning (`is_latest_version`) and position normalization (`normalized_position_name`, `campaign_office`), which are non-trivial to re-implement.

## Per-model specifications

### `int__civics_candidate_gp_api.sql`

**Grain:** One row per Product DB user who has at least one campaign.

**Source:**
- `marts/civics/users.sql` as `u` (filtered: `campaign_count > 0`)
- Enrichment join: `marts/civics/campaigns.sql` filtered to `is_latest_version AND NOT is_demo`, aggregated per user to derive `state` (most recent non-demo campaign's `campaign_state`) and `candidate_id_tier` (max tier across user's campaigns). This also provides the per-user list of campaign_ids for the ER lookup below.
- `stg_airbyte_source__gp_api_db_user.meta_data` as fallback for `hubspot_contact_id` (not surfaced on the users mart)
- ER left-join: `int__civics_er_canonical_ids` is keyed at candidacy-stage grain (`gp_api_campaign_id`), not at user grain. Candidate-level canonicalization goes through campaigns: join campaigns → ER on `gp_api_campaign_id`, then propagate `canonical_gp_candidate_id` across all of a user's rows via a window partitioned by the user's deterministic hash. Mirrors the TS pattern (`generate_ts_gp_candidate_id` window partition).

**ID generation:** Salt field order matches BR (`int__civics_candidate_ballotready.sql`):
```
coalesce(
    max(xw.canonical_gp_candidate_id) over (
        partition by generate_salted_uuid([first_name, last_name, state, null, email, phone])
    ),
    generate_salted_uuid([first_name, last_name, state, cast(null as string), email, phone])
) as gp_candidate_id
```
where the `xw` left-join is `campaigns → er_canonical_ids on gp_api_campaign_id = campaigns.campaign_id`. If any of a user's campaigns clusters to a BR row, the canonical id propagates across all of that user's rows.

**Output columns:**

| Column | Source |
|---|---|
| `gp_candidate_id` | coalesce(ER canonical, salted UUID) |
| `hubspot_contact_id` | `user.meta_data:hubspotid::string` |
| `prod_db_user_id` | `u.user_id` |
| `candidate_id_tier` | `max(c.tier)` over user's non-demo campaigns |
| `first_name, last_name` | `u.first_name, u.last_name` |
| `full_name` | `concat(first_name, ' ', last_name)` |
| `birth_date` | `cast(null as date)` — not in Product DB |
| `state` | most-recent non-demo campaign's `campaign_state` |
| `email, phone_number` | `u.email, u.phone` |
| `street_address, website_url, linkedin_url, twitter_handle, facebook_url, instagram_handle` | `cast(null as string)` — not in Product DB |
| `created_at, updated_at` | `u.created_at, u.updated_at` |

**Dedup:** `qualify row_number() over (partition by gp_candidate_id order by updated_at desc) = 1` — handles ER-collapse cases.

### `int__civics_candidacy_gp_api.sql`

**Grain:** One row per latest-version campaign.

**Source:**
- `marts/civics/campaigns.sql` as `c` (filtered: `is_latest_version`)
- Referential integrity: INNER JOIN to `int__civics_candidate_gp_api` on computed `gp_candidate_id` (same pattern as `int__civics_candidacy_ballotready`'s `valid_candidates` filter)
- ER left-join: `int__civics_er_canonical_ids` on `gp_api_campaign_id = c.campaign_id`, adopt `canonical_gp_candidacy_id` when present

**ID generation:**
```
coalesce(
    xw.canonical_gp_candidacy_id,
    generate_salted_uuid([
        first_name, last_name, state, party_affiliation, candidate_office,
        cast(coalesce(general_election_date, primary_election_date,
                      general_runoff_election_date, primary_runoff_election_date) as string),
        district
    ])
) as gp_candidacy_id
```

Salt field order matches BR for cross-provider convergence.

**Output columns** (aligned to BR candidacy schema for downstream union):

| Column | Source |
|---|---|
| `gp_candidacy_id` | coalesce(ER canonical, salted UUID) |
| `gp_candidate_id` | matches `int__civics_candidate_gp_api` logic |
| `gp_election_id` | `generate_gp_election_id()` macro — requires `state`, `office_type`, `candidate_office`, `district`, `seats_available`, `general_election_date` in scope before the macro call; `seats_available` is `cast(null as int)` for Product DB (not tracked) |
| `product_campaign_id` | `c.campaign_id` |
| `hubspot_contact_id` | `c.hubspot_id` |
| `hubspot_company_ids` | `cast(null as string)` |
| `candidate_id_source` | `'gp_api'` |
| `party_affiliation` | `nullif(c.campaign_party, '')` |
| `is_incumbent, is_open_seat` | `cast(null as boolean)` |
| `candidate_office` | `c.campaign_office` |
| `official_office_name` | `c.normalized_position_name` |
| `office_level` | `case when lower(c.election_level) = 'city' then 'Local' else initcap(c.election_level) end` |
| `office_type` | `map_office_type(candidate_office)` |
| `candidacy_result` | `case when c.did_win then 'Won' else null end` |
| `is_pledged` | `c.is_pledged` |
| `is_verified` | `c.is_verified` |
| `verification_status_reason` | `cast(null as string)` |
| `is_partisan` | `party_affiliation is not null and lower(party_affiliation) != 'nonpartisan'` |
| `primary_election_date` | `cast(null as date)` |
| `primary_runoff_election_date` | `cast(null as date)` |
| `general_election_date` | `c.election_date` |
| `general_runoff_election_date` | `cast(null as date)` |
| `br_position_database_id` | `c.ballotready_position_id` |
| `br_candidacy_id`, `br_race_id` | `cast(null as string)` |
| `viability_score, win_number, win_number_model` | `cast(null as ...)` |
| `is_demo` | `c.is_demo` |
| `created_at, updated_at` | `c.created_at, c.updated_at` |

**Filter:** Require `general_election_date IS NOT NULL` (for salted UUID determinism). Campaigns without an election date are dropped.

**Dedup:** `qualify row_number() over (partition by gp_candidacy_id order by updated_at desc) = 1`.

### `int__civics_candidacy_stage_gp_api.sql`

**Grain:** One row per (campaign × BR election_stage). One campaign may emit multiple rows (one per BR stage for its position+year).

**Source:**
- `marts/civics/campaigns.sql` (filtered: `is_latest_version AND ballotready_position_id IS NOT NULL`)
- INNER JOIN to `int__civics_election_stage_ballotready` on `ballotready_position_id = br_position_database_id AND year(c.election_date) = year(br.election_date)`
- Referential integrity: INNER JOIN to `int__civics_candidacy_gp_api` on computed `gp_candidacy_id`
- ER left-join: `int__civics_er_canonical_ids` on `gp_api_campaign_id + gp_api_stage_election_date`

**ID generation:**
```
coalesce(
    xw.canonical_gp_candidacy_stage_id,
    generate_salted_uuid([gp_candidacy_id, gp_election_stage_id])
) as gp_candidacy_stage_id
```

**Output columns** (aligned to TS candidacy_stage schema):

| Column | Source |
|---|---|
| `gp_candidacy_stage_id` | coalesce(ER canonical, salted UUID) |
| `gp_candidacy_id` | matches `int__civics_candidacy_gp_api` logic |
| `gp_election_stage_id` | coalesce(ER canonical, BR's `gp_election_stage_id`) |
| `candidate_name` | `concat(c.user_first_name, ' ', c.user_last_name)` |
| `source_candidate_id` | `cast(c.campaign_id as string)` |
| `source_race_id` | `cast(null as string)` |
| `candidate_party` | `c.campaign_party` |
| `is_winner` | `cast(null as boolean)` (stage-level result not available from Product DB) |
| `election_result` | `cast(null as string)` |
| `election_result_source` | `cast(null as string)` |
| `match_confidence, match_reasoning, match_top_candidates` | `cast(null as ...)` |
| `has_match` | `false` |
| `votes_received` | `cast(null as string)` |
| `election_stage_date` | `br.election_date` |
| `created_at, updated_at` | `c.created_at, c.updated_at` |

**Dedup:** `qualify row_number() over (partition by gp_candidacy_stage_id order by updated_at desc) = 1`.

### `int__civics_elected_official_gp_api.sql`

**Grain:** One row per `gp_api_db_elected_office` row (after orphan filter: ~1,802 rows).

**Source:**
- `stg_airbyte_source__gp_api_db_elected_office` as `eo`
- INNER JOIN `marts/civics/campaigns.sql` as `c` on `eo.campaign_id = c.campaign_id AND c.is_latest_version` — this filter also removes the 9 orphans
- LEFT JOIN `marts/civics/users.sql` as `u` on `eo.user_id = u.user_id` for email/phone/name enrichment (campaign mart has user fields too, used as fallback)

**ID generation:**
```
generate_salted_uuid(['gp_api_elected_office', cast(eo.id as string)])
as gp_elected_official_id
```

Deterministic on `elected_office.id` — same stable row → same ID across runs.

**Output columns** (narrower than BR's — omits BR-specific mailing / term / judicial / vacancy fields):

| Column | Source |
|---|---|
| `gp_elected_official_id` | salted UUID |
| `gp_api_elected_office_id` | `eo.id` |
| `gp_api_user_id` | `eo.user_id` |
| `gp_api_campaign_id` | `eo.campaign_id` |
| `gp_api_organization_slug` | `eo.organization_slug` |
| `first_name, last_name` | `u.first_name, u.last_name` |
| `full_name` | `concat(first_name, ' ', last_name)` |
| `email` | `u.email` |
| `phone` | `clean_phone_number(u.phone)` |
| `office_phone, central_phone` | `cast(null as string)` |
| `position_name` | `c.normalized_position_name` |
| `candidate_office` | `c.campaign_office` |
| `office_level` | mapping from `c.election_level` (same mapping as candidacy) |
| `office_type` | `map_office_type(candidate_office)` |
| `state` | `c.campaign_state` |
| `city, district` | `cast(null as string)` |
| `term_start_date` | `eo.sworn_in_date` |
| `term_end_date` | `cast(null as date)` |
| `is_appointed, is_judicial, is_vacant, is_off_cycle` | `cast(null as boolean)` |
| `party_affiliation` | `c.campaign_party` |
| `website_url, linkedin_url, facebook_url, twitter_url` | `cast(null as string)` |
| `candidate_id_source` | `'gp_api'` |
| `created_at, updated_at` | `eo.created_at, eo.updated_at` |

**No `gp_candidate_id` column** — consistent with BR's elected_official schema. Elected-official is its own entity.

**Dedup:** `qualify row_number() over (partition by gp_elected_official_id order by updated_at desc) = 1`.

## ER crosswalk extension — `int__civics_er_canonical_ids.sql`

Modify the existing file to unify TS and Product DB sections under a single wide schema. Both sections resolve cluster mates to BR's canonical gp_* IDs; consumers join on the columns relevant to their source.

**New output columns** (added, all nullable):

| Column | Notes |
|---|---|
| `gp_api_campaign_id` | `bigint`, populated on gp_api rows only |
| `gp_api_stage_election_date` | `date`, populated on gp_api rows only |

Existing `ts_source_candidate_id`, `ts_stage_election_date`, `canonical_gp_*` columns are preserved unchanged. TS rows have null `gp_api_*`, and vice versa.

**New CTE** `gp_api_stage_matches` (mirrors the existing `stage_matches` CTE structure, filtered to `br_cw.source_name = 'ballotready' and gp_cw.source_name = 'gp_api'`). Extracts `gp_api_campaign_id` via `regexp_replace(source_id, '__(primary|general|runoff)$', '')::bigint`.

**Final SELECT:** UNION ALL of the two sections, each deduped with its own `qualify row_number()`:
- TS section: `partition by ts_source_candidate_id, ts_stage_election_date order by br_updated_at desc`
- gp_api section: `partition by gp_api_campaign_id, gp_api_stage_election_date order by br_updated_at desc`

**Header comment:** updated to reflect multi-source scope; the existing CTE renamed `stage_matches` → `ts_stage_matches` for symmetry.

**Backward compatibility:** TS intermediate models keep working unchanged — they join only on `ts_*` columns and don't reference the new `gp_api_*` columns.

## Testing strategy

All tests in `dbt/project/models/intermediate/civics/int__civics.yaml`. Per CLAUDE.md, build each model, run `inspect_data` to confirm counts and null rates before committing test thresholds.

| Model | Tests |
|---|---|
| `int__civics_candidate_gp_api` | `gp_candidate_id` unique + not_null; `prod_db_user_id` unique + not_null; `first_name`, `last_name`, `email`, `state` not_null; `state` accepted_values (US state codes); `expression_is_true` `updated_at >= created_at` |
| `int__civics_candidacy_gp_api` | `gp_candidacy_id` unique + not_null; `gp_candidate_id` not_null + relationships → `int__civics_candidate_gp_api.gp_candidate_id`; `product_campaign_id` unique + not_null; `candidate_id_source` accepted_values `['gp_api']`; `office_level` accepted_values; `candidacy_result` accepted_values `['Won']` (nulls allowed by default); `general_election_date` not_null; `expression_is_true` `updated_at >= created_at` |
| `int__civics_candidacy_stage_gp_api` | `gp_candidacy_stage_id` unique + not_null; `gp_candidacy_id` relationships → `int__civics_candidacy_gp_api.gp_candidacy_id`; `gp_election_stage_id` relationships → `int__civics_election_stage_ballotready.gp_election_stage_id`; `has_match` accepted_values `[false]` |
| `int__civics_elected_official_gp_api` | `gp_elected_official_id` unique + not_null; `gp_api_elected_office_id` unique + not_null; `gp_api_user_id` not_null; `state` not_null; `candidate_id_source` accepted_values `['gp_api']` |
| `int__civics_er_canonical_ids` (modified) | Existing tests preserved; new: `dbt_utils.unique_combination_of_columns` on `(gp_api_campaign_id, gp_api_stage_election_date)` with `config.where: "gp_api_campaign_id is not null"` |

**Build/validate flow (per CLAUDE.md):**
1. `dbt run --select "int__civics_candidate_gp_api int__civics_candidacy_gp_api int__civics_candidacy_stage_gp_api int__civics_elected_official_gp_api int__civics_er_canonical_ids"`
2. For each new model: `dbt run-operation inspect_data --args '{"model": "int__civics_<model>"}'` — verify row counts, column types, null rates, sample rows.
3. `dbt test --select "int__civics_candidate_gp_api int__civics_candidacy_gp_api int__civics_candidacy_stage_gp_api int__civics_elected_official_gp_api int__civics_er_canonical_ids"`
4. Regression: rebuild + retest BR and TS intermediates that depend on `int__civics_er_canonical_ids` (`int__civics_candidate_techspeed`, `int__civics_candidacy_techspeed`, `int__civics_candidacy_stage_techspeed`) to confirm no breakage from the crosswalk UNION.

## Out of scope for this PR

- Downstream `marts/civics/*.sql` unions (candidacy, candidate, candidacy_stage). Those will be updated in a followup once the intermediates are landed and validated.
- Removal/deprecation of `int__civics_{candidacy,candidate,candidacy_stage,election,election_stage}_2025.sql` (archive models). They coexist until downstream consumers are migrated.
- Standalone `int__civics_election_gp_api.sql` and `int__civics_election_stage_gp_api.sql` — intentionally skipped; BR/TS/DDHQ own those grains.
- Enrichment of `elected_official_gp_api` with `is_serve_user` behavioral signals from Segment (e.g., `sworn_in_completed`) — deferred.
- Zip-to-state derivation for any null-state rows. We filter orphans instead.

## Acceptance criteria (from ticket)

- ✅ Intermediate models for all relevant Product DB entities materialized in dbt (4 models: candidate, candidacy, candidacy_stage, elected_official; `election` and `election_stage` explicitly scoped out per design).
- ✅ Schemas and values match the expected Civics Mart schemas (BR/TS schemas are the reference).
- ✅ Includes dbt data tests (per yaml table above).
- ✅ Incorporates `gp_user_id` (as `prod_db_user_id`) and `product_campaign_id` in candidate/candidacy tables respectively.
