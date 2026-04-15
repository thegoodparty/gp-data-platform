# Civics Mart: `elected_officials` (BallotReady pass-through)

**Ticket:** [DATA-1724](https://app.clickup.com/t/86ag7kgt2) — "Add BallotReady records to Civics Mart (elected_officials)"

**Date:** 2026-04-15

**Author:** Hugh Karimi

---

## Summary

Introduce a new mart table `mart_civics.elected_officials` as a pass-through of the existing BallotReady office-holder intermediate (`int__civics_elected_official_ballotready`). BallotReady is the first (and currently only) provider for elected officials in the Civics Mart, so no cross-provider reconciliation is performed.

The file is shaped like the rest of the civics mart (a `combined → deduplicated` CTE structure with `qualify row_number() ... = 1` on `updated_at desc`) so that adding a future provider after entity resolution lands is a one-line `union all` addition with no restructuring.

This work also includes a small upstream rename (`tier → br_position_tier`) in the BR office-holder staging and intermediate models, to (a) give the column a non-misleading name grounded in BallotReady's `Position.tier` source, and (b) avoid a future column-name collision with TechSpeed's unrelated `tier` column when both providers are unioned post-entity-resolution.

---

## Acceptance criteria (from ticket)

- BallotReady elected_officials records are present in the Civics mart.
- No reconciliation is needed (first provider).
- Data flows correctly from the intermediate models into the mart.

---

## Files touched

| File | Change |
|---|---|
| `dbt/project/models/marts/civics/elected_officials.sql` | **NEW** — pass-through mart from BR intermediate |
| `dbt/project/models/marts/civics/m_civics.yaml` | **MODIFIED** — add `elected_officials` model entry with column docs and tests |
| `dbt/project/models/staging/airbyte_source/ballotready_s3/stg_airbyte_source__ballotready_s3_office_holders_v3.sql` | **MODIFIED** — rename `tier → br_position_tier` (line 79) |
| `dbt/project/models/intermediate/civics/int__civics_elected_official_ballotready.sql` | **MODIFIED** — rename `tier → br_position_tier` (lines 90 and 137 as of writing). The intermediate keeps `br_candidate_id` and `br_candidacy_id`; the mart drops them by enumerating columns explicitly in its final select. |
| `dbt/project/models/intermediate/civics/int__civics.yaml` | **MODIFIED** — update the column entry for `tier` to `br_position_tier` under `int__civics_elected_official_ballotready` (currently undocumented; add a description) |

---

## Mart SQL

```sql
-- Civics mart: elected_officials
-- Pass-through of BallotReady office holders. BallotReady is the first
-- (and currently only) provider for elected officials. Additional providers
-- (e.g., TechSpeed) will be added once entity resolution is in place.
-- Grain: one row per office-holder term.

with
    combined as (
        select * from {{ ref("int__civics_elected_official_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_elected_official_id order by updated_at desc)
            = 1
    )

select
    gp_elected_official_id,
    br_office_holder_id,
    br_position_id,
    first_name,
    last_name,
    middle_name,
    suffix,
    full_name,
    email,
    phone,
    office_phone,
    central_phone,
    position_name,
    normalized_position_name,
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,
    term_start_date,
    term_end_date,
    is_appointed,
    is_judicial,
    is_vacant,
    is_off_cycle,
    party_affiliation,
    website_url,
    linkedin_url,
    facebook_url,
    twitter_url,
    mailing_address_line_1,
    mailing_address_line_2,
    mailing_city,
    mailing_state,
    mailing_zip,
    br_geo_id,
    br_position_tier,
    candidate_id_source,
    created_at,
    updated_at
from deduplicated
```

The explicit column list in the final select serves two purposes: (1) drops `br_candidate_id` and `br_candidacy_id`, and (2) matches the column-enumeration style of sibling marts like `candidacy.sql` and `candidate.sql`. (When a second provider is eventually unioned into `combined`, that union will also need to enumerate columns inside each half — but that's deferred work.)

Materialization, schema (`mart_civics`), and tags (`["mart", "civics"]`) are inherited from `dbt_project.yml`.

---

## Columns (41)

All columns from `int__civics_elected_official_ballotready` flow through unchanged, **except**:

- `br_candidate_id` — **dropped** (YAGNI; not used by any consumer today; trivially recoverable from the intermediate if needed).
- `br_candidacy_id` — **dropped** (same rationale).
- `tier` — **renamed upstream** to `br_position_tier` (see "Upstream rename" below). Surfaces in the mart as `br_position_tier`.
- `candidate_id_source` — **kept as-is.** Although the name is awkward in the elected-officials context (the row isn't a candidate), the column is a project-wide convention used in 33 files, including the as-yet-unused `int__civics_elected_official_techspeed`. Renaming only here would fragment the convention and create an asymmetry with the TechSpeed sibling intermediate. The hardcoded value remains `'ballotready'`.

The full mart column list:

| Group | Columns |
|---|---|
| Keys | `gp_elected_official_id` (PK), `br_office_holder_id` (natural key), `br_position_id` |
| Person | `first_name`, `last_name`, `middle_name`, `suffix`, `full_name`, `email`, `phone`, `office_phone`, `central_phone` |
| Office | `position_name`, `normalized_position_name`, `candidate_office`, `office_level`, `office_type` |
| Geo | `state`, `city`, `district` |
| Term | `term_start_date`, `term_end_date` |
| Flags | `is_appointed`, `is_judicial`, `is_vacant`, `is_off_cycle` |
| Affiliation | `party_affiliation` |
| URLs | `website_url`, `linkedin_url`, `facebook_url`, `twitter_url` |
| Mailing | `mailing_address_line_1`, `mailing_address_line_2`, `mailing_city`, `mailing_state`, `mailing_zip` |
| Other | `br_geo_id`, `br_position_tier`, `candidate_id_source` |
| Audit | `created_at`, `updated_at` |

---

## Tests (mart layer)

These mirror the consumer-facing contract; the intermediate already enforces these but mart-layer tests guard against regressions in any future provider added to the union.

| Column | Tests |
|---|---|
| `gp_elected_official_id` | `unique`, `not_null`, `is_uuid` |
| `br_office_holder_id` | `unique`, `not_null` |
| `br_position_id` | `not_null` |
| `first_name` | `not_null` |
| `last_name` | `not_null` |
| `position_name` | `not_null` |
| `state` | `not_null` + `accepted_values: {{ get_us_states_list(include_DC=true, include_US=true, include_territories=true) }}` |
| `office_level` | `accepted_values: [Local, County, Township, State, Regional, Federal]` |
| `party_affiliation` | `accepted_values: [Independent, Nonpartisan, Democrat, Republican, Libertarian, Green, Other]` |
| `candidate_id_source` | `accepted_values: [ballotready]` |
| `created_at` | `not_null` |
| `updated_at` | `not_null` |

**Notes:**

- No `relationships` (FK) tests. This entity is independent in the civics mart — no clean FK relationship to `candidate`, `candidacy`, `election`, or `election_stage` exists today (see "Deferred / out of scope" below).
- No `dbt_utils.expression_is_true` row-level invariants. No sibling civics mart uses them; staying consistent.
- `accepted_values` tests do **not** include `config.where` clauses — per project CLAUDE.md, `accepted_values` already skips nulls.
- `br_position_tier` is documented but untested — its valid integer range hasn't been confirmed with BallotReady.

---

## Upstream rename: `tier → br_position_tier`

The BR office-holder S3 source carries a column called `tier`. Tracing it upstream:

1. **Source** (`ballotready_s3_office_holders_v3`): raw `tier` column, untyped string.
2. **Staging** (`stg_airbyte_source__ballotready_s3_office_holders_v3.sql:79`): `try_cast(tier as int) as tier`.
3. **Intermediate** (`int__civics_elected_official_ballotready.sql:90, 137`): passed through as `tier`.

The semantics inherit from BallotReady's `Position.tier` GraphQL field (an integer ranking on the underlying position). Exact business meaning is not yet confirmed with BallotReady (the [BR API docs](https://developers.civicengine.com/docs/api/graphql/reference/objects/position#positiontierint--) describe the field exists but not the semantics).

**Two reasons to rename now**, before introducing the new mart:

1. **Avoid a future column collision.** TechSpeed's elected-official intermediate (`int__civics_elected_official_techspeed`) also exposes a column called `tier`, with a different concept ("TechSpeed tier classification"). When TechSpeed is unioned into this mart post-entity-resolution, two unrelated semantics under the same column name would silently merge. Renaming now removes that landmine.
2. **Avoid baking an inaccurate name into a brand-new mart.** Better to land the new mart with a correctly-scoped, source-tagged column name than to rename it after consumers depend on it.

**Chosen name:** `br_position_tier`. The `br_` prefix explicitly tags the source system (per user preference).

**Out of scope for this rename:**

- The candidacies stream's `tier → geographic_tier` rename in `int__ballotready_clean_candidacies.sql` and downstream files. Different ticket, different stream, leave as-is.
- TechSpeed's `tier` column in `int__civics_elected_official_techspeed`. Will be addressed when entity resolution lands and TechSpeed is wired into the union.

---

## Deferred / out of scope

### Candidate / candidacy linkage

It would be useful to relate `elected_officials` to `candidate` and `candidacy` (an elected official is a person who once was a candidate, and won a particular candidacy). Investigating the existing UUID generation:

- `gp_candidate_id` is a deterministic salted hash of (`first_name`, `last_name`, `state`, `birth_date`, `email`, `phone`).
- `gp_candidacy_id` is a deterministic salted hash of (`first_name`, `last_name`, `state`, `party_affiliation`, `candidate_office`, election date, `district`).

A linkage to `candidate` could in principle be added today by re-deriving the same hash in the elected-officials intermediate using the same inputs. **It is intentionally not added in this ticket** for two reasons:

1. **Soft match quality.** Office-holder records frequently have sparse contact info; the hash would collapse to (name, state, null, null, null) for many rows, creating collisions among same-named officials in the same state.
2. **Symmetric reasoning to the TechSpeed deferral.** The user has already established that fingerprint matching across providers is too naïve and is gating TechSpeed integration on entity resolution. The same concern applies symmetrically here. Layering soft-matched `gp_candidate_id` into the new mart now would create the same kind of debt.

**Deferred to:** the entity resolution work that gates TechSpeed integration. When that lands, `gp_candidate_id` (and possibly `gp_candidacy_id`) should be surfaced from the resolution layer rather than re-hashed.

A `gp_candidacy_id` linkage would also require an `election_date` that the office-holder record does not cleanly carry (only term start/end dates), making it doubly inappropriate to attempt as a stop-gap.

### TechSpeed (and other providers)

`int__civics_elected_official_techspeed` already exists, but is not unioned into this mart. Per user direction, additional providers wait for entity resolution infrastructure. The `combined → deduplicated` CTE shape is in place precisely so that adding a provider later is a one-line change inside `combined` — but that change should not happen reflexively. It should be a deliberate decision after entity resolution has landed.

### Foreign-key tests

No `relationships` tests on `br_position_id ↔ election.br_position_database_id` or similar. The semantics of position joins between elected officials and elections are sane in principle but not yet validated against real data; introducing a `relationships` test that may quietly fail on edge cases (vacancies, special elections, position renumbering) would be premature.

---

## Build & verification plan

Per `dbt/project/CLAUDE.md`:

1. `dbt build --select elected_officials` — run + test the new mart.
2. `dbt run-operation inspect_data --args '{"model": "elected_officials"}'` — confirm row count, column types, null rates, and sample rows.
3. `dbt build --select "stg_airbyte_source__ballotready_s3_office_holders_v3+"` — confirm the upstream `tier → br_position_tier` rename doesn't break anything downstream of the staging model (the `+` suffix runs all descendants).
4. Grep the codebase to confirm no other model references `tier` from these specific upstream models by that name before committing.
5. Sanity-check via `dbt show --inline` that `elected_officials.br_position_id` joins meaningfully to `election.br_position_database_id` (informational only — no test added).

---

## Open follow-ups (separate tickets)

- **Confirm `br_position_tier` semantics** with BallotReady; add a description and accepted-range test once known.
- **Entity resolution** for elected_officials + candidate + candidacy linkage (gating ticket for TechSpeed integration here).
- **Rename `tier` in TechSpeed elected-official intermediate** when entity resolution lands and TechSpeed is unioned.
- **Reconcile `tier` naming in candidacies stream** (currently renamed to `geographic_tier`, which the user has flagged as likely inaccurate). Out of scope here; deserves its own investigation.
