# DATA-1894: Clean 2025-archive `br_position_database_id` collisions

Date: 2026-07-09
Status: Approved design, pending implementation plan

## Problem

`mart_civics.election` violates one-row-per `(br_position_database_id, election_date)` in
1,156 pairs (2,533 rows) in prod. All of them are 2025 elections originating in the
`archive_2025` branch (`int__civics_election_2025`). None come from the 2026+ BR/TS/DDHQ
merge that the original ticket described.

Root cause: in `int__civics_election_2025`, `gp_election_id` is hashed from the HubSpot
**contest** fields (correctly distinct per contest), but `br_position_database_id` is pulled
from the **candidacy** join (`tbl_candidacy.br_position_database_id`, joined on
`hubspot_contact_id = contest.contact_id`). That value is HubSpot's contact-to-BR-position
tag, and it is unreliable: candidacies for genuinely different offices get pinned to a single
BR position.

Two sub-populations (bucketed 2026-07-09):

- ~417 pairs are **mis-tags** — the contest's real office differs from the BR position's
  office (155 differ on office level, 7 on office type, 255 on candidate office). Example:
  BR position 47891 is "State Senator", but a "State House" contest is tagged to it; BR
  position 58211 is "Local School Board", but a "City Select Board" contest is tagged to it.
- ~739 pairs are **same-office name variants** — the same office fragmented into multiple
  `gp_election_id`s by inconsistent contest name strings. These all carry a *correct*
  `br_position_database_id` and still collide.

No current downstream consumer is broken by this: `m_election_api__zip_to_position` is
future-only and already dedups on `(br_position_database_id, election_date)`;
`candidacy_techspeed` and `candidacy_hubspot` join `election` on `gp_election_id`, not on the
position id. The fix is data-hygiene: make the archive tag trustworthy and unique so future
consumers and historical joins are safe.

The archive is a **frozen 2026-01-22 HubSpot snapshot**, so a one-time authoritative
resolution is preferred over a live heuristic — the data never changes and the archive
`gp_election_id` does not drift.

## Goal

For the 2025 archive, `br_position_database_id` must be:
- **Trustworthy** — if a row has `br_position_database_id = X`, that row's office really is BR
  position X's office.
- **Unique** — at most one row per `(br_position_database_id, election_date)` carries a
  non-null tag.

## Design

> **Revision 2026-07-09 (hybrid).** Implementation found that the archive `gp_election_id`
> is not perfectly stable across builds — it rides a live SCD snapshot, and ~1.7% of ids
> drift between builds (new/changed HubSpot contest rows). A pure `gp_election_id` seed
> therefore leaves residual collisions on a fresh build and would periodically fail a hard
> uniqueness test in prod. The design is revised to split the fix by mechanism:
>
> - **Collapse (the `duplicate_variant` case) moves in-model.** `int__civics_election_2025`
>   nulls `br_position_database_id` on all but the completeness-winner per
>   `(br_position_database_id, election_date)`, computed live. Uniqueness becomes a build-time
>   invariant, drift-proof, and needs no seed.
> - **Validation (the `office_mismatch` case) stays a seed** — the human-judged list of genuine
>   mis-tags, now the seed's only content (~78 rows). If a mismatch id drifts, uniqueness still
>   holds (collapse guarantees it) and the seed-integrity relationships test is `warn`-level, so
>   drift alerts a refresh rather than failing CI.
>
> Sections 1-4 below describe the original pure-seed approach; where they conflict with this
> revision, the revision governs. Net changes: seed is `office_mismatch`-only; model adds a
> deterministic collapse CTE; seed relationships test is `warn`.

### 1. Core operation: null, never drop

Rows are never deleted (2025 `gp_election_id`s are FKs in candidacy / election-stage;
dropping would orphan them). We set `br_position_database_id = NULL` on the rows that should
not carry it. After the fix, exactly one row per group keeps the non-null tag.

A row is nulled for one of two reasons:
- `office_mismatch` — contest office differs from the BR position's office (validation).
- `duplicate_variant` — same office as the winner, a name-variant fragment (collapse).

### 2. The seed

A committed CSV seed, `seed_civics_election_2025_position_nullouts`, grain one row per
`gp_election_id` to null:

| column | purpose |
|---|---|
| `gp_election_id` | key the model nulls on |
| `br_position_database_id` | audit / readability |
| `election_date` | audit |
| `reason` | `office_mismatch` or `duplicate_variant` |
| `contest_office` | human-readable audit (contest side) |
| `br_position_name` | human-readable audit (BR truth side) |

`int__civics_election_2025` applies it at the point `br_position_database_id` is selected:

```sql
case
    when gp_election_id in (
        select gp_election_id from {{ ref("seed_civics_election_2025_position_nullouts") }}
    )
    then null
    else tbl_candidacy.br_position_database_id
end as br_position_database_id
```

Single point of change; propagates to the mart `archive_2025` branch automatically. Durable
because the archive `gp_election_id` hashes a frozen snapshot and does not re-roll.

### 3. Seed generation (one-time)

1. Extract the collision set from `int__civics_election_2025` with comparison fields:
   `gp_election_id`, `br_position_database_id`, `election_date`, `official_office_name`,
   `candidate_office`, `contest_office_type`, `contest_office_level`, `district`, `seat_name`,
   `br_normalized_position_type`, `has_ddhq_match`, `updated_at`.
2. Classify each row against its BR position's true office: use structured
   `contest_office_type` vs BR normalized type where usable; LLM-assisted on the noisy /
   "Other" / null cases.
3. Pick the winner among genuine matches deterministically:
   `official_office_name is not null` first, then `has_ddhq_match desc`, then
   `updated_at desc` (mirrors the existing `archived_elections` qualify).
4. Emit every non-winner as a null-out row with its reason. Groups where no row matches the BR
   office null all of them.
5. Human review: all low-confidence LLM calls plus a random sample of the rest.

Expected ~1,377 null-out rows (2,533 collision rows minus ~1,156 winners). Classification is
automated-with-review, not hand-typed. The generation script/query is kept for auditability
but is not part of the dbt DAG.

### 4. Testing

- **Hard**: `dbt_utils.unique_combination_of_columns` on `int__civics_election_2025` for
  `(br_position_database_id, election_date)` where `br_position_database_id is not null`.
- **Seed integrity**: a check that every seed `gp_election_id` still exists in
  `int__civics_election_2025` (catches seed rot if the archive is ever rebuilt).
- **Not** a hard uniqueness test on the full `mart_civics.election`, because the separate,
  still-live 2026 transient-staleness splits could make it flake. A `warn`-level mart test is
  optional.

### 5. Out of scope

- The 2026 BR/TS transient staleness (separate root cause; self-heals on daily refresh).
- Re-deriving the correct BR position for nulled mis-tag rows (we remove the wrong tag; we do
  not find the right one).

## Plan-time assumptions to verify

1. `int__hubspot_contest_2025` sources a static/frozen archive, so archive `gp_election_id`
   is stable and the seed keyed on it will not rot on the next run.
2. Nulling archive `br_position_database_id` has no downstream harm: the `election.sql` joins
   to `int__icp_offices` and `int__civics_position_office_type` are LEFT joins on the position
   id, so nulled rows simply get null ICP / office-type enrichment (correct, since the
   position is unknown).
