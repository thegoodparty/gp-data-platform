# DDHQ election_stage coverage gaps (vs BallotReady) — design

DATA-1582 follow-on. (Ticket: continuing the DATA-1582 lineage unless a new ticket is assigned.)

## Purpose

A committed dbt **diagnostic** model that lists every BallotReady local 2026+
election_stage that has **no matched DDHQ counterpart** — i.e. races BR covers
but DDHQ does not. Used to find and triage gaps in DDHQ election coverage.

It is a QA/diagnostic artifact, not a consumer-facing mart: lighter on tests and
docs, lives alongside the other ER intermediate models.

## Grain

One row per BallotReady `gp_election_stage_id` (a BR race/stage) that is in
scope and has no DDHQ match.

## Scope filters

A BR election_stage is in scope when:

- `election_date >= '2026-01-01'`, and
- `office_level NOT IN ('State', 'Federal')` — i.e. local: City, County,
  Township, Local, Regional. (Regional is included by "non-state and
  non-federal"; it is multi-jurisdiction local, not state/federal.)

## How "DDHQ doesn't have it" is determined

Via the existing entity-resolution crosswalk
`int__civics_er_canonical_election_stages` (the maintained dependency), **not** a
naive office/geo join.

- A BR race "has a DDHQ match" iff its `gp_election_stage_id` appears as the
  `canonical_gp_election_stage_id` of a DDHQ row in the crosswalk:
  `SELECT DISTINCT canonical_gp_election_stage_id FROM
  int__civics_er_canonical_election_stages WHERE source_name = 'ddhq'`.
  (For BR-anchored clusters the canonical is the BR race's own
  `gp_election_stage_id`; non-BR DDHQ clusters carry a non-BR canonical, so they
  correctly do not match any BR id.)
- **Gap = in-scope BR races whose `gp_election_stage_id` is NOT in that set.**

The same pattern against `source_name = 'techspeed'` yields the
`has_techspeed_match` flag.

### Accuracy caveat

In the developer dev schema the crosswalk is currently built from the
**OR-baseline** matcher output (no anchors), so dev row counts **overstate** the
gap. The model logic is correct and self-corrects once prod's crosswalk is
rebuilt from the final-config matcher (post-merge prod refresh + prod matcha
re-run). No model change needed for that to take effect.

## Logic (CTEs)

1. `br_local` — `int__civics_election_stage_ballotready` filtered to the scope
   above. `state` is joined in from the BallotReady position staging
   (`stg_airbyte_source__ballotready_api_position` via `br_position_id`), the way
   `int__er_prematch_election_stages` does, since the BR stage model has no
   native `state` column.
2. `ddhq_matched` / `ts_matched` — distinct `canonical_gp_election_stage_id`
   from the crosswalk for `source_name = 'ddhq'` / `'techspeed'`.
3. `br_candidacy_counts` — count of BR candidacies per `br_race_id`, from the BR
   candidacy model (exact ref resolved in implementation).
4. Final — `br_local` anti-joined to `ddhq_matched` on `gp_election_stage_id`,
   enriched with `has_techspeed_match` and `br_candidate_count`.

## Output columns

| column | description |
|---|---|
| `gp_election_stage_id` | BR race/stage id (PK) |
| `br_race_id` | BallotReady race id |
| `state` | 2-letter state |
| `official_office_name` | BR office/race name |
| `candidate_office` | normalized office |
| `office_level` | City/County/Township/Local/Regional |
| `office_type` | office type |
| `district_identifier` | district number, when present |
| `seat_name` | seat, when present |
| `election_date` | election date (>= 2026-01-01) |
| `stage_type` | primary/general/etc |
| `has_techspeed_match` | bool — TS covered it even though DDHQ didn't |
| `br_candidate_count` | number of BR candidacies on the race |

## Location & name

`models/intermediate/civics/int__civics_ddhq_election_stage_coverage_gaps.sql`.
Alongside the other ER intermediate models; no `qa/` folder exists and adding one
for a single model is not worth it. The 2026 cutoff is a `where` clause, not part
of the name, so the name does not rot.

## Tests

Light, befitting a diagnostic model:

- `unique` + `not_null` on `gp_election_stage_id`
- `not_null` on `br_race_id`
- schema-doc entry describing the diagnostic intent

## Out of scope

- Wiring this into any mart or consumer.
- Changing the matcher or the crosswalk.
- TS or cross-source gap variants (DDHQ-vs-BR only for this PR).
