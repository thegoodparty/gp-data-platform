# Repair patterns

Catalog of the override seeds already in the project and the chains they repair. Check here before inventing a mechanism — most "missing record" tickets in the election-api marts land on a rung that already has a repair path.

Verify any model or column named here against the live DAG before relying on it. These drift.

## Override seeds

All under `dbt/project/seeds/`, documented in `seeds_schema.yaml`.

| Seed | Keyed on | Repairs | Applied in |
|---|---|---|---|
| `l2_manual_district_assignments` | sparse tuple (state, county, city, precinct, district type) where null means "do not constrain" | a district L2 carries no geography for | `int__l2_nationwide_uniform` (a view, so edits need no voter-file rebuild) |
| `l2_br_match_overrides` | BR position `br_database_id` | LLM mapped a position to the wrong L2 district, scored below the confidence threshold, or has no match row at all | `m_election_api__position` |
| `br_position_place_overrides` | position geography (geo_id + mtfcc) | BallotReady puts a geography on a position but publishes no Place row for it | `int__enhanced_race` |
| `election_api_race_filing_address_overrides` | race `br_database_id` | BR supplies the wrong filing office address | `m_election_api__race` |
| `seed_civics_election_2025_position_nullouts` | — | civics position corrections | civics models |

Shared shape: the key, the corrected value, and a `reason` column that is `not_null` and carries a citation. Applied with a `coalesce` so the vendor stays authoritative wherever it supplies a value.

## Worked example: office missing from the picker

The chain a candidate's office travels to become selectable during onboarding. When an office is missing, walk it in order and stop at the first rung that is empty.

1. **`m_election_api__district`** — does the L2 district exist for (state, district type, district name)?
   No → L2 carries no geography for the seat. Add `l2_manual_district_assignments`.

2. **`m_election_api__position`** — is `district_id` populated for the BR position?
   No → the LLM match is wrong, below threshold (95 for `state` type, 90 otherwise), or absent. Add `l2_br_match_overrides`.

3. **`m_election_api__zip_to_position`** — are there zip rows, and is `pct_districtzip_to_zip` above the API's threshold (`PCT_DISTRICTZIP_TO_ZIP_THRESHOLD`, default 0.005)?
   Also note `future_elections` in this model only spans `current_date` to `+2 years`, so a position whose next election is further out will legitimately have no rows.

4. **`m_election_api__race`** — are there races for the position?
   No → check `int__enhanced_race.place_id`. Null there means BallotReady has no Place for the position's geography; add `br_position_place_overrides`. The race mart inner-joins the place mart because `Race.placeId` is a required FK that also supplies the race slug, so a placeless race cannot be served and is dropped.

Rung 4 is easy to miss because rungs 1-3 can all look healthy. The picker needs **both** a covered position and a published race: `ZipToPositionService.search` uses ZipToPosition only to collect `positionId`s, then reads the actual rows off `Race.positionId` with `electionDate >= today`. A position with no race lists nothing.

### Known gap

About 4,656 future races (1.7%, across 3,183 positions) carry no `place_id`. Almost all are BallotReady `X`-prefixed special districts (fire, justice precinct, and similar) where no census place exists to point at, so a geo_id remap does not fix them. If a ticket lands on one of these, it needs a different treatment than `br_position_place_overrides` — do not force the seed onto it.

## Geo_id mechanics

`int__enhanced_race` resolves a race's place by geo_id, rolling the position's geo_id up to a parent for certain MTFCCs (the `position_to_place_geo_id` case). The mapping table is linked from a comment in that model.

Useful when reading these: a 7-digit geo_id's parent is the state (first 2 digits), so "fall back to the parent geography" is not a usable generic fix for a school or special district — it would put the race under the state.

Census GEOIDs that look similar are different geographies. A place FIPS and a unified school district LEA code can both be 7 digits for the same state, which is why the override seed keys on MTFCC as well as geo_id.
