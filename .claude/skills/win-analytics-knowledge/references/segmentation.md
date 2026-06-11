# Segmentation dimensions reference

Part of the **win-analytics-knowledge** skill. Slicing the Win population.

## Quick reference

- **Business context:** the dimensions you cut Win analyses by — office level, type, state, party, Pro, ICP, viability.
- **Entity grain:** `users_win_candidacy` (one row per `campaign_version_id`); most dimensions are available there with no join.
- **Standard hygiene filter:** `is_latest_version AND NOT is_demo`. ICP is a **slicing dimension, not a filter** (DATA-1935).

## Routing triggers

- IF the dimension is in the table below → it's on `users_win_candidacy` directly (no join).
- IF you need `is_incumbent` / `is_open_seat` / `is_partisan` → join `mart_civics.candidacy` (see [joins.md](joins.md)).
- IF you're tempted to **filter** on `icp_office_win` → don't; slice instead (see ICP as dimension below).
- IF you need viability as a stratifier → see [viability.md](viability.md).

## Dimensions on `users_win_candidacy` (no join needed)

| Dimension | Coverage notes |
|---|---|
| `election_level` | ~55% non-NULL. Values: `city`, `county`, `state`, `federal`. **Large NULL bucket** — see [gotchas.md](gotchas.md). |
| `office_type` | HubSpot-sourced string. Common values: School Board, City Council, Mayor, Judge, State House (`m_civics.yaml:507-514`). |
| `campaign_state` | ~99% populated. |
| `campaign_party` | ~95% populated. |
| `election_date`, `primary_election_date`, `general_election_date`, runoff dates | Per-stage dates. Preserve cycle separation — do NOT use `users_win_base.election_date` for outcome analyses (it's a `coalesce(next, last)` that leaks). |
| `is_pro` | Boolean. Campaign-grain Pro flag at mart-materialization time. Pro upgrade TIMING comes from Amplitude (`int__amplitude_user_milestones.pro_upgrade_completed_at`). |
| `is_verified`, `is_demo`, `is_pledged`, `is_latest_version` | Quality / state flags. Default to `is_latest_version AND NOT is_demo`. |
| `icp_office_win`, `icp_office_serve`, `icp_win_supersize` | ICP flags. **Use as slicing dimensions, NOT filters** (per DATA-1935 resolved scope). |
| `is_judicial`, `is_appointed` | Non-traditional office flags. Often correlated with NULL ICP / NULL `election_level`. |
| `l2_district_name`, `l2_district_type`, `voter_count` | L2 district context — already surfaced from `int__icp_offices`. Use these instead of joining L2 directly. |

## Dimensions requiring a join to `mart_civics.candidacy`

- `is_incumbent` (TS-sourced; ~70% populated, sparse on BR-only)
- `is_open_seat` (BR > TS > DDHQ; NULL on BR-only)
- `is_partisan` (boolean)

## Upcoming / live election base

The standard "Win users with an election on or after date D" population. Resolve it one way:
take per-user `MAX(election_date)` over `is_latest_version AND NOT is_demo` candidacies,
**bounded to `[2020-01-01, 2050-01-01]`** (drops corrupt far-future dates — see
[gotchas.md](gotchas.md)), and keep users whose bounded max is `>= D`. Use `election_date`
(the most-populated per-stage field, ~41.5k of 59.8k users), not `users_win_base.election_date`
(the leaky coalesce). Users with no in-range `election_date` (~29%) are excluded by construction;
note that exclusion in the brief. This is the open-ended definition (any live/upcoming election,
including future cycles); a "current-cycle only" cut would instead cap the upper bound at the
cycle end.

## ICP as dimension, not filter

`icp_office_win` flags candidacies for offices Win supports. Originally proposed as a population filter; resolved scope (DATA-1935, 2026-05-27) says **slice, don't filter** because:

- ICP=true candidacies are competitive races by definition — filtering removes the comparison baseline.
- Reporting unfiltered first characterizes the broader Win population.
- ICP=true cohort tends to show LOWER raw win rate than ICP=false (~53% vs ~63%) — likely because Win supports harder races. This pattern only surfaces with a slice, not a filter.

## Cross-references

- [joins.md](joins.md) — the candidacy join for incumbency/open-seat/partisan dimensions.
- [viability.md](viability.md) — viability bands as a stratifier.
- [gotchas.md](gotchas.md) — the NULL `election_level` bucket and civics classification lag.
