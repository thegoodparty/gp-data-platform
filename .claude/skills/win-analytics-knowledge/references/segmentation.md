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
| `campaign_party` | ⚠ **~46% populated, not ~95%** (verified 2026-07-23, DATA-2153): 32,625/60,491 (53.9%) NULL across latest non-demo candidacies; ~45% NULL in the 2025-2026 generals voter>=1000 population. Values are also fragmented across casing/format - `independent`/`Independent`, `nonpartisan`/`Non-partisan`, `green`/`Green Party`, `forward`/`Forward Party` - normalize (lowercase + strip ` Party`) before grouping as a cut. |
| `election_date`, `primary_election_date`, `general_election_date`, runoff dates | Per-stage dates. Preserve cycle separation — do NOT use `users_win_base.election_date` for outcome analyses (it's a `coalesce(next, last)` that leaks). |
| `is_pro` | Boolean. Campaign-grain Pro flag at mart-materialization time — as-of-today, so it misses Pros who churned (Nov-2025 bucket: 752 Pro-at-election vs 421 Pro-today). **Point-in-time Pro tenure comes from Stripe**: `dbt.stg_airbyte_source__stripe_api_subscriptions` + `_customers` joined on `LOWER(email)` (`is_livemode`, exclude `subscription_status='incomplete_expired'`; count an open-ended interval only while `subscription_status='active'`). For **lifetime revenue and current subscription state at user grain, read `dbt.int__user_revenue_profile` instead of rolling your own** — it carries `lifetime_paid_usd`, the per-line split, `subscription_status` and `pro_since` with a `pro_since_source`, and it reads charges rather than invoices, so it sees the one-off and sales-led revenue an invoice-only total misses (subscription billing is about a third of collections). Note the subscriptions staging model now names and casts its columns: `customer`/`status`/`livemode`/`start_date` are `stripe_customer_id`/`subscription_status`/`is_livemode`/`started_at`, and the timestamps arrive already converted. A **direct ID join** also exists: `gp_api_db_campaign.details:$.subscriptionId` → `subscriptions.id` (485/1,296 current Pros; +84 users the email join misses). The Amplitude milestone (`int__amplitude_user_milestones.pro_upgrade_completed_at`, 2025-05-29+) covers only ~30% of current Pros; HubSpot company pro dates are near-empty and are fed FROM the product's `details:$.isProUpdatedAt`. **The product DB stores no first-Pro timestamp** (exhaustive sweep 2026-07-23): `isPro` is set only by the Stripe `checkout.session.completed`/`subscription.resumed` webhooks, cleared by `subscription.deleted`, and admin-flippable with no billing trail. What exists in the campaign `details` blob: `isProUpdatedAt` = LAST isPro flip (overwritten on cancels too — not a first-upgrade date; ~46% of current Pros), `proUpgradeSlackNotifiedAt` (2026-05+, ~150), `freeTextsOfferRedeemedAt` (2026-01+; the 5k free-texts offer is granted on FIRST Pro upgrade, so it's a first-Pro proxy, not a free tier). Stripe `events` (checkout/subscription lifecycle) sync only from 2025-08-25. ⚠ data-state (2026-07-23): ~54% of current Pros (≈700) have no timing evidence in ANY channel — concentrated in accounts created 2025-01..07, consistent with comped/admin grants plus billing-email mismatch (~590 orphan livemode subscriptions match no account by email or ID) — so pair any point-in-time Pro count with the as-of-today flag as a reference column. Worked recipe: `analytics/projects/win_topline_reporting/topline_report.py`. **Coverage is cohort- and period-dependent (verified 2026-07-24, DATA-2183):** undatable-by-any-channel runs 21% of 2026-period current Pros but 71% of Nov-2025-period (driver: only 21% of Nov-2025 Pros match *any* Stripe record vs 67% for 2026 - comped/sales-granted Pro plus billing-email mismatch). For behavioral cohorts prefer **ever-upgraded** (`pro_upgrade_completed_at IS NOT NULL`, event live 2025-05-29..present) over `is_pro`: it covers 65% of Nov-2025 engaged senders and surfaces churned ex-Pros the flag hides (522 in that window) - the lens flip moved a dashboard-only Pro read from 11% to 31%. Churn-dating floor: `Account - Pro Subscription Canceled` (2025-09-17+, in-product cancels only); Stripe intervals stay authoritative. Backend fix ticketed DATA-2185. |
| `is_verified`, `is_demo`, `is_pledged`, `is_latest_version` | Quality / state flags. Default to `is_latest_version AND NOT is_demo`. |
| `icp_office_win`, `icp_office_serve`, `icp_win_supersize` | ICP flags. **Use as slicing dimensions, NOT filters** (per DATA-1935 resolved scope). |
| `is_judicial`, `is_appointed` | Non-traditional office flags. Often correlated with NULL ICP / NULL `election_level`. |
| `l2_district_name`, `l2_district_type`, `voter_count` | L2 district context — already surfaced from `int__icp_offices`. Use these instead of joining L2 directly. |

## Dimensions requiring a join to `mart_civics.candidacy`

- `is_incumbent` (TS-sourced; ~70% populated, sparse on BR-only)
- `is_open_seat` (BR > TS > DDHQ; NULL on BR-only)
- `is_partisan` (boolean)

## Account provenance (join to `dbt.stg_airbyte_source__gp_api_db_user`)

`has_password` is the **signup-channel** dimension **for accounts created before 2026-04-01**, and
the strongest Win engagement predictor measured on that population (CV AUC 0.70 power, 0.79 touch;
ranked first in every subset where it varies). It is not on `users_win_candidacy` - join from the
gp-api user staging table on `user_id`:

```sql
left join goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user us
  on cast(us.id as string) = cast(u.user_id as string)
```

- `false` = sales-created off a roster, `true` = self-signup. Roll to candidacy grain with
  `MAX(...)`, which is where the working set's `usr_has_password` comes from.
- **Dead as a channel marker from April 2026.** Signup moved to magic links, so every account created
  from 2026-05-01 onward has `has_password = false` regardless of how it arrived, and April 2026
  is the transition month (measured 2026-09-24: 0% of May to September 2026 signups carry a
  password; April runs ~50%; January to March ~80%; 2025 99.9%). A 2026-book cut that labels
  no-password accounts "sales-created" is mislabeling magic-link self-signups. For accounts
  created from 2026-04-01 onward, read channel from whether a dated HubSpot sales touch predates
  the account, which is the check the DATA-2239 audit used to validate the flag in the first
  place. Left-join `mart_analytics.prospects` on the product user id; `u` here is
  `users_win_candidacy`, which carries `user_created_at`:

  ```sql
  left join goodparty_data_catalog.mart_analytics.prospects pr
    on cast(pr.gp_user_id as string) = cast(u.user_id as string)
  -- sales-sourced when:
  --   pr.first_sales_touch_at is not null
  --   and cast(pr.first_sales_touch_at as date) < cast(u.user_created_at as date)
  ```

  Read the join result as three buckets, not two. **Sales-sourced**: a dated touch strictly before
  the signup day. **Not sales-sourced**: no `prospects` row at all, or a dated touch on or after
  the signup day, since a touch that follows the signup cannot have sourced it (sales reached a
  self-signup after the fact). **Undated**: a row whose `first_sales_touch_at` is NULL. The mart's gate admits contacts on Win-stage strings and
  pledge or opt-in flags that carry no timestamp, and those flags are set for ordinary
  self-signups by product automation, so an undated row is evidence of neither channel. The
  sentinel `9999-01-01` the mart uses inside its `least()` is stripped to NULL before
  materialization and never appears in the column. It must be a LEFT join: an inner join drops
  every organic self-signup from the denominator. Measured 2026-09-24 on latest-version, non-demo
  users created from May 2026 onward (N=2,224): 179 sales-sourced (8%); 507 not sales-sourced
  (23%), of which 334 have no row, 150 a dated touch after the signup day, and 23 a
  midnight-stamped HubSpot date on the signup day itself; 1,538 undated (69%). The 23 same-day
  cases land in not-sales-sourced deliberately: HubSpot lifecycle dates carry no time of day, so
  same-day ordering is unknowable, and the date cast keeps the definition identical to the
  audit's. Admin-created accounts also stopped in
  early 2026, so accounts from April onward are self-signups unless the check says otherwise.
- Clean as a stratifier: it was deliberately excluded from the corroboration model, so cutting by
  it is not restating a model input.
- Corroborated rows are 69.9% roster against the frame's 57.7%, so **any** corroboration-filtered
  read shifts channel mix and is partly a statement about acquisition rather than about candidates.
- Missingness on race-context fields tracks this dimension - see [gotchas.md](gotchas.md).
- Reference: `audit_haspw_provenance.py` (DATA-2239 provenance audit), `predictor_ranking.py`
  (DATA-2247 CV ranking).

## Upcoming / live election base

The standard "Win users with an election on or after date D" population. Resolve it one way:
take per-user `MAX(election_date)` over `is_latest_version AND NOT is_demo` candidacies,
**bounded to `[2020-01-01, 2050-01-01]`** (drops corrupt far-future dates — apply the same
bound to any per-stage date column you aggregate; see [gotchas.md](gotchas.md)), and keep
users whose bounded max is `>= D`. Use `election_date`
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
