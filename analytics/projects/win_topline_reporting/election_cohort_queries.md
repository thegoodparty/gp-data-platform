# Election cohort: numbers and the BallotReady match gap (DATA-2599 / DATA-2603)

Follow-up to DATA-2599 (Nov 3 cohort counts). Two parallel threads (sessions `d-2599` and `d-2600`) converged on the same headline numbers and the same unexplained gap: most Win users with a 2026-11-03 election, including a large share of paying users who told us they are on the ballot, have no BallotReady candidacy match. This ticket is to settle why, and to decide whether the roster can be used as a confirmation source for the Nov 3 report.

## What is established (as of 2026-10-01, staff `@goodparty.org` excluded)

| | Users | Pro | Activated (governed) |
|---|---|---|---|
| Election on 11/3 | 9,935 | 578 | 354 |
| Self-reported on-ballot in onboarding | 1,173 | 291 | 94 |

Both threads reproduce these exactly once staff are excluded (DATA-2599 comments). Activated moves hourly with the Amplitude load, so every number carries its timestamp.

**BallotReady match for the 11/3 cohort, three independent instruments:**

| Instrument | Matched to a BallotReady candidacy |
|---|---|
| `mart_civics.candidacy_stage.br_candidacy_id` via `candidacy.product_campaign_id` | 1,521 (1,742 via the person key) |
| Entity-resolution clusters read directly (`stg_er_source__clustered_candidacy_stages`) | 1,634 |
| Name match (same last name, same first name or initial) inside the user's own 11/3 race roster | 1,683 |

So about 16% of the cohort. For self-reported on-ballot users: 507 to 531 by the mart and ER paths, 618 of the 969 whose race has a roster by name match (64%). Pro and on-ballot: 179 of 291 (62%).

## What we already checked and ruled out

- **Stale BallotReady data.** No. Candidacies loaded 2026-09-28 (weekly, Mondays), races and filing periods 2026-09-30, civics mart rebuilt 2026-10-01 10:51.
- **Wrong instrument.** Partly, now corrected. "In the civics mart" is not a match flag: `int__civics_candidacy_gp_api` only admits a product campaign once ER clustered it with a vendor record or HubSpot verified the contact (2,593 of 9,935 cohort users are admitted). Both threads initially reported inflated rates from this (26% / 71% and 80%); both corrected on the ticket.
- **Entity resolution broadly failing.** No. 9,604 of 9,935 cohort campaigns enter ER. ER and the in-race name match agree within 3% overall. ER under-matches on-ballot users by about 90 to 150 net (name match finds 618, ER 531); that is an 8 to 13% tail, not the gap.
- **The race is unknown.** No. 9,674 cohort users (97%) carry a `ballotready_position_id` from signup and 9,633 resolve to an 11/3 general race. 8,800 have a filing deadline on file, all in the past.
- **Filing-deadline lag has flushed through.** No, and this is the main finding from thread d-2599. On this cycle's cohort races, only 26% of roster rows existed by the filing deadline; median lag 36 days, p90 140 days; 42% of rows arrived more than 40 days after the deadline. 3,928 roster rows were created in August and 3,132 in September; the August-deadline block (1,954 races) added 2,028 rows in September alone. Thread d-2600 measured the match rate flat across deadline age (25% under 40 days, 26% over), which is consistent with a roster that is still filling for most deadline months.
- **Users have no race at all.** Small. 85 on-ballot users have no BR position; 6 of the 20 users sampled for the spot-check had none.

## Hypotheses, ranked, with the test that settles each

1. **BallotReady is still loading these races** (d-2599). Prediction: the match rate for the cohort rises materially through October. Test: re-run query C weekly and record it on this ticket. Cheap, already scripted.
2. **Wrong race picked at onboarding** (d-2600). The user's `ballotready_position_id` points at a different office or district than the one they filed for, so ER and the in-race name test both look in the wrong roster. Test: for the ~110 Pro + on-ballot unmatched users, search the BallotReady roster statewide by name, not within the race; and the 20-user web spot-check already launched in d-2600 (results pending).
3. **BallotReady local coverage gap** (both). Races BallotReady never fills. Test: among past-40-day unmatched on-ballot users, 39 are in races with no roster at all and 35 in races with a roster of one; the 249 in races with 2+ candidates are the ones this hypothesis does not explain and the spot-check must cover.
4. **ER under-match** (bounded by both threads). 130 to 150 on-ballot users name-match in their own race but are not clustered. Test: hand-review a sample with the matcher owner; if confirmed, these are matcher misses with a known fix surface.
5. **Self-report is wrong** (both). The user said on-ballot and did not file, or is a write-in. Not separable from the warehouse; the spot-check is the only test.
6. **Ballot-status column not backfilled.** 318 to 539 on-ballot answers exist only in the `data.onboarding` JSON snapshot, not in the `ballot_status` column that landed 2026-08-25. Affects the denominator, not the match. Test: confirm with omni whether the column was meant to be backfilled.

## Deliverables

- A one-paragraph answer to "can we use BallotReady to confirm on-ballot status for the Nov 3 report, and from what date".
- The weekly match-rate series through election day (hypothesis 1).
- Spot-check verdicts for the Pro + on-ballot unmatched sample, classified per hypothesis 2 to 5.
- A sample CSV of the ~130 name-match-but-unclustered on-ballot users for the matcher owner.
- Filed separately if confirmed: `ballot_status` backfill (omni), and surfacing `ballot_status` plus `signup_goal` into staging and `users_win_candidacy` (dbt).

Related, not in scope here: the governed activation metric counts pre-2026-01-09 self-reports tagged `method = 'unknown'` (98 of the cohort's 354, 340 of 1,307 overall). That is a semantic-layer change and is being raised with the metric owner on DATA-2599.

## Appendix: exact queries (Databricks SQL, `goodparty_data_catalog`)

The SQL below matches `election_cohort.py` after PR #1129 review round 2: the ballot answer is resolved by commitment rank rather than a lexicographic `max()`, and the rule-consistent activation flag treats a user with no qualifying event at all as not-a-self-report. The DATA-2603 ticket description carries the originally filed text without those two fixes; numbers on the 2026-11-03 cohort are identical either way (0 users with more than one answer, 0 activated users without event evidence). The internal-account exclusion below is also the canonical `int__civics_internal_persons` rule used by the script (231 accounts on 2026-10-01, giving 9,931 users), where the filed ticket text and the narrative above used the older `@goodparty.org` proxy (225 accounts, 9,935 users); Pro and activation counts are the same under both.

Script and brief: `election_cohort.py` and `election_cohort_brief.yaml` in this folder. Text above is the DATA-2603 ticket description as filed on 2026-10-01; corrections posted as comments on the ticket (the `unknown` events ran until 2026-04-28 in data, from the legacy LogTaskModal, and omni's 2026-08-25 migration did backfill `ballot_status`).

### A. Headline: cohort, Pro, activated, ballot answer

```sql
-- DATA-2599 headline: Win users with an election on 2026-11-03, Pro, activated, ballot answer.
with cand as (
    select user_id, lower(user_email) as email, cast(campaign_id as string) as campaign_id,
           is_pro, election_date, user_created_at
    from goodparty_data_catalog.mart_analytics.users_win_candidacy
    where is_latest_version and not is_demo
      and election_date = date'2026-11-03'
      and cast(user_id as string) not in (
        -- canonical internal rule, same as int__civics_internal_persons and election_cohort.py
        select cast(id as string) from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user
        where regexp_extract(lower(email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
           or arrays_overlap(from_json(roles, 'array<string>'), array('admin', 'sales')))
),
ballot as (
    -- product column landed 2026-08-25; earlier answers live only in the JSON archive
    select c.user_id,
           coalesce(rc.ballot_status, get_json_object(rc.data, '$.onboarding.ballotStatus')) as ballot_status
    from cand c
    join goodparty_data_catalog.airbyte_source.gp_api_db_campaign rc on cast(rc.id as string) = c.campaign_id
),
users as (
    select c.user_id, min(c.user_created_at) as user_created_at,
           max(case when c.is_pro then 1 else 0 end) as is_pro,
           -- most-committed answer wins across a user's campaigns; a plain max() is lexicographic
           case max(case b.ballot_status when 'on-ballot' then 4 when 'qualified-not-filed' then 3 when 'considering' then 2 when 'testing' then 1 end) when 4 then 'on-ballot' when 3 then 'qualified-not-filed' when 2 then 'considering' when 1 then 'testing' end as ballot_status
    from cand c left join ballot b on b.user_id = c.user_id
    group by c.user_id
),
-- activation evidence per user, by whether it is the pre-2026-01-09 self-report (method = 'unknown')
act_ev as (
    select cast(user_id as bigint) as user_id,
           sum(case when get_json_object(event_properties, '$.method') = 'unknown' then 0 else 1 end) as non_legacy_ev
    from goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events
    where event_type in ('Voter Outreach - Campaign Completed', 'Outreach - Campaign Completed',
                         'Voter Outreach - Campaign Scheduled', 'Outreach - Phone Banking: Complete',
                         'Door Knocking - Door Logged', 'Outreach - Door Knocking Door Logged',
                         'Outreach - Phone Banking: Call Logged', 'Outreach - Phone Banking Call Logged')
      and coalesce(get_json_object(event_properties, '$.method'), '') <> 'manual'
      and coalesce(get_json_object(event_properties, '$.product'), '') <> 'serve'
      and try_cast(user_id as bigint) is not null
    group by 1
),
-- product-DB reference: an outreach row that committed and was not canceled
sent as (
    select sc.user_id
    from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_outreach o
    join goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_campaign sc on sc.id = o.campaignId
    where o.canceled_at is null and (o.status in ('completed', 'in_progress') or o.approved_at is not null)
    group by sc.user_id
),
final as (
    select u.user_id, u.is_pro,
           coalesce(b.is_activated, false) as is_activated,
           -- NULL non_legacy_ev = no qualifying event at all (a leg mismatch, not a self-report); only an explicit 0 excludes
           coalesce(b.is_activated, false) and (e.non_legacy_ev is null or e.non_legacy_ev > 0) as is_activated_strict,
           s.user_id is not null as has_committed_send,
           case when u.ballot_status is not null then u.ballot_status
                when u.user_created_at < timestamp'2026-05-07' then 'never asked (pre-2026-05-07)'
                else 'asked, no answer' end as ballot_bucket
    from users u
    left join goodparty_data_catalog.mart_analytics.users_win_base b on b.user_id = u.user_id
    left join act_ev e on e.user_id = u.user_id
    left join sent s on s.user_id = u.user_id
)
select ballot_bucket, count(*) as users, sum(is_pro) as pro,
       sum(case when is_activated then 1 else 0 end) as activated_governed,
       sum(case when is_activated_strict then 1 else 0 end) as activated_strict,
       sum(case when has_committed_send then 1 else 0 end) as product_db_committed_send
from final
group by grouping sets ((ballot_bucket), ())
order by users desc
```

### B. Simple check: civics mart row and provider ids, plus the person-key path

```sql
-- Q1 the simple check: cohort user -> civics mart candidacy (via product campaign id) -> stage ids
with cand as (
    select user_id, cast(campaign_id as string) campaign_id, is_verified
    from goodparty_data_catalog.mart_analytics.users_win_candidacy
    where is_latest_version and not is_demo and election_date = date'2026-11-03' and cast(user_id as string) not in (
        -- canonical internal rule, same as int__civics_internal_persons and election_cohort.py
        select cast(id as string) from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user
        where regexp_extract(lower(email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
           or arrays_overlap(from_json(roles, 'array<string>'), array('admin', 'sales')))
),
ballot as (select c.user_id, case max(case coalesce(rc.ballot_status, get_json_object(rc.data,'$.onboarding.ballotStatus')) when 'on-ballot' then 4 when 'qualified-not-filed' then 3 when 'considering' then 2 when 'testing' then 1 end) when 4 then 'on-ballot' when 3 then 'qualified-not-filed' when 2 then 'considering' when 1 then 'testing' end ballot_status from cand c join goodparty_data_catalog.airbyte_source.gp_api_db_campaign rc on cast(rc.id as string) = c.campaign_id group by 1),
j as (
    select c.user_id, coalesce(b.ballot_status,'never asked') bucket, max(c.is_verified) is_verified,
           max(case when cd.gp_candidacy_id is not null then 1 else 0 end) in_civics_mart,
           max(case when cs.br_candidacy_id is not null then 1 else 0 end) has_br_candidacy_id,
           max(case when cs.ts_source_candidate_id is not null then 1 else 0 end) has_ts_id,
           max(case when cs.ddhq_candidate_id is not null then 1 else 0 end) has_ddhq_id
    from cand c left join ballot b on b.user_id = c.user_id
    left join goodparty_data_catalog.mart_civics.candidacy cd on cd.product_campaign_id = c.campaign_id
    left join goodparty_data_catalog.mart_civics.candidacy_stage cs on cs.gp_candidacy_id = cd.gp_candidacy_id
    group by c.user_id, b.ballot_status
)
select bucket, count(*) users, sum(in_civics_mart) in_civics_mart, sum(has_br_candidacy_id) has_br_candidacy_id, sum(has_ts_id) has_ts_id, sum(has_ddhq_id) has_ddhq_id,
       count_if(in_civics_mart = 1 and has_br_candidacy_id = 0 and has_ts_id = 0 and has_ddhq_id = 0) in_mart_no_provider_id,
       count_if(in_civics_mart = 1 and has_br_candidacy_id = 0 and has_ts_id = 0 and has_ddhq_id = 0 and is_verified) of_which_hubspot_verified
from j group by grouping sets ((bucket), ()) order by users desc

-- Q2 alternate path: user -> gp_person_id -> candidate -> any candidacy -> stage with br_candidacy_id
select count(distinct c.user_id) cohort_users,
       count(distinct case when k.gp_person_id is not null then c.user_id end) has_person_key,
       count(distinct case when ca.gp_candidate_id is not null then c.user_id end) candidate_row,
       count(distinct case when cs.br_candidacy_id is not null then c.user_id end) br_candidacy_via_person
from goodparty_data_catalog.mart_analytics.users_win_candidacy c
left join goodparty_data_catalog.mart_analytics.user_resolved_keys k on k.user_id = c.user_id
left join goodparty_data_catalog.mart_civics.candidate ca on ca.gp_person_id = k.gp_person_id
left join goodparty_data_catalog.mart_civics.candidacy cd on cd.gp_candidate_id = ca.gp_candidate_id
left join goodparty_data_catalog.mart_civics.candidacy_stage cs on cs.gp_candidacy_id = cd.gp_candidacy_id
where c.is_latest_version and not c.is_demo and c.election_date = date'2026-11-03' and cast(c.user_id as string) not in (
        -- canonical internal rule, same as int__civics_internal_persons and election_cohort.py
        select cast(id as string) from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user
        where regexp_extract(lower(email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
           or arrays_overlap(from_json(roles, 'array<string>'), array('admin', 'sales')))
```

### C. Entity-resolution clusters vs in-race name match, by ballot answer

```sql
-- Q3 independent name-level match vs ER cluster, by ballot bucket (cohort users with an 11/3 race)
with cand as (
    select user_id, cast(campaign_id as string) campaign_id, ballotready_position_id, lower(trim(user_first_name)) fn, lower(trim(user_last_name)) ln
    from goodparty_data_catalog.mart_analytics.users_win_candidacy
    where is_latest_version and not is_demo and election_date = date'2026-11-03' and cast(user_id as string) not in (
        -- canonical internal rule, same as int__civics_internal_persons and election_cohort.py
        select cast(id as string) from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user
        where regexp_extract(lower(email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
           or arrays_overlap(from_json(roles, 'array<string>'), array('admin', 'sales')))
),
ballot as (select c.user_id, case max(case coalesce(rc.ballot_status, get_json_object(rc.data,'$.onboarding.ballotStatus')) when 'on-ballot' then 4 when 'qualified-not-filed' then 3 when 'considering' then 2 when 'testing' then 1 end) when 4 then 'on-ballot' when 3 then 'qualified-not-filed' when 2 then 'considering' when 1 then 'testing' end ballot_status from cand c join goodparty_data_catalog.airbyte_source.gp_api_db_campaign rc on cast(rc.id as string) = c.campaign_id group by 1),
rg as (select r.database_id br_race_id, r.position.databaseid br_position_id from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e on r.election.databaseid = e.database_id where not coalesce(r.is_primary,false) and not coalesce(r.is_runoff,false) and not coalesce(r.is_recall,false) and e.election_day = date'2026-11-03'),
roster as (select cast(br_race_id as string) br_race_id, lower(trim(first_name)) fn, lower(trim(last_name)) ln from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3),
er as (
    select g.source_id campaign_id, max(case when b.cluster_id is not null then 1 else 0 end) er_br
    from goodparty_data_catalog.dbt.stg_er_source__clustered_candidacy_stages g
    left join (select distinct cluster_id from goodparty_data_catalog.dbt.stg_er_source__clustered_candidacy_stages where source_name = 'ballotready') b on b.cluster_id = g.cluster_id
    where g.source_name = 'gp_api' group by 1
),
u as (
    select c.user_id, coalesce(bl.ballot_status, 'never asked') bucket,
           max(rg.br_race_id) is not null race_found,
           max(case when ro.br_race_id is not null then 1 else 0 end) race_has_roster,
           max(case when ro.ln = c.ln and (ro.fn = c.fn or left(ro.fn,1) = left(c.fn,1)) then 1 else 0 end) name_match,
           max(case when ro.ln = c.ln then 1 else 0 end) lastname_match,
           coalesce(max(e.er_br), 0) er_match
    from cand c left join ballot bl on bl.user_id = c.user_id
    left join rg on rg.br_position_id = c.ballotready_position_id
    left join roster ro on ro.br_race_id = cast(rg.br_race_id as string)
    left join er e on e.campaign_id = c.campaign_id
    group by c.user_id, bl.ballot_status
)
select bucket, count(*) users, count_if(race_found) race_found, sum(race_has_roster) race_has_roster, sum(name_match) name_match, sum(lastname_match) lastname_only_match, sum(er_match) er_match,
       count_if(name_match = 1 and er_match = 0) name_yes_er_no, count_if(name_match = 0 and er_match = 1) er_yes_name_no
from u group by grouping sets ((bucket), ()) order by users desc
```

### D. Filing deadline from the signup race, coverage and lag

```sql
-- Filing deadline from the RACE the user picked at signup (position x election day), not from a person match
with cand as (
    select user_id, cast(campaign_id as string) campaign_id, user_created_at, ballotready_position_id
    from goodparty_data_catalog.mart_analytics.users_win_candidacy
    where is_latest_version and not is_demo and election_date = date'2026-11-03' and cast(user_id as string) not in (
        -- canonical internal rule, same as int__civics_internal_persons and election_cohort.py
        select cast(id as string) from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user
        where regexp_extract(lower(email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
           or arrays_overlap(from_json(roles, 'array<string>'), array('admin', 'sales')))
),
ballot as (
    select c.user_id, case max(case coalesce(rc.ballot_status, get_json_object(rc.data,'$.onboarding.ballotStatus')) when 'on-ballot' then 4 when 'qualified-not-filed' then 3 when 'considering' then 2 when 'testing' then 1 end) when 4 then 'on-ballot' when 3 then 'qualified-not-filed' when 2 then 'considering' when 1 then 'testing' end ballot_status
    from cand c join goodparty_data_catalog.airbyte_source.gp_api_db_campaign rc on cast(rc.id as string) = c.campaign_id group by 1
),
race_general as (
    select r.database_id br_race_id, r.position.databaseid br_position_id, e.election_day
    from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
    join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e on r.election.databaseid = e.database_id
    where not coalesce(r.is_primary, false) and not coalesce(r.is_runoff, false) and not coalesce(r.is_recall, false)
      and e.election_day = date'2026-11-03'
),
fp_ids as (
    select r.database_id race_database_id, max(fp.databaseid) filing_period_database_id
    from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r lateral view explode(r.filing_periods) as fp group by 1
),
race_deadline as (
    select rg.br_position_id, max(p.end_on) filing_deadline, count(distinct rg.br_race_id) n_races
    from race_general rg
    left join fp_ids fi on fi.race_database_id = rg.br_race_id
    left join goodparty_data_catalog.dbt.int__ballotready_filing_period p on p.database_id = fi.filing_period_database_id
    group by 1
),
person_match as (
    select c.user_id, max(case when cs.br_candidacy_id is not null then 1 else 0 end) br_person_match,
           max(case when cs.br_candidacy_id is not null or cs.ts_source_candidate_id is not null or cs.ddhq_candidate_id is not null then 1 else 0 end) any_person_match
    from cand c join goodparty_data_catalog.mart_civics.candidacy cd on cd.product_campaign_id = c.campaign_id
    left join goodparty_data_catalog.mart_civics.candidacy_stage cs on cs.gp_candidacy_id = cd.gp_candidacy_id group by 1
),
u as (
    select c.user_id,
           case when b.ballot_status is not null then b.ballot_status when min(c.user_created_at) < timestamp'2026-05-07' then 'never asked' else 'asked, no answer' end bucket,
           max(c.ballotready_position_id) is not null has_position,
           max(rd.br_position_id) is not null race_found,
           max(rd.filing_deadline) deadline,
           datediff(current_date(), max(rd.filing_deadline)) days_since,
           coalesce(max(pm.any_person_match), 0) any_person_match
    from cand c
    left join ballot b on b.user_id = c.user_id
    left join race_deadline rd on rd.br_position_id = c.ballotready_position_id
    left join person_match pm on pm.user_id = c.user_id
    group by c.user_id, b.ballot_status
)
select bucket, count(*) users,
       count_if(has_position) has_br_position,
       count_if(race_found) race_found_for_11_3,
       count_if(deadline is not null) deadline_known,
       count_if(deadline >= current_date()) deadline_not_yet_passed,
       count_if(days_since between 0 and 40) passed_0_40d,
       count_if(days_since between 41 and 101) passed_41_101d,
       count_if(days_since > 101) passed_over_101d,
       round(100 * count_if(any_person_match = 1 and days_since > 40) / nullif(count_if(days_since > 40), 0), 1) person_match_pct_over_40d,
       round(100 * count_if(any_person_match = 1 and days_since > 101) / nullif(count_if(days_since > 101), 0), 1) person_match_pct_over_101d
from u group by grouping sets ((bucket), ()) order by users desc

-- Q2 deadline distribution across the cohort (race-derived)
with cand as (
    select user_id, ballotready_position_id from goodparty_data_catalog.mart_analytics.users_win_candidacy
    where is_latest_version and not is_demo and election_date = date'2026-11-03' and cast(user_id as string) not in (
        -- canonical internal rule, same as int__civics_internal_persons and election_cohort.py
        select cast(id as string) from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user
        where regexp_extract(lower(email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
           or arrays_overlap(from_json(roles, 'array<string>'), array('admin', 'sales')))
),
rg as (
    select r.database_id br_race_id, r.position.databaseid br_position_id
    from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
    join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e on r.election.databaseid = e.database_id
    where not coalesce(r.is_primary, false) and not coalesce(r.is_runoff, false) and not coalesce(r.is_recall, false) and e.election_day = date'2026-11-03'
),
fp as (select r.database_id race_database_id, max(f.databaseid) fpid from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r lateral view explode(r.filing_periods) as f group by 1),
d as (
    select c.user_id, max(p.end_on) deadline from cand c join rg on rg.br_position_id = c.ballotready_position_id
    left join fp on fp.race_database_id = rg.br_race_id left join goodparty_data_catalog.dbt.int__ballotready_filing_period p on p.database_id = fp.fpid group by 1
)
select count(*) users_with_race, count_if(deadline is null) no_deadline, min(deadline) earliest, percentile(datediff(current_date(), deadline), 0.1) p10_days, percentile(datediff(current_date(), deadline), 0.5) median_days, max(deadline) latest from d
```

### E. This cycle's BallotReady roster lag and arrival curve

```sql
-- Q3 this cycle's BR lag: roster row creation minus the race filing deadline, cohort's 11/3 races
with cand as (select distinct ballotready_position_id from goodparty_data_catalog.mart_analytics.users_win_candidacy where is_latest_version and not is_demo and election_date = date'2026-11-03'),
rg as (select r.database_id br_race_id, r.position.databaseid br_position_id from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e on r.election.databaseid = e.database_id where not coalesce(r.is_primary,false) and not coalesce(r.is_runoff,false) and not coalesce(r.is_recall,false) and e.election_day = date'2026-11-03'),
fp as (select r.database_id race_database_id, max(f.databaseid) fpid from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r lateral view explode(r.filing_periods) as f group by 1),
rd as (select rg.br_race_id, max(p.end_on) deadline from cand c join rg on rg.br_position_id = c.ballotready_position_id left join fp on fp.race_database_id = rg.br_race_id left join goodparty_data_catalog.dbt.int__ballotready_filing_period p on p.database_id = fp.fpid group by 1),
rows_ as (select datediff(cast(cv.candidacy_created_at as date), rd.deadline) lag_days, rd.deadline from rd join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 cv on cast(cv.br_race_id as string) = cast(rd.br_race_id as string) where rd.deadline is not null)
select count(*) roster_rows, count_if(lag_days <= 0) by_deadline, round(100*count_if(lag_days <= 0)/count(*),1) pct_by_deadline, percentile(lag_days, 0.5) median_lag_days, percentile(lag_days, 0.9) p90_lag_days, max(lag_days) max_lag_days,
       count_if(lag_days > 40) rows_after_40d, count_if(lag_days > 101) rows_after_101d from rows_

-- Q4 same lag, by deadline month, to see whether late deadlines are still filling
with cand as (select distinct ballotready_position_id from goodparty_data_catalog.mart_analytics.users_win_candidacy where is_latest_version and not is_demo and election_date = date'2026-11-03'),
rg as (select r.database_id br_race_id, r.position.databaseid br_position_id from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e on r.election.databaseid = e.database_id where not coalesce(r.is_primary,false) and not coalesce(r.is_runoff,false) and not coalesce(r.is_recall,false) and e.election_day = date'2026-11-03'),
fp as (select r.database_id race_database_id, max(f.databaseid) fpid from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r lateral view explode(r.filing_periods) as f group by 1),
rd as (select rg.br_race_id, max(p.end_on) deadline from cand c join rg on rg.br_position_id = c.ballotready_position_id left join fp on fp.race_database_id = rg.br_race_id left join goodparty_data_catalog.dbt.int__ballotready_filing_period p on p.database_id = fp.fpid group by 1)
select date_trunc('month', rd.deadline) deadline_month, count(distinct rd.br_race_id) races, count(cv.br_candidacy_id) roster_rows, percentile(datediff(cast(cv.candidacy_created_at as date), rd.deadline), 0.5) median_lag_days, count_if(cv.candidacy_created_at >= '2026-09-01') rows_added_sept
from rd left join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 cv on cast(cv.br_race_id as string) = cast(rd.br_race_id as string) where rd.deadline is not null group by 1 order by 1

-- Q2 BR freshness (safe columns)
select 'br_candidacies_v3' t, max(_airbyte_extracted_at) extracted, max(candidacy_created_at) latest_src_created, count(*) rows from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3
union all select 'br_race', max(_airbyte_extracted_at), null, count(*) from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race
union all select 'br_filing_period', null, max(updated_at), count(*) from goodparty_data_catalog.dbt.int__ballotready_filing_period

-- Q3 BR roster recency for the cohort's 11/3 races
with cand as (select distinct ballotready_position_id from goodparty_data_catalog.mart_analytics.users_win_candidacy where is_latest_version and not is_demo and election_date = date'2026-11-03'),
rg as (select r.database_id br_race_id, r.position.databaseid br_position_id from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e on r.election.databaseid = e.database_id where not coalesce(r.is_primary,false) and not coalesce(r.is_runoff,false) and not coalesce(r.is_recall,false) and e.election_day = date'2026-11-03')
select count(distinct rg.br_race_id) races, count(distinct case when cv.br_candidacy_id is not null then rg.br_race_id end) races_with_roster, count(cv.br_candidacy_id) br_candidacy_rows,
       min(cv.candidacy_created_at) first_created, max(cv.candidacy_created_at) last_created,
       count_if(cv.candidacy_created_at >= '2026-09-01') created_since_sept
from cand c join rg on rg.br_position_id = c.ballotready_position_id left join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 cv on cast(cv.br_race_id as string) = cast(rg.br_race_id as string)
```

### F. Past-40-day unmatched on-ballot users, split by whether the race has a roster

```sql
-- On-ballot users past the 40-day lag: is the person on BR's roster, and does the race have a roster at all?
with cand as (
    select user_id, cast(campaign_id as string) campaign_id, ballotready_position_id
    from goodparty_data_catalog.mart_analytics.users_win_candidacy
    where is_latest_version and not is_demo and election_date = date'2026-11-03' and cast(user_id as string) not in (
        -- canonical internal rule, same as int__civics_internal_persons and election_cohort.py
        select cast(id as string) from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user
        where regexp_extract(lower(email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
           or arrays_overlap(from_json(roles, 'array<string>'), array('admin', 'sales')))
),
ballot as (
    select c.user_id, case max(case coalesce(rc.ballot_status, get_json_object(rc.data,'$.onboarding.ballotStatus')) when 'on-ballot' then 4 when 'qualified-not-filed' then 3 when 'considering' then 2 when 'testing' then 1 end) when 4 then 'on-ballot' when 3 then 'qualified-not-filed' when 2 then 'considering' when 1 then 'testing' end ballot_status
    from cand c join goodparty_data_catalog.airbyte_source.gp_api_db_campaign rc on cast(rc.id as string) = c.campaign_id group by 1
),
rg as (
    select r.database_id br_race_id, r.position.databaseid br_position_id
    from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
    join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e on r.election.databaseid = e.database_id
    where not coalesce(r.is_primary, false) and not coalesce(r.is_runoff, false) and not coalesce(r.is_recall, false) and e.election_day = date'2026-11-03'
),
fp as (select r.database_id race_database_id, max(f.databaseid) fpid from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r lateral view explode(r.filing_periods) as f group by 1),
rd as (
    select rg.br_position_id, max(p.end_on) deadline, max(rg.br_race_id) br_race_id
    from rg left join fp on fp.race_database_id = rg.br_race_id left join goodparty_data_catalog.dbt.int__ballotready_filing_period p on p.database_id = fp.fpid group by 1
),
roster as (select br_race_id, count(*) n_br_candidacies from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 group by 1),
pm as (
    select c.user_id, max(case when cs.br_candidacy_id is not null or cs.ts_source_candidate_id is not null or cs.ddhq_candidate_id is not null then 1 else 0 end) any_person_match
    from cand c join goodparty_data_catalog.mart_civics.candidacy cd on cd.product_campaign_id = c.campaign_id
    left join goodparty_data_catalog.mart_civics.candidacy_stage cs on cs.gp_candidacy_id = cd.gp_candidacy_id group by 1
),
u as (
    select c.user_id, b.ballot_status,
           datediff(current_date(), max(rd.deadline)) days_since,
           coalesce(max(pm.any_person_match), 0) person_match,
           coalesce(max(ro.n_br_candidacies), 0) race_roster_size
    from cand c left join ballot b on b.user_id = c.user_id
    left join rd on rd.br_position_id = c.ballotready_position_id
    left join roster ro on cast(ro.br_race_id as string) = cast(rd.br_race_id as string)
    left join pm on pm.user_id = c.user_id
    group by c.user_id, b.ballot_status
)
select coalesce(ballot_status, 'never asked') bucket,
       count(*) past_40d_users,
       count_if(person_match = 1) person_on_roster,
       count_if(person_match = 0 and race_roster_size = 0) not_matched_race_has_no_roster,
       count_if(person_match = 0 and race_roster_size = 1) not_matched_race_roster_of_1,
       count_if(person_match = 0 and race_roster_size >= 2) not_matched_race_has_roster_2plus,
       round(100 * count_if(person_match = 1) / count(*), 1) match_pct,
       round(100 * count_if(person_match = 1) / nullif(count_if(person_match = 1 or race_roster_size >= 2), 0), 1) match_pct_where_race_has_roster
from u where days_since > 40 group by 1 order by 2 desc
```
