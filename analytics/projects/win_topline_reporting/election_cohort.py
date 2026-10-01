"""Election-cohort census: Win users with an election on one date, how many are Pro, how many
reached voters through the product, split by the onboarding "Are you already on the ballot?" answer.

First built for DATA-2599 (2026-11-03). Brief: election_cohort_brief.yaml; standalone SQL for every
number: election_cohort_queries.md. Re-run weekly through election day; the mart refreshes hourly,
so the printed as-of time travels with every figure.

Run: cd analytics && uv run python projects/win_topline_reporting/election_cohort.py [--election-date 2026-11-03]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
import databricks_conn as dbc

pd.set_option("display.width", 200)
CAT = "goodparty_data_catalog"
CUTOVER = pd.Timestamp("2026-05-07")  # onboarding rebuild that introduced the ballot question

WORKING_SET = """
with cand as (
    select c.user_id, lower(c.user_email) as email, cast(c.campaign_id as string) as campaign_id,
           c.is_pro, c.is_active, c.election_date, c.user_created_at,
           -- canonical internal rule, same as int__civics_internal_persons
           (regexp_extract(lower(c.user_email), '@(.+)$', 1) ilike any ('%goodparty%', '%mailinator%')
            or arrays_overlap(from_json(gu.roles, 'array<string>'), array('admin', 'sales'))) as is_internal
    from {cat}.mart_analytics.users_win_candidacy c
    left join {cat}.dbt.stg_airbyte_source__gp_api_db_user gu on cast(gu.id as string) = cast(c.user_id as string)
    where c.is_latest_version and not c.is_demo
      and c.election_date between date'{window_start}' and date'{window_end}'
),
ballot as (
    -- column landed 2026-08-25; earlier answers live only in the JSON archive
    select c.user_id,
           coalesce(rc.ballot_status, get_json_object(rc.data, '$.onboarding.ballotStatus')) as ballot_status
    from cand c
    join {cat}.airbyte_source.gp_api_db_campaign rc on cast(rc.id as string) = c.campaign_id
),
users as (
    select c.user_id, min(c.email) as email, min(c.user_created_at) as user_created_at,
           max(case when c.is_pro then 1 else 0 end) as is_pro,
           max(case when c.is_active then 1 else 0 end) as is_active,
           max(case when c.is_internal then 1 else 0 end) as is_internal,
           max(case when c.election_date = date'{election_date}' then 1 else 0 end) as on_1103,
           max(b.ballot_status) as ballot_status,
           count(distinct b.ballot_status) as n_ballot_answers
    from cand c left join ballot b on b.user_id = c.user_id
    group by c.user_id
),
sent as (
    -- product-DB reference: an outreach row that committed (completed/in progress, or approved
    -- under the legacy flow) and was not canceled. 'pending' legacy rows are requests, not sends.
    select sc.user_id, count(*) as committed_rows
    from {cat}.dbt.stg_airbyte_source__gp_api_db_outreach o
    join {cat}.dbt.stg_airbyte_source__gp_api_db_campaign sc on sc.id = o.campaignId
    where o.canceled_at is null
      and (o.status in ('completed', 'in_progress') or o.approved_at is not null)
    group by sc.user_id
),
legacy_only as (
    -- activation evidence that is ONLY a self-report from the legacy log-progress modals, which tagged
    -- method = 'unknown' (in data 2025-06-26..2026-04-28; omni LogTaskModal, removed 2026-06-11). The
    -- ratified rule excludes self-report but the build only excludes method = 'manual'.
    select cast(user_id as string) as user_id,
           sum(case when get_json_object(event_properties, '$.method') = 'unknown' then 0 else 1 end) as non_legacy_ev
    from {cat}.dbt.stg_airbyte_source__amplitude_api_events
    where event_type in ('Voter Outreach - Campaign Completed', 'Outreach - Campaign Completed',
                         'Voter Outreach - Campaign Scheduled', 'Outreach - Phone Banking: Complete',
                         'Door Knocking - Door Logged', 'Outreach - Door Knocking Door Logged',
                         'Outreach - Phone Banking: Call Logged', 'Outreach - Phone Banking Call Logged')
      and coalesce(get_json_object(event_properties, '$.method'), '') <> 'manual'
      and coalesce(get_json_object(event_properties, '$.product'), '') <> 'serve'
      and user_id in (select cast(user_id as string) from users)
    group by 1
),
corroborated as (
    -- amendment (2026-10-01): roster corroboration as a secondary source for "on the ballot".
    -- Match flag per joins.md; deadline gate per sources.md (filing_period_end_on is 2026+ only).
    select c.user_id,
           max(case when cs.br_candidacy_id is not null then 1 else 0 end) as br_match,
           max(case when cs.br_candidacy_id is not null or cs.ts_source_candidate_id is not null
                      or cs.ddhq_candidate_id is not null then 1 else 0 end) as any_match,
           max(case when es.filing_period_end_on < current_date() then 1 else 0 end) as deadline_passed,
           max(case when es.filing_period_end_on is not null then 1 else 0 end) as deadline_known
    from cand c
    join {cat}.mart_civics.candidacy cd on cd.product_campaign_id = c.campaign_id
    left join {cat}.mart_civics.candidacy_stage cs on cs.gp_candidacy_id = cd.gp_candidacy_id
    left join {cat}.mart_civics.election_stage es on es.gp_election_stage_id = cs.gp_election_stage_id
    group by c.user_id
),
amp as (
    select user_id, max(get_json_object(user_properties, '$.officeElectionDate')) as amp_election_date
    from {cat}.dbt.stg_airbyte_source__amplitude_api_events
    where event_time >= '2026-01-01' and user_properties like '%officeElectionDate%'
      and user_id in (select cast(user_id as string) from users)
    group by user_id
)
select u.*, b.is_activated, b.first_campaign_sent_at, s.committed_rows, a.amp_election_date,
       coalesce(l.non_legacy_ev, 0) as non_legacy_ev,
       coalesce(k.br_match, 0) as br_match, coalesce(k.any_match, 0) as any_match,
       coalesce(k.deadline_passed, 0) as deadline_passed, coalesce(k.deadline_known, 0) as deadline_known,
       (k.user_id is not null) as in_civics
from users u
left join corroborated k on k.user_id = u.user_id
left join legacy_only l on l.user_id = cast(u.user_id as string)
left join {cat}.mart_analytics.users_win_base b on b.user_id = u.user_id
left join sent s on s.user_id = u.user_id
left join amp a on a.user_id = cast(u.user_id as string)
"""


def bucket(row):
    if pd.notna(row.ballot_status):
        return row.ballot_status
    return "never asked (pre-2026-05-07)" if row.user_created_at < CUTOVER else "asked, no answer"


def summarize(df, label):
    out = pd.DataFrame(
        {
            "users": [len(df)],
            "pro": [int(df.is_pro.sum())],
            "activated (mart build today)": [int(df.is_activated.sum())],
            "of which self-report only (method unknown)": [int(df.legacy_only.sum())],
            "activated (rule-consistent, self-reports removed)": [int(df.activated_strict.sum())],
            "has active campaign": [int(df.is_active.sum())],
            "ref: product-DB committed send": [int((df.committed_rows > 0).sum())],
        },
        index=[label],
    )
    return out


def by_bucket(df):
    g = df.groupby("bucket").agg(
        users=("user_id", "size"),
        pro=("is_pro", "sum"),
        activated=("is_activated", "sum"),
        activated_strict=("activated_strict", "sum"),
    )
    g["pro %"] = (100 * g.pro / g.users).round(1)
    g["activated %"] = (100 * g.activated / g.users).round(1)
    g["rule-consistent %"] = (100 * g.activated_strict / g.users).round(1)
    g["share of all Pro %"] = (100 * g.pro / g.pro.sum()).round(1)
    g["share of all activated %"] = (100 * g.activated / g.activated.sum()).round(1)
    order = [
        "on-ballot",
        "qualified-not-filed",
        "considering",
        "testing",
        "asked, no answer",
        "never asked (pre-2026-05-07)",
    ]
    return g.reindex([o for o in order if o in g.index])


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--election-date", default="2026-11-03", help="exact election date for figure A")
    ap.add_argument(
        "--window-days", type=int, default=7, help="figure B widens to [date-2, date+window-days]"
    )
    args = ap.parse_args()
    ed = pd.Timestamp(args.election_date)
    window_start, window_end = (
        (ed - pd.Timedelta(days=2)).date(),
        (ed + pd.Timedelta(days=args.window_days)).date(),
    )
    sql = WORKING_SET.format(
        cat=CAT, election_date=ed.date(), window_start=window_start, window_end=window_end
    )
    raw = dbc.run_query(sql)
    raw["is_activated"] = raw.is_activated.fillna(False).astype(bool)
    raw["committed_rows"] = raw.committed_rows.fillna(0)
    raw["legacy_only"] = raw.is_activated & (raw.non_legacy_ev == 0)
    raw["activated_strict"] = raw.is_activated & ~raw.legacy_only
    raw["user_created_at"] = pd.to_datetime(raw.user_created_at)
    internal = raw.is_internal == 1
    df = raw[~internal].copy()
    df["bucket"] = df.apply(bucket, axis=1)
    a = df[df.on_1103 == 1]

    print(
        f"\nAs of {pd.Timestamp.utcnow():%Y-%m-%d %H:%M} UTC; latest first-outreach timestamp in the mart: "
        f"{pd.to_datetime(raw.first_campaign_sent_at).max()} UTC"
    )
    print(
        f"Excluded internal accounts (goodparty/mailinator domain or admin/sales role, per int__civics_internal_persons): {int(internal.sum())} "
        f"(of which Pro {int(raw[internal].is_pro.sum())}, activated {int(raw[internal].is_activated.sum())})"
    )
    print(f"Users with more than one ballot answer across campaigns: {int((df.n_ballot_answers > 1).sum())}")

    print("\n== Headline ==")
    print(
        pd.concat(
            [
                summarize(a, f"A: election on {ed.date()}"),
                summarize(df, f"B: election {window_start}..{window_end}"),
            ]
        ).to_string()
    )

    print(f"\n== A: {ed.date()} cohort by onboarding ballot answer ==")
    print(by_bucket(a).to_string())
    print(f"\n== B: {window_start}..{window_end} cohort by onboarding ballot answer ==")
    print(by_bucket(df).to_string())

    answered = a[a.ballot_status.notna()]
    print(
        f"\nWithin the 11/3 users who answered the question ({len(answered)}): "
        f"on-ballot holds {int(answered[answered.bucket=='on-ballot'].is_pro.sum())} of "
        f"{int(answered.is_pro.sum())} Pro and "
        f"{int(answered[answered.bucket=='on-ballot'].is_activated.sum())} of "
        f"{int(answered.is_activated.sum())} activated."
    )

    print(
        "\n== Secondary source: roster corroboration (BallotReady / TechSpeed / DDHQ) by ballot answer, 11/3 =="
    )
    k = a.groupby("bucket").agg(
        users=("user_id", "size"),
        in_civics_mart=("in_civics", "sum"),
        br_match=("br_match", "sum"),
        any_roster_match=("any_match", "sum"),
        deadline_known=("deadline_known", "sum"),
        deadline_passed=("deadline_passed", "sum"),
    )
    k["any match %"] = (100 * k.any_roster_match / k.users).round(1)
    # deadline columns here are gated on civics-mart admission; the race-derived deadline (DATA-2603 query D) is the right read
    print(
        k.reindex(
            [
                o
                for o in [
                    "on-ballot",
                    "qualified-not-filed",
                    "considering",
                    "testing",
                    "asked, no answer",
                    "never asked (pre-2026-05-07)",
                ]
                if o in k.index
            ]
        ).to_string()
    )
    print(
        f"Whole 11/3 cohort: any roster match {int(a.any_match.sum())} of {len(a)} ({100*a.any_match.mean():.1f}%); "
        f"Pro with a match {int(a[a.is_pro==1].any_match.sum())} of {int(a.is_pro.sum())}; "
        f"activated (strict) with a match {int(a[a.activated_strict].any_match.sum())} of {int(a.activated_strict.sum())}"
    )

    print("\n== Robustness ==")
    print(
        f"Legacy-tracker-only activated who are not Pro today (11/3): {int((a.legacy_only & (a.is_pro==0)).sum())} of {int(a.legacy_only.sum())}"
    )
    print(
        f"Activated but not Pro today (churned/comped Pros, 11/3): {int((a.is_activated & (a.is_pro==0)).sum())}"
    )
    print(
        f"Governed-activated with no product-DB committed send (11/3): {int((a.is_activated & (a.committed_rows==0)).sum())}"
    )
    print(
        f"Product-DB committed send but not governed-activated (11/3): {int((~a.is_activated & (a.committed_rows>0)).sum())}"
    )
    print(
        f"First outreach action after today (should be 0): {int((pd.to_datetime(a.first_campaign_sent_at) > pd.Timestamp.utcnow().tz_localize(None)).sum())}"
    )

    print("\n== Gap check: election date in Amplitude (user property officeElectionDate) ==")
    has = a.amp_election_date.notna()
    print(
        f"11/3 cohort users with the property set in Amplitude (events since 2026-01-01): {int(has.sum())} of {len(a)} "
        f"({100*has.mean():.1f}%); of those, value = 2026-11-03 for {int((a.amp_election_date=='2026-11-03').sum())}"
    )
    print("Pre/post onboarding-rebuild registrants with the property:")
    print(
        a.assign(post=a.user_created_at >= CUTOVER)
        .groupby("post")
        .agg(users=("user_id", "size"), with_amp_date=("amp_election_date", lambda s: s.notna().sum()))
        .to_string()
    )


if __name__ == "__main__":
    main()
