"""DATA-2239 Area 2: campaign `details` key mutability audit on the CDC window.

The gp_user_details family's pass-B claim is "creation-time keys are written once, so the
current value equals the as-of-signup value". SOURCE_CATALOG 3 established the claim from the
key inventory; this audit measures it directly on the only history we have — the Airbyte raw
campaign table, multi-generation since 2026-04-13.

For every details key, over campaigns with at least two extracts:
- change_rate: share of key-bearing campaigns whose value differs across extracts
- appear_rate: share of campaigns where the key is absent in some extracts and present in
  others (appearance is mutation too — a key that arrives later than creation is not B)

Verdict rule (printed): any key used by build_gp_user_details.py with change_rate or
appear_rate above 2% is flagged NOT-PIT and must be dropped or demoted before the family is
accepted. The window only covers campaigns alive after 2026-04-13, so this validates the
mechanism (does the app rewrite the key), not each historical row.

Output: datasets/details_mutability.csv + stdout verdicts.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/audit_details_mutability.py
"""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from databricks_conn import run_query  # noqa: E402

BUILD_KEYS = {
    "knowRun",
    "pledged",
    "officeTermLength",
    "filingPeriodsEnd",
    "occupation",
    "pastExperience",
    "website",
    "runningAgainst",
    "campaignCommittee",
    "customIssues",
    "funFact",
}

SQL = """
with versions as (
  select cast(id as string)      as cid,
         _airbyte_extracted_at   as ts,
         details
  from goodparty_data_catalog.dbt.stg_airbyte_internal__raw_gp_api_db_campaign
  where details is not null
),
multi as (
  select cid, count(distinct ts) as n_extracts
  from versions group by cid having count(distinct ts) >= 2
),
ex as (
  select v.cid, m.n_extracts, v.ts, e.k, e.v
  from versions v
  join multi m on m.cid = v.cid
  lateral view explode(from_json(v.details, 'map<string,string>')) e as k, v
),
perkey as (
  select k, cid,
         max(n_extracts)                  as n_extracts,
         count(distinct ts)               as n_extracts_with_key,
         count(distinct coalesce(v, '')) as n_values
  from ex group by k, cid
)
select k,
       count(*)                                                       as n_campaigns_with_key,
       sum(case when n_values > 1 then 1 else 0 end)                  as n_value_changed,
       avg(case when n_values > 1 then 1.0 else 0.0 end)              as change_rate,
       sum(case when n_extracts_with_key < n_extracts then 1 else 0 end) as n_appeared,
       avg(case when n_extracts_with_key < n_extracts then 1.0 else 0.0 end) as appear_rate
from perkey
group by k
order by change_rate desc, n_campaigns_with_key desc
"""

if __name__ == "__main__":
    df = run_query(SQL)
    out = HERE / "datasets" / "details_mutability.csv"
    df.to_csv(out, index=False)
    print(f"{len(df)} keys measured over the CDC window -> {out}\n")

    used = df[df.k.isin(BUILD_KEYS)].copy()
    used["verdict"] = [
        "NOT-PIT" if (r.change_rate > 0.02 or r.appear_rate > 0.02) else "ok" for r in used.itertuples()
    ]
    print("keys used by build_gp_user_details.py:")
    print(used.to_string(index=False))

    flagged = used[used.verdict == "NOT-PIT"]
    if len(flagged):
        print(
            f"\n!! {len(flagged)} build key(s) mutate on the CDC window: "
            f"{', '.join(flagged.k)} — drop or demote before accepting the family."
        )
    else:
        print("\nall build keys stable on the CDC window; the creation-time claim holds.")

    print("\ntop movers overall (context):")
    print(df.head(15).to_string(index=False))
