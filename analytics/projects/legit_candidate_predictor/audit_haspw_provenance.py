"""DATA-2239: usr_has_password provenance audit (CRM-created-account hypothesis).

Sniff test found has_password at 42.7% in the working set against 67-80% era rates: the
office-declared corroborated population is disproportionately password-less. Hypothesis: a
password-less account is often CRM/sales-created (the no-browser 2025 wave), and those users
corroborate high because they were sourced from rosters — making has_password=0 an inverted
coverage proxy, not a candidate-behavior signal.

Test, on the working set (label use is legitimate here: this is a leakage audit, not feature
selection): stratify corroboration by has_password x pre-signup-sales-touch. If the
has_password gap collapses (or flips) within touch strata, the signal routes through
provenance and the column stays out of pass B.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/audit_haspw_provenance.py
"""

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from databricks_conn import run_query  # noqa: E402

SQL = """
select cast(u.user_id as bigint)                              as user_id,
       max(case when us.has_password then 1 else 0 end)       as has_password,
       max(case when pr.first_sales_touch_at is not null
                 and cast(pr.first_sales_touch_at as date) < cast(u.user_created_at as date)
                then 1 else 0 end)                            as sales_touch_before_signup,
       max(case when pr.gp_user_id is not null then 1 else 0 end) as in_prospects
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
left join goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user us
  on cast(us.id as string) = cast(u.user_id as string)
left join goodparty_data_catalog.mart_analytics.prospects pr
  on cast(pr.gp_user_id as string) = cast(u.user_id as string)
where u.is_latest_version and not u.is_demo
group by 1
"""

if __name__ == "__main__":
    prov = run_query(SQL)
    ws = pd.read_parquet(HERE / "datasets" / "working_set.parquet")
    j = ws[["user_id", "corroborated", "signup_date"]].merge(prov, on="user_id", how="left")
    j["year"] = pd.to_datetime(j["signup_date"]).dt.year
    base = j["corroborated"].mean()
    print(f"working set n={len(j):,}, base corroboration {base:.1%}\n")

    print("=== who lacks a password ===")
    print(
        j.groupby("has_password")
        .agg(
            n=("user_id", "size"),
            touched_pre_signup=("sales_touch_before_signup", "mean"),
            corroborated=("corroborated", "mean"),
        )
        .round(3)
        .to_string()
    )

    print("\n=== corroboration by has_password x pre-signup sales touch ===")
    tab = (
        j.groupby(["sales_touch_before_signup", "has_password"])
        .agg(n=("user_id", "size"), corroborated=("corroborated", "mean"))
        .round(3)
    )
    print(tab.to_string())

    print("\n=== same, within signup year (era control) ===")
    tab = (
        j.groupby(["year", "sales_touch_before_signup", "has_password"])
        .agg(n=("user_id", "size"), corroborated=("corroborated", "mean"))
        .round(3)
    )
    print(tab.to_string())

    gap_all = j[j.has_password.eq(0)].corroborated.mean() - j[j.has_password.eq(1)].corroborated.mean()
    print(f"\nraw no-password corroboration edge: {gap_all:+.3f}")
    for t in (0, 1):
        s = j[j.sales_touch_before_signup.eq(t)]
        gap = s[s.has_password.eq(0)].corroborated.mean() - s[s.has_password.eq(1)].corroborated.mean()
        print(f"  within touch={t}: {gap:+.3f}  (n={len(s):,})")
