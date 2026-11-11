"""DATA-2239 Area 2: HubSpot provenance audit — runs BEFORE the hubspot family build.

HubSpot mixes originally-captured data (web analytics first-touch, form fills) with imports and
enrichment from the same vendors that construct our label (BR rosters via sales sourcing,
TechSpeed). A field is admissible to pass B only if its values are captured at/before signup AND
its origin is not "the pipeline already knew this person from a roster".

Three measurements, all on the working set (label use is legitimate in a leakage audit):
1. Origin bucketing: corroboration rate + BR-roster presence by hs_analytics_source, with
   source_data_1/2 decomposing OFFLINE into its actual import/integration channels.
2. First-touch field fill by origin: whether the attribution fields we want even exist for
   organically-arrived users, or only for imported ones (in which case fill itself is the proxy).
3. candidate_id_source values: which vendor identified the person — the direct tracer for the
   label supply chain (diagnostic only; the field itself is L-tagged and never a feature).

Verdict rule: an origin bucket whose corroboration is far above base AND whose members are
BR-roster-nested marks its channel as label supply chain; fields whose FILL concentrates in such
buckets are coverage proxies regardless of their values.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/audit_hubspot_provenance.py
"""

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from databricks_conn import run_query  # noqa: E402

SQL = """
with pop as (
  select u.user_id, cast(u.campaign_id as string) as campaign_id
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  where u.is_latest_version and not u.is_demo
  qualify row_number() over (partition by u.user_id
                             order by u.election_date desc nulls last, u.campaign_id desc) = 1
),
bridge as (
  select cast(c.product_campaign_id as string) as campaign_id,
         c.hubspot_contact_id
  from goodparty_data_catalog.mart_civics.candidacy c
  where c.hubspot_contact_id is not null
  qualify row_number() over (partition by cast(c.product_campaign_id as string)
                             order by c.general_election_date desc nulls last) = 1
)
select p.user_id,
       ct.hs_analytics_source,
       ct.hs_analytics_source_data_1,
       ct.hs_analytics_source_data_2,
       ct.channel_group__first_conversion_source as channel_group,
       ct.conversion_source,
       ct.candidate_id_source,
       ct.candidate_id_tier,
       case when ct.hs_analytics_first_referrer is not null
             and ct.hs_analytics_first_referrer <> '' then 1 else 0 end as has_first_referrer,
       case when ct.hs_analytics_first_url is not null
             and ct.hs_analytics_first_url <> '' then 1 else 0 end      as has_first_url,
       ct.hs_analytics_first_visit_timestamp
from pop p
join bridge b on b.campaign_id = p.campaign_id
join goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_contacts ct
  on ct.id = b.hubspot_contact_id
"""

if __name__ == "__main__":
    hs = run_query(SQL)
    print(f"contact rows: {len(hs):,} (should be ~1 per user)")
    hs = hs.drop_duplicates("user_id")

    ws = pd.read_parquet(HERE / "datasets" / "working_set.parquet")
    j = ws[["user_id", "corroborated", "n_br_candidacies_by_deadline"]].merge(hs, on="user_id", how="left")
    j["has_roster"] = j["n_br_candidacies_by_deadline"].notna()
    base = j["corroborated"].mean()
    print(
        f"working set n={len(j):,}, base {base:.1%}, "
        f"hubspot contact joined {j.hs_analytics_source.notna().mean():.1%}\n"
    )

    print("=== 1. corroboration + roster nesting by hs_analytics_source ===")
    print(
        j.groupby(j.hs_analytics_source.fillna("(no contact/source)"))
        .agg(
            n=("user_id", "size"),
            corroborated=("corroborated", "mean"),
            br_roster=("has_roster", "mean"),
            first_referrer=("has_first_referrer", "mean"),
        )
        .round(3)
        .sort_values("n", ascending=False)
        .to_string()
    )

    print("\n=== OFFLINE decomposed by source_data_1 ===")
    off = j[j.hs_analytics_source.eq("OFFLINE")]
    print(
        off.groupby(off.hs_analytics_source_data_1.fillna("(null)"))
        .agg(n=("user_id", "size"), corroborated=("corroborated", "mean"), br_roster=("has_roster", "mean"))
        .round(3)
        .sort_values("n", ascending=False)
        .head(12)
        .to_string()
    )
    print("\ntop source_data_2 within OFFLINE/INTEGRATION rows:")
    oi = off[off.hs_analytics_source_data_1.isin(["INTEGRATION", "API", "IMPORT"])]
    print(oi.hs_analytics_source_data_2.fillna("(null)").value_counts().head(10).to_string())

    print("\n=== 2. channel_group (first conversion) ===")
    print(
        j.groupby(j.channel_group.fillna("(null)"))
        .agg(n=("user_id", "size"), corroborated=("corroborated", "mean"), br_roster=("has_roster", "mean"))
        .round(3)
        .sort_values("n", ascending=False)
        .head(12)
        .to_string()
    )

    print("\n=== 3. candidate_id_source (label supply chain tracer, diagnostic only) ===")
    print(
        j.groupby(j.candidate_id_source.fillna("(null)"))
        .agg(n=("user_id", "size"), corroborated=("corroborated", "mean"), br_roster=("has_roster", "mean"))
        .round(3)
        .sort_values("n", ascending=False)
        .head(12)
        .to_string()
    )

    out = HERE / "datasets" / "hubspot_provenance_audit.parquet"
    j.to_parquet(out, index=False)
    print(f"\nsaved -> {out}")
