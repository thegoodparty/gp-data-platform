#!/usr/bin/env python3
"""Pull dbt/Databricks cost + usage from system tables and emit an interactive HTML dashboard.

Requires: databricks CLI configured with a workspace profile that can read the
`system.billing` and `system.query` schemas, and a SQL warehouse to run against.

Usage:
  python cost_audit.py --profile databricks-gp --warehouse 18583d8b081c6486 \
      --dbt-app "Databricks Dbt" --since 2026-01-01 --out cost_dashboard.html
"""

import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict


def run_sql(profile, warehouse, stmt):
    payload = {
        "warehouse_id": warehouse,
        "wait_timeout": "50s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "statement": stmt,
    }
    p = subprocess.run(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--json",
            json.dumps(payload),
            "-p",
            profile,
        ],
        capture_output=True,
        text=True,
    )
    d = json.loads(p.stdout or "{}")
    sid = d.get("statement_id")
    while d.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        g = subprocess.run(
            ["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}", "-p", profile],
            capture_output=True,
            text=True,
        )
        d = json.loads(g.stdout or "{}")
    if d.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(json.dumps(d.get("status", {}))[:500])
    return d.get("result", {}).get("data_array") or []


# --- period expressions ---------------------------------------------------
M = "date_format(date_trunc('MONTH', {c}), 'yyyy-MM')"
W = "date_format(date_trunc('WEEK', {c}), 'yyyy-MM-dd')"

PRICE_JOIN = (
    "left join system.billing.list_prices p on u.sku_name=p.sku_name "
    "and u.usage_start_time>=p.price_start_time "
    "and (p.price_end_time is null or u.usage_end_time<=p.price_end_time)"
)


def q_product(grain_expr, since):
    return (
        f"select {grain_expr.format(c='u.usage_date')} period, u.billing_origin_product prod, "
        f"round(sum(u.usage_quantity*p.pricing.default),2) usd "
        f"from system.billing.usage u {PRICE_JOIN} "
        f"where u.usage_date>='{since}' group by 1,2 having usd>0 order by 1,2"
    )


def q_wh_usd(grain_expr, since, wh):
    # dollars for the dbt warehouse itself, for prorating query-time -> $
    return (
        f"select {grain_expr.format(c='u.usage_date')} period, "
        f"round(sum(u.usage_quantity*p.pricing.default),2) usd "
        f"from system.billing.usage u {PRICE_JOIN} "
        f"where u.usage_date>='{since}' and u.billing_origin_product='SQL' "
        f"and u.usage_metadata.warehouse_id='{wh}' group by 1"
    )


def q_nodetype(grain_expr, since, wh, dbt_app):
    # dbt exec-hours split by node resource type (model/test/snapshot/seed/overhead)
    node = 'regexp_extract(statement_text, \'"node_id": ?"([^"]+)"\', 1)'
    typ = f"case when {node}='' then 'overhead' " f"else element_at(split({node},'[.]'),1) end"
    return (
        f"select {grain_expr.format(c='start_time')} period, {typ} node_type, "
        f"round(sum(execution_duration_ms)/3600000,2) exec_hr, count(*) q "
        f"from system.query.history where compute.warehouse_id='{wh}' "
        f"and client_application='{dbt_app}' and start_time>='{since}' group by 1,2"
    )


def q_consumer(grain_expr, since, wh):
    return (
        f"select {grain_expr.format(c='start_time')} period, "
        f"coalesce(nullif(client_application,''),'(service principal)') consumer, "
        f"round(sum(execution_duration_ms)/3600000,2) exec_hr "
        f"from system.query.history where compute.warehouse_id='{wh}' "
        f"and start_time>='{since}' group by 1,2"
    )


def q_topnodes(since, wh, dbt_app):
    node = 'regexp_extract(statement_text, \'"node_id": ?"([^"]+)"\', 1)'
    return (
        f"select {node} node, element_at(split({node},'[.]'),1) t, "
        f"round(sum(execution_duration_ms)/3600000,1) exec_hr, count(*) q, "
        f"round(sum(read_bytes)/1e12,2) tb "
        f"from system.query.history where compute.warehouse_id='{wh}' "
        f"and client_application='{dbt_app}' and start_time>='{since}' "
        f"and {node}!='' group by 1,2 order by exec_hr desc limit 25"
    )


def q_freq_windows(since, wh, dbt_app):
    # dbt exec-hr in the 4 daily hours that ONLY the every-4h job runs (UTC 4,8,16,20),
    # scaled x6/4 to estimate the full high-frequency-job footprint.
    return (
        f"select {M.format(c='start_time')} period, "
        f"round(sum(case when hour(start_time) in (4,8,16,20) then execution_duration_ms else 0 end)"
        f"/3600000 *6.0/4.0,2) freq_hr "
        f"from system.query.history where compute.warehouse_id='{wh}' "
        f"and client_application='{dbt_app}' and start_time>='{since}' group by 1"
    )


def collect(profile, warehouse, since, dbt_app):
    data = {"grains": {}}
    for gname, gexpr in (("month", M), ("week", W)):
        prod = run_sql(profile, warehouse, q_product(gexpr, since))
        whusd = dict(
            (r[0], float(r[1])) for r in run_sql(profile, warehouse, q_wh_usd(gexpr, since, warehouse))
        )
        ntype = run_sql(profile, warehouse, q_nodetype(gexpr, since, warehouse, dbt_app))
        cons = run_sql(profile, warehouse, q_consumer(gexpr, since, warehouse))
        # total WH exec-hr per period (all consumers) for prorating
        tot_hr = defaultdict(float)
        for period, consumer, hr in cons:
            tot_hr[period] += float(hr or 0)

        def prorate(period, hr):
            t = tot_hr.get(period, 0)
            return round(whusd.get(period, 0) * (float(hr or 0) / t), 2) if t else 0.0

        data["grains"][gname] = {
            "product": [{"period": r[0], "k": r[1], "usd": float(r[2])} for r in prod],
            "nodetype": [
                {
                    "period": r[0],
                    "k": r[1],
                    "hr": float(r[2] or 0),
                    "usd": prorate(r[0], r[2]),
                    "q": int(r[3]),
                }
                for r in ntype
            ],
            "consumer": [
                {"period": r[0], "k": r[1], "hr": float(r[2] or 0), "usd": prorate(r[0], r[2])} for r in cons
            ],
        }
    data["topnodes"] = [
        {
            "node": r[0].split(".")[-1],
            "type": r[1],
            "hr": float(r[2] or 0),
            "q": int(r[3]),
            "tb": float(r[4] or 0),
        }
        for r in run_sql(profile, warehouse, q_topnodes(since, warehouse, dbt_app))
    ]
    data["freq"] = [
        {"period": r[0], "hr": float(r[1] or 0)}
        for r in run_sql(profile, warehouse, q_freq_windows(since, warehouse, dbt_app))
    ]
    data["meta"] = {"since": since, "warehouse": warehouse, "dbt_app": dbt_app}
    return data


def render_html(data, out):
    tpl = open(__file__.replace("cost_audit.py", "dashboard_template.html")).read()
    html = tpl.replace("/*__DATA__*/{}", json.dumps(data))
    open(out, "w").write(html)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="databricks-gp")
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--dbt-app", default="Databricks Dbt")
    ap.add_argument("--since", default="2026-01-01")
    ap.add_argument("--out", default="cost_dashboard.html")
    ap.add_argument("--data-only", action="store_true")
    ap.add_argument("--from-json", help="skip queries, render HTML from an existing json file")
    a = ap.parse_args()
    if a.from_json:
        data = json.load(open(a.from_json))
        render_html(data, a.out)
        print(f"rendered {a.out} from {a.from_json}")
        sys.exit(0)
    data = collect(a.profile, a.warehouse, a.since, a.dbt_app)
    json.dump(data, open(a.out.replace(".html", ".json"), "w"), indent=2)
    # quick validation summary
    g = data["grains"]["month"]
    months = sorted({r["period"] for r in g["product"]})
    last = months[-1]
    prod_total = sum(r["usd"] for r in g["product"] if r["period"] == last)
    test_usd = sum(r["usd"] for r in g["nodetype"] if r["period"] == last and r["k"] == "test")
    dbt_usd = sum(r["usd"] for r in g["nodetype"] if r["period"] == last)
    freq = dict((r["period"], r["hr"]) for r in data["freq"])
    print(f"months: {months}")
    print(
        f"latest {last}: total=${prod_total:.0f}  dbt(alloc)=${dbt_usd:.0f}  TEST=${test_usd:.0f} ({100*test_usd/prod_total:.0f}% of total)"
    )
    print(f"freq-job est exec-hr {last}: {freq.get(last,0):.0f}")
    if a.data_only:
        sys.exit(0)
    render_html(data, a.out)
    print(f"wrote {a.out}")
