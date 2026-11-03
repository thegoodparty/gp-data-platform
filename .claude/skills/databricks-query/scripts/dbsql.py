#!/usr/bin/env python3
"""Run a SQL statement against the GoodParty Databricks SQL warehouse and print rows.

Why this exists: the `databricks` CLI has no "run a query" command, so SQL must go
through the Statement Execution REST API (`api post /api/2.0/sql/statements`).
Hand-building that `--json '{...}'` call is fine for `SELECT 1` but breaks the moment
the SQL contains quotes, newlines, or JSON-path syntax like `details:office::string`.
This wrapper json-encodes the SQL safely, pins the warehouse + OAuth profile, drops any
stale DATABRICKS_TOKEN so OAuth is used unambiguously, polls until the statement
finishes, and pages through result chunks.

Usage:
  dbsql.py "SELECT count(*) FROM goodparty_data_catalog.dbt.some_table"
  dbsql.py -f query.sql
  echo "SELECT 1" | dbsql.py
  dbsql.py --csv "SELECT ..." > out.csv
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time

PROFILE = "dbc-3d8ca484-79f3"
WAREHOUSE_ID = "18583d8b081c6486"  # serverless Starter warehouse (auto-starts)
STATEMENTS = "/api/2.0/sql/statements"


def _cell(v):
    return "" if v is None else str(v)


def _cli(method, path, body=None):
    """Call `databricks api <method> <path>` with a clean env; return parsed JSON."""
    # Strip DATABRICKS_TOKEN/HOST so the OAuth profile wins. A stale PAT in the env
    # otherwise risks a wrong-credential auth failure once it is revoked.
    env = {k: v for k, v in os.environ.items() if k not in ("DATABRICKS_TOKEN", "DATABRICKS_HOST")}
    cmd = ["databricks", "api", method, path, "-p", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if p.returncode != 0:
        sys.exit("databricks CLI error:\n" + (p.stderr.strip() or p.stdout.strip()))
    return json.loads(p.stdout)


def run_sql(sql, catalog=None, schema=None, warehouse=WAREHOUSE_ID):
    body = {
        "warehouse_id": warehouse,
        "statement": sql,
        "wait_timeout": "50s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
    }
    if catalog:
        body["catalog"] = catalog
    if schema:
        body["schema"] = schema
    resp = _cli("post", STATEMENTS, body)
    sid = resp.get("statement_id")
    # wait_timeout already waited server-side; keep polling if still running.
    while resp.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        resp = _cli("get", "{}/{}".format(STATEMENTS, sid))
    state = resp.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = resp.get("status", {}).get("error", {}).get("message", state)
        sys.exit("statement {}: {}".format(state, err))
    # DDL/utility statements succeed without a result schema.
    if not resp.get("manifest", {}).get("schema", {}).get("columns"):
        return ["state"], [[state]]
    cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    rows = list(resp.get("result", {}).get("data_array", []) or [])
    # Page through any additional result chunks (chunk 0 is inline above).
    total = resp["manifest"].get("total_chunk_count", 1)
    for idx in range(1, total):
        chunk = _cli("get", "{}/{}/result/chunks/{}".format(STATEMENTS, sid, idx))
        rows += chunk.get("data_array", []) or []
    return cols, rows


def main():
    ap = argparse.ArgumentParser(description="Run SQL on Databricks via the CLI.")
    ap.add_argument("sql", nargs="?", help="SQL text (or use -f / stdin)")
    ap.add_argument("-f", "--file", help="read SQL from this file")
    ap.add_argument("--catalog", help="default catalog for unqualified names")
    ap.add_argument("--schema", help="default schema for unqualified names")
    ap.add_argument("--warehouse", default=WAREHOUSE_ID, help="warehouse id override")
    ap.add_argument("--limit", type=int, help="max rows to print (safety cap)")
    fmt = ap.add_mutually_exclusive_group()
    fmt.add_argument("--csv", action="store_true", help="CSV output")
    fmt.add_argument("--json", action="store_true", help="raw row JSON")
    args = ap.parse_args()

    if args.file:
        with open(args.file) as fh:
            sql = fh.read()
    elif args.sql:
        sql = args.sql
    elif not sys.stdin.isatty():
        sql = sys.stdin.read()
    else:
        ap.error("provide SQL as an argument, with -f FILE, or on stdin")
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        ap.error("empty SQL")

    cols, rows = run_sql(sql, args.catalog, args.schema, args.warehouse)
    if args.limit is not None:
        rows = rows[: args.limit]

    if args.json:
        print(json.dumps([dict(zip(cols, r)) for r in rows], indent=2))
        return
    if args.csv:
        w = csv.writer(sys.stdout)
        w.writerow(cols)
        w.writerows(rows)
        return
    # default: tab-aligned, readable in a terminal
    widths = []
    for i, c in enumerate(cols):
        width = len(_cell(c))
        for r in rows:
            width = max(width, len(_cell(r[i])))
        widths.append(width)
    print("  ".join(_cell(c).ljust(widths[i]) for i, c in enumerate(cols)))
    print("  ".join("-" * widths[i] for i in range(len(cols))))
    for r in rows:
        print("  ".join(_cell(v).ljust(widths[i]) for i, v in enumerate(r)))
    print("\n({} rows)".format(len(rows)), file=sys.stderr)


if __name__ == "__main__":
    main()
