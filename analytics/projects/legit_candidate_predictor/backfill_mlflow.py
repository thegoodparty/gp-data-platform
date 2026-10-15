"""DATA-2248 Phase 1: one-time backfill of results_ledger.jsonl into MLflow.

Replays every ledger row as an MLflow run in /Shared/legit_candidate_predictor,
with start_time set from the row's date and tag backfill=true so replayed runs
are distinguishable from live ones. Idempotence guard: refuses to run if any
backfilled run already exists (pass --force to add anyway).

Run (from analytics/):
  uv run --with mlflow-skinny --with pyarrow python \
      projects/legit_candidate_predictor/backfill_mlflow.py
"""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient

HERE = Path(__file__).resolve().parent
LEDGER = HERE / "results_ledger.jsonl"
MANIFESTS = HERE / "manifests"
TRACKING_URI = "databricks://tristana_goodparty"
EXPERIMENT = "/Shared/legit_candidate_predictor"

ap = argparse.ArgumentParser()
ap.add_argument("--force", action="store_true", help="backfill even if backfilled runs exist")
args = ap.parse_args()

mlflow.set_tracking_uri(TRACKING_URI)
client = MlflowClient()
exp = client.get_experiment_by_name(EXPERIMENT)
exp_id = exp.experiment_id if exp else client.create_experiment(EXPERIMENT)

existing = client.search_runs([exp_id], "tags.backfill = 'true'", max_results=1)
if existing and not args.force:
    raise SystemExit("backfilled runs already present; refusing to duplicate (use --force)")

rows = [json.loads(line) for line in LEDGER.open()]
print(f"backfilling {len(rows)} ledger rows -> {EXPERIMENT} (id {exp_id})")

for i, e in enumerate(rows):
    ts_ms = int(datetime.strptime(e["date"], "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1000)
    run = client.create_run(exp_id, start_time=ts_ms, run_name=e["experiment"])
    rid = run.info.run_id
    for k in ("split", "model", "ws_version", "model_id", "git_commit"):
        if e.get(k):
            client.log_param(rid, k, e[k])
    for k, v in e["metrics"].items():
        if isinstance(v, int | float):
            client.log_metric(rid, k, float(v), timestamp=ts_ms)
    client.set_tag(rid, "area", e.get("area", ""))
    client.set_tag(rid, "notes", e.get("notes", ""))
    client.set_tag(rid, "ledger_date", e["date"])
    client.set_tag(rid, "backfill", "true")
    if e.get("model_id") and (MANIFESTS / f"{e['model_id']}.json").exists():
        client.log_artifact(rid, str(MANIFESTS / f"{e['model_id']}.json"))
    client.set_terminated(rid, end_time=ts_ms)
    print(f"  {i + 1:2d}/{len(rows)} {e['date']} {e['experiment'][:60]!r} -> {rid[:8]}")

print("backfill complete")
