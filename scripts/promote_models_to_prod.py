"""
promote_models_to_prod.py

- Installs the pinned LightGBM version on the all-purpose cluster.
- Registers new versions of the four voter turnout models under model_predictions.
- Sets the @production alias on each new version (the visible promotion step).

Run once after each retrain:
    uv run scripts/promote_models_to_prod.py

Prerequisites:
- DATABRICKS_HOST and DATABRICKS_TOKEN set in environment, OR edit the
  constants below.
- The source models must have the @latest_ alias set in private_nigel.
- The all-purpose cluster must be running.
"""
import json
import os
import time

import mlflow
import requests

# ── Config ────────────────────────────────────────────────────────────────────

HOST  = os.environ.get("DATABRICKS_HOST", "https://dbc-3d8ca484-79f3.cloud.databricks.com")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
H     = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Cluster that dbt Python models run on.  LightGBM must be installed here.
ALL_PURPOSE_CLUSTER_ID = "0409-211859-6hzpukya"

# LightGBM version pinned to match training environment (DBR ML runtime).
# Update this if you retrain on a newer runtime.
LIGHTGBM_VERSION = "4.6.0"

SRC_CATALOG = "goodparty_data_catalog"
SRC_SCHEMA  = "private_nigel"
DST_CATALOG = "goodparty_data_catalog"
DST_SCHEMA  = "model_predictions"

# The precinct opportunity table used by inference to determine 1/0/NULL vote history encoding.
# Lives in sandbox during development; must be promoted alongside models so inference can read it.
PRECINCT_OPP_SRC = f"{SRC_CATALOG}.sandbox.turnout_historical_precincts"
PRECINCT_OPP_DST = f"{DST_CATALOG}.{DST_SCHEMA}.turnout_historical_precincts"

SLUGS = [
    "presidential_lag3",
    "midterm",
    "even_year_local",
    "off_year_local_lag2",
]

# Pin source version numbers explicitly so the PR diff shows exactly what is
# being promoted.  Update these after each retrain.
SRC_VERSIONS = {
    "presidential_lag3":   1,
    "midterm":             2,
    "even_year_local":     3,
    "off_year_local_lag2": 2,
}



# ── Step 1: Install LightGBM on the all-purpose cluster ──────────────────────

def copy_precinct_opportunity_table() -> None:
    """Copy turnout_historical_precincts from sandbox to model_predictions."""
    print(f"\n[1/4] Copying precinct opportunity table...")
    print(f"  {PRECINCT_OPP_SRC} → {PRECINCT_OPP_DST}")

    r = requests.post(f"{HOST}/api/2.1/jobs/runs/submit", headers=H, json={
        "run_name": "copy_turnout_historical_precincts",
        "existing_cluster_id": ALL_PURPOSE_CLUSTER_ID,
        "notebook_task": {
            "notebook_path": None,  # not applicable; use spark_python_task instead
        },
    })
    # Use SQL API since this is a simple CTAS — simpler than a notebook run.
    wh_id = "18583d8b081c6486"
    sql = f"CREATE OR REPLACE TABLE {PRECINCT_OPP_DST} AS SELECT * FROM {PRECINCT_OPP_SRC}"
    r = requests.post(f"{HOST}/api/2.0/sql/statements", headers=H, json={
        "warehouse_id": wh_id,
        "statement": sql,
        "wait_timeout": "120s",
    }, timeout=150)
    r.raise_for_status()
    result = r.json()
    status = result.get("status", {}).get("state")
    if status != "SUCCEEDED":
        raise RuntimeError(
            f"Failed to copy precinct opportunity table: {result.get('status')}"
        )
    print("  done.")


def install_lightgbm_on_cluster(cluster_id: str, version: str) -> None:
    print(f"\n[2/4] Installing lightgbm=={version} on cluster {cluster_id}...")

    # Check current state — install API requires a running cluster.
    r = requests.get(f"{HOST}/api/2.0/clusters/get", headers=H,
                     params={"cluster_id": cluster_id}, timeout=15)
    r.raise_for_status()
    state = r.json().get("state")
    if state != "RUNNING":
        raise RuntimeError(
            f"Cluster {cluster_id} is in state '{state}'. Start it first."
        )

    r = requests.post(f"{HOST}/api/2.0/libraries/install", headers=H, json={
        "cluster_id": cluster_id,
        "libraries": [{"pypi": {"package": f"lightgbm=={version}"}}],
    }, timeout=15)
    r.raise_for_status()

    # Poll until installed (or failed).
    print(f"  waiting for install to complete ", end="", flush=True)
    for _ in range(60):
        time.sleep(5)
        st = requests.get(f"{HOST}/api/2.0/libraries/cluster-status", headers=H,
                          params={"cluster_id": cluster_id}, timeout=15).json()
        lib_statuses = st.get("library_statuses", [])
        for ls in lib_statuses:
            spec = ls.get("library", {})
            if spec.get("pypi", {}).get("package", "").startswith("lightgbm"):
                status = ls["status"]
                print(f"\r  {status:<30}", end="", flush=True)
                if status == "INSTALLED":
                    print()
                    return
                if status in ("FAILED", "UNINSTALL_ON_RESTART"):
                    print()
                    raise RuntimeError(
                        f"LightGBM install failed: {ls.get('messages')}"
                    )
        print(".", end="", flush=True)
    raise RuntimeError("Timed out waiting for LightGBM install.")


# ── Step 2: Register new versions under model_predictions ────────────────────

def promote_models(client: mlflow.MlflowClient) -> dict[str, str]:
    """Register each slug under DST and return {slug: new_version}."""
    print("\n[3/4] Registering model versions under model_predictions...")
    new_versions = {}

    for slug in SLUGS:
        src_name = f"{SRC_CATALOG}.{SRC_SCHEMA}.voter_turnout_model_{slug}"
        dst_name = f"{DST_CATALOG}.{DST_SCHEMA}.voter_turnout_model_{slug}"
        src_version = SRC_VERSIONS[slug]

        src_mv = client.get_model_version(src_name, src_version)
        artifact_uri = src_mv.source  # points to the run artifact directory

        # Ensure destination registered model exists.
        try:
            client.get_registered_model(dst_name)
        except mlflow.exceptions.MlflowException:
            client.create_registered_model(dst_name)
            print(f"  created registered model {dst_name}")

        new_mv = client.create_model_version(
            name=dst_name,
            source=artifact_uri,
            run_id=src_mv.run_id,
        )
        new_versions[slug] = new_mv.version
        print(f"  {slug}: v{src_version} → {dst_name} v{new_mv.version}")

    return new_versions


# ── Step 3: Tag and alias ─────────────────────────────────────────────────────

def tag_and_alias(client: mlflow.MlflowClient, new_versions: dict[str, str]) -> None:
    print("\n[4/4] Setting tags and @production alias...")

    for slug, version in new_versions.items():
        dst_name = f"{DST_CATALOG}.{DST_SCHEMA}.voter_turnout_model_{slug}"

        client.set_registered_model_tag(dst_name, "lightgbm_version", LIGHTGBM_VERSION)

        # @production is the alias the inference dbt models read from.
        # Setting it here is an explicit, reviewable promotion step.
        client.set_registered_model_alias(dst_name, "production", version)
        print(f"  {slug} v{version} → @production")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DATABRICKS_TOKEN environment variable.")

    copy_precinct_opportunity_table()
    install_lightgbm_on_cluster(ALL_PURPOSE_CLUSTER_ID, LIGHTGBM_VERSION)

    mlflow.set_tracking_uri(HOST)
    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.MlflowClient()

    new_versions = promote_models(client)
    tag_and_alias(client, new_versions)

    print("\nDone. Summary:")
    for slug, v in new_versions.items():
        dst_name = f"{DST_CATALOG}.{DST_SCHEMA}.voter_turnout_model_{slug}"
        print(f"  {dst_name} v{v} @production")
