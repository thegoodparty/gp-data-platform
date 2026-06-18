"""
promote_models_to_sandbox.py

Dev/test version of promote_models_to_prod.py.
Copies the four voter turnout models from private_nigel → sandbox and sets
@production aliases there, so you can run the dbt inference models against
sandbox before the real promotion to model_predictions.

Run:
    uv run scripts/promote_models_to_sandbox.py

After this script succeeds, test one state:
    dbt run -s int__voter_turnout_lgbm_inference_al --vars '{"voter_turnout_models_schema": "sandbox"}'

Prerequisites:
- DATABRICKS_HOST and DATABRICKS_TOKEN set in environment, OR edit the
  constants below.
- The all-purpose cluster must be running (lightgbm already installed, or
  run promote_models_to_prod.py first which installs it).
"""
import os

import mlflow

# ── Config ────────────────────────────────────────────────────────────────────

HOST  = os.environ.get("DATABRICKS_HOST", "https://dbc-3d8ca484-79f3.cloud.databricks.com")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

LIGHTGBM_VERSION = "4.6.0"

SRC_CATALOG = "goodparty_data_catalog"
SRC_SCHEMA  = "private_nigel"
DST_CATALOG = "goodparty_data_catalog"
DST_SCHEMA  = "sandbox"

SLUGS = [
    "presidential_lag3",
    "midterm",
    "even_year_local",
    "off_year_local_lag2",
]

SRC_VERSIONS = {
    "presidential_lag3":   1,
    "midterm":             2,
    "even_year_local":     3,
    "off_year_local_lag2": 2,
}


# ── Step 1: Register versions under sandbox ───────────────────────────────────

def promote_to_sandbox(client: mlflow.MlflowClient) -> dict[str, str]:
    print("\n[1/2] Registering model versions under sandbox...")
    new_versions = {}

    for slug in SLUGS:
        src_name = f"{SRC_CATALOG}.{SRC_SCHEMA}.voter_turnout_model_{slug}"
        dst_name = f"{DST_CATALOG}.{DST_SCHEMA}.voter_turnout_model_{slug}"
        src_version = SRC_VERSIONS[slug]

        src_mv = client.get_model_version(src_name, src_version)
        artifact_uri = src_mv.source

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


# ── Step 2: Tag, alias, then sanity-check ─────────────────────────────────────

def tag_alias_and_verify(client: mlflow.MlflowClient, new_versions: dict[str, str]) -> None:
    print("\n[2/2] Setting tags and @production alias...")

    for slug, version in new_versions.items():
        dst_name = f"{DST_CATALOG}.{DST_SCHEMA}.voter_turnout_model_{slug}"
        client.set_registered_model_tag(dst_name, "lightgbm_version", LIGHTGBM_VERSION)
        client.set_registered_model_alias(dst_name, "production", version)
        print(f"  {slug} v{version} → @production")

    print("\nSanity check — resolving @production aliases:")
    all_ok = True
    for slug in SLUGS:
        dst_name = f"{DST_CATALOG}.{DST_SCHEMA}.voter_turnout_model_{slug}"
        try:
            mv = client.get_model_version_by_alias(dst_name, "production")
            lgbm_tag = client.get_registered_model(dst_name).tags.get("lightgbm_version", "NOT SET")
            print(f"  OK  {slug}  v{mv.version}  run_id={mv.run_id[:8]}...  lightgbm_version={lgbm_tag}")
        except Exception as e:
            print(f"  FAIL  {slug}  {e}")
            all_ok = False

    if not all_ok:
        raise SystemExit("One or more aliases could not be resolved. Check errors above.")
    print("\nAll aliases verified. Ready to test:")
    print("  dbt run -s int__voter_turnout_lgbm_inference_al --vars '{\"voter_turnout_models_schema\": \"sandbox\"}'")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DATABRICKS_TOKEN environment variable.")

    mlflow.set_tracking_uri(HOST)
    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.MlflowClient()

    new_versions = promote_to_sandbox(client)
    tag_alias_and_verify(client, new_versions)
