"""
Promote viability MLflow models from sandbox to model_predictions.

Run once after this PR merges, before the new dbt model is executed.
Safe to re-run: skips copy if @prod alias already points at a version
copied from the same sandbox source URI.

Usage (locally via uv):
    UV="/c/Users/nigel/AppData/Local/Packages/PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0/LocalCache/local-packages/Python313/Scripts/uv.exe"
    "$UV" run --with mlflow --with databricks-sdk python promote_viability_models.py

Usage (Databricks notebook): paste and run directly — mlflow is pre-installed.
"""

import mlflow
import os

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]   # e.g. https://dbc-3d8ca484-79f3.cloud.databricks.com
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"] # Databricks PAT — set in env before running

mlflow.set_tracking_uri("databricks")

client = mlflow.tracking.MlflowClient()

# Maps sandbox model name → latest sandbox version to promote.
# Update these version numbers if models are retrained before this script runs.
MODELS_TO_PROMOTE = [
    ("viabilitywithopponentdata",  5),
    ("viabilitywithoutopenseat",   5),
    ("viabilitynoincumbency",      2),
    ("viabilitynoopponentdata",    2),
    ("viabilitynocandidatedatahs", 2),
]

SANDBOX_CATALOG    = "goodparty_data_catalog.sandbox"
PROD_CATALOG       = "goodparty_data_catalog.model_predictions"
PROD_ALIAS         = "prod"


def _current_prod_source(prod_name: str) -> str | None:
    """Return the source_model_uri of the version currently tagged @prod, or None."""
    try:
        mv = client.get_model_version_by_alias(prod_name, PROD_ALIAS)
        return mv.source
    except mlflow.exceptions.MlflowException:
        return None


def promote(sandbox_model: str, sandbox_version: int) -> None:
    src_uri  = f"models:/{SANDBOX_CATALOG}.{sandbox_model}/{sandbox_version}"
    dst_name = f"{PROD_CATALOG}.{sandbox_model}"

    current_source = _current_prod_source(dst_name)
    if current_source == src_uri:
        print(f"  SKIP  {sandbox_model}: @prod already points at {src_uri}")
        return

    print(f"  COPY  {src_uri}  →  {dst_name}")
    new_mv = client.copy_model_version(
        src_model_uri=src_uri,
        dst_name=dst_name,
    )
    new_version = new_mv.version
    print(f"        created version {new_version} in {dst_name}")

    print(f"  ALIAS {dst_name}@{PROD_ALIAS}  →  v{new_version}")
    client.set_registered_model_alias(
        name=dst_name,
        alias=PROD_ALIAS,
        version=new_version,
    )
    print(f"  DONE  {sandbox_model}")


if __name__ == "__main__":
    print(f"Promoting {len(MODELS_TO_PROMOTE)} models to {PROD_CATALOG}\n")
    for model_name, version in MODELS_TO_PROMOTE:
        promote(model_name, version)
    print("\nAll done.")
