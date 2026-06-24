"""
Promote viability MLflow models from sandbox to model_predictions.

Run once after this PR merges, before the new dbt model is executed.
Safe to re-run: skips copy if @prod alias already points at a version
promoted from the same sandbox source URI (tracked via a model version tag).

Requires env vars: DATABRICKS_HOST, DATABRICKS_TOKEN
"""

import os

import mlflow

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]  # e.g. https://dbc-3d8ca484-79f3.cloud.databricks.com
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]  # Databricks PAT — set in env before running

mlflow.set_tracking_uri("databricks")

client = mlflow.tracking.MlflowClient()

# Maps sandbox model name → latest sandbox version to promote.
# Update these version numbers if models are retrained before this script runs.
MODELS_TO_PROMOTE = [
    ("viabilitywithopponentdata", 5),
    ("viabilitywithoutopenseat", 5),
    ("viabilitynoincumbency", 2),
    ("viabilitynoopponentdata", 2),
    ("viabilitynocandidatedatahs", 2),
]

SANDBOX_CATALOG = "goodparty_data_catalog.sandbox"
PROD_CATALOG = "goodparty_data_catalog.model_predictions"
PROD_ALIAS = "prod"
TAG_PROMOTED_FROM = "promoted_from"


def _prod_alias_source_tag(prod_name: str) -> str | None:
    """Return the promoted_from tag on the current @prod version, or None."""
    try:
        mv = client.get_model_version_by_alias(prod_name, PROD_ALIAS)
        return mv.tags.get(TAG_PROMOTED_FROM)
    except mlflow.exceptions.MlflowException as e:
        if e.error_code == "RESOURCE_DOES_NOT_EXIST":
            return None
        raise


def _ensure_registered_model(name: str) -> None:
    """Create the destination registered model if it does not exist yet.

    copy_model_version does not reliably auto-create the destination registered
    model in Unity Catalog, so create it first (idempotently). Three of the five
    viability models exist only in sandbox, not in model_predictions, on first run.
    """
    try:
        client.create_registered_model(name)
        print(f"  CREATE registered model {name}")
    except mlflow.exceptions.MlflowException as e:
        if e.error_code != "RESOURCE_ALREADY_EXISTS":
            raise


def promote(sandbox_model: str, sandbox_version: int) -> None:
    src_uri = f"models:/{SANDBOX_CATALOG}.{sandbox_model}/{sandbox_version}"
    dst_name = f"{PROD_CATALOG}.{sandbox_model}"

    if _prod_alias_source_tag(dst_name) == src_uri:
        print(f"  SKIP  {sandbox_model}: @prod already promoted from {src_uri}")
        return

    _ensure_registered_model(dst_name)
    print(f"  COPY  {src_uri}  →  {dst_name}")
    new_mv = client.copy_model_version(
        src_model_uri=src_uri,
        dst_name=dst_name,
    )
    new_version = new_mv.version
    print(f"        created version {new_version} in {dst_name}")

    client.set_model_version_tag(
        name=dst_name,
        version=new_version,
        key=TAG_PROMOTED_FROM,
        value=src_uri,
    )

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
