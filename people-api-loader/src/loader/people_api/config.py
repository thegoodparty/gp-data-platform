"""People-API runtime configuration.

`LoaderConfig.from_env()` is the one entrypoint. Every step accepts a
`LoaderConfig` and a `run_date` — no step reaches into `os.environ` directly.

The one secret we can't avoid (the RDS master password) is fetched lazily
from AWS Secrets Manager — never read from the process environment. This
module hard-fails if `VOTER_DB_MASTER_PASSWORD` is set, so nobody can paper
over that contract by exporting it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

from loader.core.config import BaseLoaderConfig

DEFAULT_AWS_REGION = "us-west-2"
DEFAULT_S3_BUCKET = "gp-voter-loader"
DEFAULT_DATABRICKS_TABLE = "goodparty_data_catalog.dbt.int__l2_nationwide_uniform"

# Infrastructure identifiers (AWS account ID, cluster endpoint, prod DB name/user,
# VPC / subnet group / security group / KMS key) are intentionally NOT hardcoded here —
# this repo is public. They are supplied at runtime from the AWS Secrets Manager config
# secret named by LOADER_PROD_CONFIG_SECRET_ID (a JSON object; see the key list below),
# or per-field LOADER_* env vars (which take precedence over the secret). The live DB
# connection itself comes from ~/.pg_service.conf, never from these values.
_PLACEHOLDER = ""
DEFAULT_PROD_CLUSTER_ID = _PLACEHOLDER
DEFAULT_PROD_WRITER_ENDPOINT = _PLACEHOLDER
DEFAULT_PROD_SECRET_ID = _PLACEHOLDER
DEFAULT_PROD_DB_NAME = _PLACEHOLDER
DEFAULT_PROD_DB_USER = _PLACEHOLDER
DEFAULT_PROD_DB_PORT = 5432
DEFAULT_VPC_ID = _PLACEHOLDER
DEFAULT_DB_SUBNET_GROUP = _PLACEHOLDER
DEFAULT_SECURITY_GROUP_ID = _PLACEHOLDER
DEFAULT_KMS_KEY_ARN = _PLACEHOLDER
DEFAULT_AWS_ACCOUNT_ID = _PLACEHOLDER


def _fetch_prod_config_secret(region: str, profile: str | None, secret_id: str) -> dict[str, str]:
    """Fetch the prod infra config (a JSON object) from AWS Secrets Manager.

    Keeps infra identifiers out of this public repo. Recognized keys (all optional; a
    per-field LOADER_* env var overrides the secret, which overrides the placeholder):
    aws_account_id, prod_cluster_id, prod_writer_endpoint, prod_secret_id, prod_db_name,
    prod_db_user, prod_db_port, vpc_id, db_subnet_group, security_group_id, kms_key_arn.
    Returns the parsed dict; callers apply env-var overrides on top.
    """
    import json

    import boto3

    client = boto3.Session(profile_name=profile, region_name=region).client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_id)
    raw = resp.get("SecretString")
    if not raw:
        raise RuntimeError(f"prod config secret {secret_id!r} has no SecretString")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise RuntimeError(f"prod config secret {secret_id!r} must be a JSON object")
    return data


# Load-phase instance. Prod is serverless, but we use provisioned for load
# (see PLAN_LOADER.md "Provisioned-vs-Serverless"). Resize step flips this.
DEFAULT_LOAD_INSTANCE_CLASS = "db.r7g.16xlarge"
DEFAULT_SERVE_MIN_ACU = 0.5
DEFAULT_SERVE_MAX_ACU = 128.0
DEFAULT_ENGINE_VERSION = "16.8"

# Env-var name reserved for the master password. MUST NEVER be set — included
# only so we can detect and refuse it. See `LoaderConfig.from_env()`.
_FORBIDDEN_ENV_VAR = "VOTER_DB_MASTER_PASSWORD"


@dataclass(slots=True, kw_only=True)
class LoaderConfig(BaseLoaderConfig):
    """Concrete runtime config, fully resolved up front.

    Construct via `LoaderConfig.from_env()` so the forbidden-env-var guard
    runs. Downstream code takes a `LoaderConfig` parameter — never peeks at
    `os.environ`.
    """

    databricks_table: str

    # Prod (inspected / validated against)
    prod_cluster_id: str
    prod_writer_endpoint: str
    prod_secret_id: str
    prod_db_name: str
    prod_db_user: str
    prod_db_port: int

    # VPC / security
    vpc_id: str
    db_subnet_group: str
    security_group_id: str
    kms_key_arn: str

    # New cluster defaults
    engine_version: str
    load_instance_class: str
    serve_min_acu: float
    serve_max_acu: float

    @classmethod
    def from_env(cls) -> LoaderConfig:
        if _FORBIDDEN_ENV_VAR in os.environ:
            raise RuntimeError(
                f"${_FORBIDDEN_ENV_VAR} is set in the process environment. The "
                "loader fetches the RDS master password from AWS Secrets Manager "
                "at runtime; env-var passwords are not allowed. Unset it and re-run."
            )

        # Tagging for loader-created resources. The IAM role this loader runs
        # under has a permissions-boundary condition restricting most actions
        # to `ResourceTag/Environment = dev`, so every resource the loader
        # creates MUST carry `Environment=dev`. At cutover the ops team can
        # re-tag to `Environment=prod` to match the serving-cluster convention.
        tags = {
            "Project": os.environ.get("LOADER_TAG_PROJECT", "gp-api"),
            "Environment": os.environ.get("LOADER_TAG_ENVIRONMENT", "dev"),
        }

        # Infra identifiers come from the LOADER_PROD_CONFIG_SECRET_ID secret (kept out of
        # this public repo); per-field LOADER_* env vars override it, then empty placeholder.
        region = os.environ.get("AWS_REGION", DEFAULT_AWS_REGION)
        profile = os.environ.get("AWS_PROFILE")
        prod_secret_id_cfg = os.environ.get("LOADER_PROD_CONFIG_SECRET_ID")
        s = _fetch_prod_config_secret(region, profile, prod_secret_id_cfg) if prod_secret_id_cfg else {}

        return cls(
            aws_region=region,
            aws_profile=profile,
            account_id=os.environ.get(
                "LOADER_AWS_ACCOUNT_ID", s.get("aws_account_id", DEFAULT_AWS_ACCOUNT_ID)
            ),
            s3_bucket=os.environ.get("LOADER_S3_BUCKET", DEFAULT_S3_BUCKET),
            databricks_table=os.environ.get("LOADER_DATABRICKS_TABLE", DEFAULT_DATABRICKS_TABLE),
            prod_cluster_id=os.environ.get(
                "LOADER_PROD_CLUSTER_ID", s.get("prod_cluster_id", DEFAULT_PROD_CLUSTER_ID)
            ),
            prod_writer_endpoint=os.environ.get(
                "LOADER_PROD_WRITER_ENDPOINT", s.get("prod_writer_endpoint", DEFAULT_PROD_WRITER_ENDPOINT)
            ),
            prod_secret_id=os.environ.get(
                "LOADER_PROD_SECRET_ID", s.get("prod_secret_id", DEFAULT_PROD_SECRET_ID)
            ),
            prod_db_name=os.environ.get("LOADER_PROD_DB_NAME", s.get("prod_db_name", DEFAULT_PROD_DB_NAME)),
            prod_db_user=os.environ.get("LOADER_PROD_DB_USER", s.get("prod_db_user", DEFAULT_PROD_DB_USER)),
            prod_db_port=int(
                os.environ.get("LOADER_PROD_DB_PORT", s.get("prod_db_port", DEFAULT_PROD_DB_PORT))
            ),
            vpc_id=os.environ.get("LOADER_VPC_ID", s.get("vpc_id", DEFAULT_VPC_ID)),
            db_subnet_group=os.environ.get(
                "LOADER_DB_SUBNET_GROUP", s.get("db_subnet_group", DEFAULT_DB_SUBNET_GROUP)
            ),
            security_group_id=os.environ.get(
                "LOADER_SECURITY_GROUP_ID", s.get("security_group_id", DEFAULT_SECURITY_GROUP_ID)
            ),
            kms_key_arn=os.environ.get("LOADER_KMS_KEY_ARN", s.get("kms_key_arn", DEFAULT_KMS_KEY_ARN)),
            engine_version=os.environ.get("LOADER_ENGINE_VERSION", DEFAULT_ENGINE_VERSION),
            load_instance_class=os.environ.get("LOADER_LOAD_INSTANCE_CLASS", DEFAULT_LOAD_INSTANCE_CLASS),
            serve_min_acu=float(os.environ.get("LOADER_SERVE_MIN_ACU", DEFAULT_SERVE_MIN_ACU)),
            serve_max_acu=float(os.environ.get("LOADER_SERVE_MAX_ACU", DEFAULT_SERVE_MAX_ACU)),
            _tags=tags,
        )

    def export_prefix(self, run_date: str) -> str:
        return f"voter_export_{run_date}"

    def new_cluster_id(self, run_date: str) -> str:
        return f"gp-people-db-{run_date}"

    def new_writer_instance_id(self, run_date: str) -> str:
        return f"gp-people-db-{run_date}-writer"

    def new_master_secret_id(self, run_date: str) -> str:
        return f"gp-people-db/{run_date}/master"

    def new_iam_role_name(self, run_date: str) -> str:
        return f"rds-s3-import-{run_date}"

    def new_load_param_group(self, run_date: str) -> str:
        return f"gp-people-db-{run_date}-load"

    def new_serve_param_group(self, run_date: str) -> str:
        return f"gp-people-db-{run_date}-serve"


def require_run_date(run_date: str) -> str:
    """Validate the run_date stamp — 'YYYYMMDD'."""
    try:
        datetime.strptime(run_date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"run_date must be YYYYMMDD (got {run_date!r})") from e
    return run_date
