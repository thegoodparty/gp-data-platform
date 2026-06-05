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

DEFAULT_PROD_CLUSTER_ID = "gp-voter-db-20250728"
DEFAULT_PROD_WRITER_ENDPOINT = "gp-voter-db-20250728.cluster-cmb1uukjsfbe.us-west-2.rds.amazonaws.com"
DEFAULT_PROD_SECRET_ID = "gp-voter-db-20250728/master"

DEFAULT_VPC_ID = "vpc-0763fa52c32ebcf6a"
DEFAULT_DB_SUBNET_GROUP = "api-master-rds-subnet-group"
DEFAULT_SECURITY_GROUP_ID = "sg-03783e4adbbee87dc"
DEFAULT_KMS_KEY_ARN = "arn:aws:kms:us-west-2:333022194791:key/a728ea20-f375-4039-b7c1-ef9ff192abcc"
DEFAULT_AWS_ACCOUNT_ID = "333022194791"

DEFAULT_VOTER_DB_NAME = "voters"
DEFAULT_VOTER_DB_USER = "postgres"
DEFAULT_VOTER_DB_PORT = 5432

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

        return cls(
            aws_region=os.environ.get("AWS_REGION", DEFAULT_AWS_REGION),
            aws_profile=os.environ.get("AWS_PROFILE"),
            account_id=os.environ.get("LOADER_AWS_ACCOUNT_ID", DEFAULT_AWS_ACCOUNT_ID),
            s3_bucket=os.environ.get("LOADER_S3_BUCKET", DEFAULT_S3_BUCKET),
            databricks_table=os.environ.get("LOADER_DATABRICKS_TABLE", DEFAULT_DATABRICKS_TABLE),
            prod_cluster_id=os.environ.get("LOADER_PROD_CLUSTER_ID", DEFAULT_PROD_CLUSTER_ID),
            prod_writer_endpoint=os.environ.get("LOADER_PROD_WRITER_ENDPOINT", DEFAULT_PROD_WRITER_ENDPOINT),
            prod_secret_id=os.environ.get("LOADER_PROD_SECRET_ID", DEFAULT_PROD_SECRET_ID),
            prod_db_name=os.environ.get("LOADER_PROD_DB_NAME", DEFAULT_VOTER_DB_NAME),
            prod_db_user=os.environ.get("LOADER_PROD_DB_USER", DEFAULT_VOTER_DB_USER),
            prod_db_port=int(os.environ.get("LOADER_PROD_DB_PORT", DEFAULT_VOTER_DB_PORT)),
            vpc_id=os.environ.get("LOADER_VPC_ID", DEFAULT_VPC_ID),
            db_subnet_group=os.environ.get("LOADER_DB_SUBNET_GROUP", DEFAULT_DB_SUBNET_GROUP),
            security_group_id=os.environ.get("LOADER_SECURITY_GROUP_ID", DEFAULT_SECURITY_GROUP_ID),
            kms_key_arn=os.environ.get("LOADER_KMS_KEY_ARN", DEFAULT_KMS_KEY_ARN),
            engine_version=os.environ.get("LOADER_ENGINE_VERSION", DEFAULT_ENGINE_VERSION),
            load_instance_class=os.environ.get("LOADER_LOAD_INSTANCE_CLASS", DEFAULT_LOAD_INSTANCE_CLASS),
            serve_min_acu=float(os.environ.get("LOADER_SERVE_MIN_ACU", DEFAULT_SERVE_MIN_ACU)),
            serve_max_acu=float(os.environ.get("LOADER_SERVE_MAX_ACU", DEFAULT_SERVE_MAX_ACU)),
            _tags=tags,
        )

    def export_prefix(self, run_date: str) -> str:
        return f"voter_export_{run_date}"

    def new_cluster_id(self, run_date: str) -> str:
        return f"gp-voter-db-{run_date}"

    def new_writer_instance_id(self, run_date: str) -> str:
        return f"gp-voter-db-{run_date}-writer"

    def new_master_secret_id(self, run_date: str) -> str:
        return f"gp-voter-db/{run_date}/master"

    def new_iam_role_name(self, run_date: str) -> str:
        return f"rds-s3-import-{run_date}"

    def new_load_param_group(self, run_date: str) -> str:
        return f"gp-voter-db-{run_date}-load"

    def new_serve_param_group(self, run_date: str) -> str:
        return f"gp-voter-db-{run_date}-serve"


def require_run_date(run_date: str) -> str:
    """Validate the run_date stamp — 'YYYYMMDD'."""
    try:
        datetime.strptime(run_date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"run_date must be YYYYMMDD (got {run_date!r})") from e
    return run_date
