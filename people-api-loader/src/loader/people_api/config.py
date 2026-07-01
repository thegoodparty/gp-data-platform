"""People-API runtime configuration.

`LoaderConfig.from_env()` is the one entrypoint. Every step accepts a
`LoaderConfig` and a `run_date` — no step reaches into `os.environ` directly.

Connections are never assembled from a process-env password. Both clusters
are reached through SSM SecureString connection strings: the Present cluster
via `db_conn_param`, and the freshly-provisioned cluster via `new_conn_param`
(provision writes that one, embedding the generated master password). This
module hard-fails if `VOTER_DB_MASTER_PASSWORD` is set, so nobody can paper
over that contract by exporting it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

from loader.core.config import BaseLoaderConfig

DEFAULT_AWS_REGION = "us-west-2"
# The loader S3 bucket is real infrastructure and this repo is public, so it is NOT hardcoded.
# It must come from LOADER_S3_BUCKET; from_env() hard-fails if unset. Empty = placeholder.
DEFAULT_S3_BUCKET = ""

# Connection strings live in SSM Parameter Store as SecureStrings, keyed by env
# (LOADER_ENV, dev/qa/prod). The Present cluster is `{CONN_PARAM_PREFIX}-{env}`; each
# provisioned cluster is `{CONN_PARAM_PREFIX}-{env}-{run_date}` (unique per run, no collision
# with the serving cluster). connect_prod/connect_new fetch and decrypt at connect time —
# nothing connection-related is committed here.
DEFAULT_DB_ENV = "dev"
CONN_PARAM_PREFIX = "people-db-connection-string"

# Infrastructure identifiers (AWS account ID, VPC / subnet group / security group / KMS
# key, plus the new cluster's DB name/user/port) are intentionally NOT hardcoded — this
# repo is public. They come from per-field LOADER_* env vars (provision consumes them;
# provision is still a stub). Empty placeholders are the out-of-the-box default.
_PLACEHOLDER = ""
DEFAULT_PROD_CLUSTER_ID = _PLACEHOLDER
DEFAULT_PROD_DB_NAME = _PLACEHOLDER
DEFAULT_PROD_DB_USER = _PLACEHOLDER
DEFAULT_PROD_DB_PORT = 5432
DEFAULT_VPC_ID = _PLACEHOLDER
DEFAULT_DB_SUBNET_GROUP = _PLACEHOLDER
DEFAULT_SECURITY_GROUP_ID = _PLACEHOLDER
DEFAULT_KMS_KEY_ARN = _PLACEHOLDER
DEFAULT_AWS_ACCOUNT_ID = _PLACEHOLDER
# Durable rds-s3-import role (created once by platform IaC, not per run). provision
# references and attaches it via PassRole; it does not create it.
DEFAULT_S3_IMPORT_ROLE_ARN = _PLACEHOLDER


# Load-phase instance. Prod is serverless, but we use provisioned for load
# (see PLAN_LOADER.md "Provisioned-vs-Serverless"). Resize step flips this.
DEFAULT_LOAD_INSTANCE_CLASS = "db.r7g.16xlarge"
DEFAULT_SERVE_MIN_ACU = 0.5
DEFAULT_SERVE_MAX_ACU = 128.0
DEFAULT_ENGINE_VERSION = "16.8"

# Env-var name reserved for the master password. MUST NEVER be set — included
# only so we can detect and refuse it. See `LoaderConfig.from_env()`.
_FORBIDDEN_ENV_VAR = "VOTER_DB_MASTER_PASSWORD"

# DDL generation reads column schemas from these dbt marts (Unity Catalog). The PG table
# name (key) maps to the mart's fully-qualified name. Default schema matches the int model.
DEFAULT_MART_CATALOG_SCHEMA = "goodparty_data_catalog.dbt"
_MART_MODELS = {
    "Voter": "m_people_api__voter",
    "District": "m_people_api__district",
    "DistrictStats": "m_people_api__districtstats",
    "DistrictVoter": "m_people_api__districtvoter",
}


@dataclass(slots=True, kw_only=True)
class LoaderConfig(BaseLoaderConfig):
    """Concrete runtime config, fully resolved up front.

    Construct via `LoaderConfig.from_env()` so the forbidden-env-var guard
    runs. Downstream code takes a `LoaderConfig` parameter — never peeks at
    `os.environ`.
    """

    # Databricks SQL warehouse the unload step submits INSERT OVERWRITE DIRECTORY to.
    databricks_warehouse_id: str

    # Deployment env (dev/qa/prod) — keys the SSM connection-string parameter names.
    db_env: str
    # SSM Parameter Store name holding the Present-cluster connection string (SecureString).
    db_conn_param: str

    # Prod (inspected / validated against)
    prod_cluster_id: str
    prod_db_name: str
    prod_db_user: str
    prod_db_port: int

    # VPC / security
    vpc_id: str
    db_subnet_group: str
    security_group_id: str
    kms_key_arn: str
    s3_import_role_arn: str

    # Bastion (SSH) for reaching Postgres from outside the VPC (Astro worker).
    # Empty host = connect directly (local/VPN runs).
    bastion_host: str
    bastion_port: int
    bastion_user: str
    bastion_private_key: str
    bastion_private_key_passphrase: str

    # New cluster defaults
    engine_version: str
    load_instance_class: str
    serve_min_acu: float
    serve_max_acu: float

    # PG table name -> Databricks mart FQN (column/type source for emit-ddl).
    mart_fqns: dict[str, str]

    @classmethod
    def from_env(cls) -> LoaderConfig:
        if _FORBIDDEN_ENV_VAR in os.environ:
            raise RuntimeError(
                f"${_FORBIDDEN_ENV_VAR} is set in the process environment. The loader "
                "builds connections from SSM SecureString connection strings at runtime; "
                "env-var passwords are not allowed. Unset it and re-run."
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

        # Present-cluster connection comes from an SSM SecureString (connect_prod fetches it);
        # the param name is people-db-connection-string-{LOADER_ENV} unless fully overridden.
        env = os.environ.get("LOADER_ENV", DEFAULT_DB_ENV)
        db_conn_param = os.environ.get("LOADER_DB_CONN_PARAM", f"{CONN_PARAM_PREFIX}-{env}")
        # Loader output bucket is used from the first step (inspect) onward. It is real infra, not
        # committed to this public repo, so require it explicitly — a clear failure here beats an
        # opaque S3 AccessDenied/NoSuchBucket later (e.g. against a stale default bucket).
        s3_bucket = os.environ.get("LOADER_S3_BUCKET", DEFAULT_S3_BUCKET)
        if not s3_bucket:
            raise RuntimeError(
                "LOADER_S3_BUCKET is not set. The loader's S3 bucket is real infrastructure and is "
                "not committed to this public repo; set LOADER_S3_BUCKET (e.g. on the Astro "
                "deployment) to the loader bucket."
            )
        # Infra identifiers (provision-only; provision is a stub) come from LOADER_* env vars,
        # else empty placeholders — nothing infra-identifying is committed to this public repo.
        mart_schema = os.environ.get("LOADER_MART_CATALOG_SCHEMA", DEFAULT_MART_CATALOG_SCHEMA)
        mart_fqns = {pg: f"{mart_schema}.{model}" for pg, model in _MART_MODELS.items()}
        return cls(
            aws_region=os.environ.get("AWS_REGION", DEFAULT_AWS_REGION),
            aws_profile=os.environ.get("AWS_PROFILE"),
            # Cross-account RDS-admin role assumed for all AWS calls (the Astro worker's identity
            # lives in a different account). Unset for local runs whose ambient creds already have
            # access. ExternalId is required by the role's trust policy when present.
            assume_role_arn=os.environ.get("AWS_PEOPLE_API_RDS_ROLE_ARN") or None,
            assume_role_external_id=os.environ.get("AWS_PEOPLE_API_RDS_EXTERNAL_ID") or None,
            account_id=os.environ.get("LOADER_AWS_ACCOUNT_ID", DEFAULT_AWS_ACCOUNT_ID),
            s3_bucket=s3_bucket,
            databricks_warehouse_id=os.environ.get("LOADER_DATABRICKS_WAREHOUSE_ID", ""),
            db_env=env,
            db_conn_param=db_conn_param,
            prod_cluster_id=os.environ.get("LOADER_PROD_CLUSTER_ID", DEFAULT_PROD_CLUSTER_ID),
            prod_db_name=os.environ.get("LOADER_PROD_DB_NAME", DEFAULT_PROD_DB_NAME),
            prod_db_user=os.environ.get("LOADER_PROD_DB_USER", DEFAULT_PROD_DB_USER),
            prod_db_port=int(os.environ.get("LOADER_PROD_DB_PORT", DEFAULT_PROD_DB_PORT)),
            vpc_id=os.environ.get("LOADER_VPC_ID", DEFAULT_VPC_ID),
            db_subnet_group=os.environ.get("LOADER_DB_SUBNET_GROUP", DEFAULT_DB_SUBNET_GROUP),
            security_group_id=os.environ.get("LOADER_SECURITY_GROUP_ID", DEFAULT_SECURITY_GROUP_ID),
            kms_key_arn=os.environ.get("LOADER_KMS_KEY_ARN", DEFAULT_KMS_KEY_ARN),
            s3_import_role_arn=os.environ.get("LOADER_S3_IMPORT_ROLE_ARN", DEFAULT_S3_IMPORT_ROLE_ARN),
            bastion_host=os.environ.get("LOADER_BASTION_HOST", ""),
            bastion_port=int(os.environ.get("LOADER_BASTION_PORT", "22")),
            bastion_user=os.environ.get("LOADER_BASTION_USER", ""),
            bastion_private_key=os.environ.get("LOADER_BASTION_PRIVATE_KEY", ""),
            bastion_private_key_passphrase=os.environ.get("LOADER_BASTION_KEY_PASSPHRASE", ""),
            engine_version=os.environ.get("LOADER_ENGINE_VERSION", DEFAULT_ENGINE_VERSION),
            load_instance_class=os.environ.get("LOADER_LOAD_INSTANCE_CLASS", DEFAULT_LOAD_INSTANCE_CLASS),
            serve_min_acu=float(os.environ.get("LOADER_SERVE_MIN_ACU", DEFAULT_SERVE_MIN_ACU)),
            serve_max_acu=float(os.environ.get("LOADER_SERVE_MAX_ACU", DEFAULT_SERVE_MAX_ACU)),
            mart_fqns=mart_fqns,
            _tags=tags,
        )

    @property
    def bastion_enabled(self) -> bool:
        return bool(self.bastion_host)

    def export_prefix(self, run_date: str) -> str:
        return f"voter_export_{run_date}"

    def new_cluster_id(self, run_date: str) -> str:
        return f"gp-people-db-{run_date}"

    def new_writer_instance_id(self, run_date: str) -> str:
        return f"gp-people-db-{run_date}-writer"

    def new_conn_param(self, run_date: str) -> str:
        """SSM SecureString name for the provisioned cluster's connection string."""
        return f"{CONN_PARAM_PREFIX}-{self.db_env}-{run_date}"

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
