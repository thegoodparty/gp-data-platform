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


def _env(key: str, default: str = "") -> str:
    """Read an env var with surrounding whitespace stripped.

    Values pasted into the Astro env-var UI commonly carry a trailing newline; unstripped it
    reaches AWS as a control character (e.g. `CreateDBCluster` rejects "control characters").
    Not used for the bastion private key, whose surrounding whitespace can be significant.
    """
    return os.environ.get(key, default).strip()


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
DEFAULT_DB_NAME = _PLACEHOLDER
DEFAULT_DB_USER = _PLACEHOLDER
DEFAULT_DB_PORT = 5432
DEFAULT_VPC_ID = _PLACEHOLDER
DEFAULT_DB_SUBNET_GROUP = _PLACEHOLDER
DEFAULT_SECURITY_GROUP_ID = _PLACEHOLDER
DEFAULT_KMS_KEY_ARN = _PLACEHOLDER
DEFAULT_AWS_ACCOUNT_ID = _PLACEHOLDER
# Durable rds-s3-import role (created once by platform IaC, not per run). provision
# references and attaches it via PassRole; it does not create it.
DEFAULT_S3_IMPORT_ROLE_ARN = _PLACEHOLDER


# Load-phase instance sizing. Prod serving is a provisioned instance (see the loader DAG
# spec, airflow/astro/docs/people_api_loader.md). We use THREE classes: provision/
# create_schema/copy run on the smaller `load_instance_class` (copy is I/O/WAL-bound, not
# CPU-bound), build_indexes scales the writer UP to `index_instance_class` (build_indexes
# is cleanly CPU-bound and scales with vCPU — see steps/build_indexes.py), and resize then
# flips the writer DOWN to `serve_instance_class` — the class the API actually reads from
# (prod default db.r6g.4xlarge). Override per-env with LOADER_LOAD_INSTANCE_CLASS /
# LOADER_INDEX_INSTANCE_CLASS / LOADER_SERVE_INSTANCE_CLASS (e.g. dev sets
# LOADER_SERVE_INSTANCE_CLASS=db.t4g.medium); keep _DEFAULT_BUILDERS in build_indexes.py in
# step with the index box's vCPU.
#
# scale_down's failure-cost guard is unrelated to serving: on a post-provision failure it
# flips the writer to Serverless v2 (db.serverless) purely to stop the provisioned-instance
# cost while keeping the cluster + loaded data around for resume/forensics — see
# `scale_down_min_acu` / `scale_down_max_acu` below and steps/scale_down.py.
DEFAULT_LOAD_INSTANCE_CLASS = "db.r8g.16xlarge"
DEFAULT_INDEX_INSTANCE_CLASS = "db.r8g.48xlarge"
DEFAULT_SERVE_INSTANCE_CLASS = "db.r6g.4xlarge"
DEFAULT_SCALE_DOWN_MIN_ACU = 0.5
DEFAULT_SCALE_DOWN_MAX_ACU = 128.0
DEFAULT_ENGINE_VERSION = "16.8"

# build_indexes's concurrent-builder count, overridable via LOADER_INDEX_PARALLELISM so an
# operator can back off from the loader default (e.g. against a bastion sshd MaxStartups limit)
# without a code change. Must match steps/build_indexes.py's `_DEFAULT_BUILDERS` (128, sized for
# the ~192-vCPU db.r8g.48xlarge index instance) — kept as a separate constant here (not imported
# from build_indexes) to avoid a config <-> steps import cycle; update both if the index box
# changes.
DEFAULT_INDEX_PARALLELISM = 128

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
    # Optional server-side statement_timeout (ms) applied to each Postgres session; 0 disables it.
    # A runaway query then fails loudly instead of running unbounded (see db.py).
    db_statement_timeout_ms: int

    # The Present/serving cluster inspected + validated against.
    prod_cluster_id: str
    # DB identity for the provisioned cluster's master user/db (matches the serving cluster so the
    # API connects the same way), plus the port used in its connection string.
    db_name: str
    db_user: str
    db_port: int

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
    index_instance_class: str
    serve_instance_class: str
    # Failure-cost guard only (steps/scale_down.py) — NOT the serving class, see comment above.
    scale_down_min_acu: float
    scale_down_max_acu: float
    # build_indexes's default concurrent-builder count (see DEFAULT_INDEX_PARALLELISM above);
    # the CLI's --parallelism flag still overrides this when passed explicitly.
    index_parallelism: int

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
        # `managedBy=dataplatform` matches the platform IAM convention: the RDS
        # create policies condition on `RequestTag/managedBy=dataplatform`, so
        # loader-created RDS resources must carry it to be authorized.
        tags = {
            "Project": os.environ.get("LOADER_TAG_PROJECT", "gp-api"),
            "Environment": os.environ.get("LOADER_TAG_ENVIRONMENT", "dev"),
            "managedBy": os.environ.get("LOADER_TAG_MANAGED_BY", "dataplatform"),
        }

        # Present-cluster connection comes from an SSM SecureString (connect_prod fetches it);
        # the param name is people-db-connection-string-{LOADER_ENV} unless fully overridden.
        env = _env("LOADER_ENV", DEFAULT_DB_ENV)
        db_conn_param = _env("LOADER_DB_CONN_PARAM", f"{CONN_PARAM_PREFIX}-{env}")
        # Loader output bucket is used from the first step (inspect) onward. It is real infra, not
        # committed to this public repo, so require it explicitly — a clear failure here beats an
        # opaque S3 AccessDenied/NoSuchBucket later (e.g. against a stale default bucket).
        s3_bucket = _env("LOADER_S3_BUCKET", DEFAULT_S3_BUCKET)
        if not s3_bucket:
            raise RuntimeError(
                "LOADER_S3_BUCKET is not set. The loader's S3 bucket is real infrastructure and is "
                "not committed to this public repo; set LOADER_S3_BUCKET (e.g. on the Astro "
                "deployment) to the loader bucket."
            )
        # Infra identifiers (provision-only; provision is a stub) come from LOADER_* env vars,
        # else empty placeholders — nothing infra-identifying is committed to this public repo.
        mart_schema = _env("LOADER_MART_CATALOG_SCHEMA", DEFAULT_MART_CATALOG_SCHEMA)
        mart_fqns = {pg: f"{mart_schema}.{model}" for pg, model in _MART_MODELS.items()}
        return cls(
            aws_region=_env("AWS_REGION", DEFAULT_AWS_REGION),
            aws_profile=os.environ.get("AWS_PROFILE"),
            # Cross-account RDS-admin role assumed for all AWS calls (the Astro worker's identity
            # lives in a different account). Unset for local runs whose ambient creds already have
            # access. ExternalId is required by the role's trust policy when present.
            assume_role_arn=(os.environ.get("AWS_PEOPLE_API_RDS_ROLE_ARN") or "").strip() or None,
            assume_role_external_id=(os.environ.get("AWS_PEOPLE_API_RDS_EXTERNAL_ID") or "").strip() or None,
            account_id=_env("LOADER_AWS_ACCOUNT_ID", DEFAULT_AWS_ACCOUNT_ID),
            s3_bucket=s3_bucket,
            databricks_warehouse_id=_env("LOADER_DATABRICKS_WAREHOUSE_ID", ""),
            db_env=env,
            db_conn_param=db_conn_param,
            db_statement_timeout_ms=int(os.environ.get("LOADER_DB_STATEMENT_TIMEOUT_MS", "0")),
            prod_cluster_id=_env("LOADER_PROD_CLUSTER_ID", DEFAULT_PROD_CLUSTER_ID),
            db_name=_env("LOADER_DB_NAME", DEFAULT_DB_NAME),
            db_user=_env("LOADER_DB_USER", DEFAULT_DB_USER),
            db_port=int(os.environ.get("LOADER_DB_PORT", DEFAULT_DB_PORT)),
            vpc_id=_env("LOADER_VPC_ID", DEFAULT_VPC_ID),
            db_subnet_group=_env("LOADER_DB_SUBNET_GROUP", DEFAULT_DB_SUBNET_GROUP),
            security_group_id=_env("LOADER_SECURITY_GROUP_ID", DEFAULT_SECURITY_GROUP_ID),
            kms_key_arn=_env("LOADER_KMS_KEY_ARN", DEFAULT_KMS_KEY_ARN),
            s3_import_role_arn=_env("LOADER_S3_IMPORT_ROLE_ARN", DEFAULT_S3_IMPORT_ROLE_ARN),
            bastion_host=_env("LOADER_BASTION_HOST", ""),
            bastion_port=int(os.environ.get("LOADER_BASTION_PORT", "22")),
            bastion_user=_env("LOADER_BASTION_USER", ""),
            bastion_private_key=os.environ.get("LOADER_BASTION_PRIVATE_KEY", ""),
            bastion_private_key_passphrase=os.environ.get("LOADER_BASTION_KEY_PASSPHRASE", ""),
            engine_version=_env("LOADER_ENGINE_VERSION", DEFAULT_ENGINE_VERSION),
            load_instance_class=_env("LOADER_LOAD_INSTANCE_CLASS", DEFAULT_LOAD_INSTANCE_CLASS),
            index_instance_class=_env("LOADER_INDEX_INSTANCE_CLASS", DEFAULT_INDEX_INSTANCE_CLASS),
            serve_instance_class=_env("LOADER_SERVE_INSTANCE_CLASS", DEFAULT_SERVE_INSTANCE_CLASS),
            scale_down_min_acu=float(os.environ.get("LOADER_SCALE_DOWN_MIN_ACU", DEFAULT_SCALE_DOWN_MIN_ACU)),
            scale_down_max_acu=float(os.environ.get("LOADER_SCALE_DOWN_MAX_ACU", DEFAULT_SCALE_DOWN_MAX_ACU)),
            index_parallelism=max(
                1, int(os.environ.get("LOADER_INDEX_PARALLELISM", DEFAULT_INDEX_PARALLELISM))
            ),
            mart_fqns=mart_fqns,
            _tags=tags,
        )

    @property
    def bastion_enabled(self) -> bool:
        return bool(self.bastion_host)

    def export_prefix(self, run_date: str) -> str:
        return f"voter_export_{run_date}"

    # Dated identifiers are env-scoped (gp-people-db-{date}-{env}[-role]). Dev and prod provision
    # into the same AWS account and `provision` is idempotent by name, so an un-scoped name would
    # let a same-date run in the other env adopt this env's cluster — cross-env contamination, not
    # just a clash. The conn param (below) has always been env-scoped; these mirror it.
    def new_cluster_id(self, run_date: str) -> str:
        return f"gp-people-db-{run_date}-{self.db_env}"

    def new_writer_instance_id(self, run_date: str) -> str:
        return f"{self.new_cluster_id(run_date)}-writer"

    def new_conn_param(self, run_date: str) -> str:
        """SSM SecureString name for the provisioned cluster's connection string."""
        return f"{CONN_PARAM_PREFIX}-{self.db_env}-{run_date}"

    def new_load_param_group(self, run_date: str) -> str:
        return f"{self.new_cluster_id(run_date)}-load"

    def new_serve_param_group(self, run_date: str) -> str:
        return f"{self.new_cluster_id(run_date)}-serve"


def require_run_date(run_date: str) -> str:
    """Validate the run_date stamp — 'YYYYMMDD'."""
    try:
        datetime.strptime(run_date, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"run_date must be YYYYMMDD (got {run_date!r})") from e
    return run_date
