"""Guard test for the forbidden-env-var security contract.

CLAUDE.md "Never" rule: the loader must never read `VOTER_DB_MASTER_PASSWORD`
from the environment. `LoaderConfig.from_env()` hard-fails if it is set. This
test pins that behavior so a future refactor cannot silently drop the guard.
"""

from __future__ import annotations

import pytest

from loader.people_api.config import LoaderConfig


def test_from_env_rejects_forbidden_password_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOTER_DB_MASTER_PASSWORD", "secret")
    with pytest.raises(RuntimeError, match="VOTER_DB_MASTER_PASSWORD"):
        LoaderConfig.from_env()


def test_from_env_placeholders_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    # Public repo: no infra identifiers are hardcoded. Absent env overrides, the
    # provision-only infra fields resolve to empty placeholders (no AWS call).
    for var in ("LOADER_PROD_CLUSTER_ID", "LOADER_AWS_ACCOUNT_ID", "LOADER_VPC_ID"):
        monkeypatch.delenv(var, raising=False)
    cfg = LoaderConfig.from_env()
    assert cfg.prod_cluster_id == ""
    assert cfg.account_id == ""
    assert cfg.vpc_id == ""


def test_from_env_requires_s3_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    # Public repo: the loader bucket is not hardcoded, so from_env() must fail fast when
    # LOADER_S3_BUCKET is absent rather than falling back to a stale/wrong default bucket.
    monkeypatch.delenv("LOADER_S3_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="LOADER_S3_BUCKET"):
        LoaderConfig.from_env()


def test_from_env_db_conn_param_defaults_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_DB_CONN_PARAM", raising=False)
    monkeypatch.setenv("LOADER_ENV", "qa")
    assert LoaderConfig.from_env().db_conn_param == "people-db-connection-string-qa"


def test_from_env_db_conn_param_defaults_to_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("LOADER_DB_CONN_PARAM", "LOADER_ENV"):
        monkeypatch.delenv(var, raising=False)
    assert LoaderConfig.from_env().db_conn_param == "people-db-connection-string-dev"


def test_from_env_db_conn_param_full_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_ENV", "prod")
    monkeypatch.setenv("LOADER_DB_CONN_PARAM", "custom/param/name")
    assert LoaderConfig.from_env().db_conn_param == "custom/param/name"


def test_provisioned_identifiers_are_env_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    # Dev and prod provision into the SAME AWS account, and `provision` is idempotent by name,
    # so a same-date run in the other env would adopt this env's cluster (cross-env contamination)
    # unless the dated identifiers carry the env. The conn param is already env-scoped; these four
    # must match. Convention: gp-people-db-{date}-{env}[-role].
    monkeypatch.setenv("LOADER_S3_BUCKET", "gp-people-loader-us-west-2")
    monkeypatch.setenv("LOADER_ENV", "dev")
    cfg = LoaderConfig.from_env()
    assert cfg.new_cluster_id("20260707") == "gp-people-db-20260707-dev"
    assert cfg.new_writer_instance_id("20260707") == "gp-people-db-20260707-dev-writer"
    assert cfg.new_load_param_group("20260707") == "gp-people-db-20260707-dev-load"
    assert cfg.new_serve_param_group("20260707") == "gp-people-db-20260707-dev-serve"
    # prod uses the same builders with a different env, so the names cannot collide.
    monkeypatch.setenv("LOADER_ENV", "prod")
    assert LoaderConfig.from_env().new_cluster_id("20260707") == "gp-people-db-20260707-prod"


def test_people_api_mart_fqns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    for k in list(os.environ):
        if k.startswith("LOADER_MART_"):
            monkeypatch.delenv(k, raising=False)
    cfg = LoaderConfig.from_env()
    assert cfg.mart_fqns["Voter"] == "goodparty_data_catalog.dbt.m_people_api__voter"
    assert set(cfg.mart_fqns) == {"Voter", "District", "DistrictStats", "DistrictVoter"}


def test_from_env_strips_surrounding_whitespace(monkeypatch: pytest.MonkeyPatch) -> None:
    # A value pasted into the Astro env UI often carries a trailing newline; unstripped it reaches
    # AWS as a control character (CreateDBCluster rejects them). from_env must strip it.
    monkeypatch.setenv("LOADER_S3_BUCKET", " gp-people-loader-us-west-2\n")
    monkeypatch.setenv("LOADER_DB_SUBNET_GROUP", "people-db-subnets\n")
    monkeypatch.setenv("LOADER_KMS_KEY_ARN", "  arn:aws:kms:us-west-2:333022194791:key/abc \t")
    cfg = LoaderConfig.from_env()
    assert cfg.s3_bucket == "gp-people-loader-us-west-2"
    assert cfg.db_subnet_group == "people-db-subnets"
    assert cfg.kms_key_arn == "arn:aws:kms:us-west-2:333022194791:key/abc"


def test_statement_timeout_defaults_off_and_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_DB_STATEMENT_TIMEOUT_MS", raising=False)
    assert LoaderConfig.from_env().db_statement_timeout_ms == 0
    monkeypatch.setenv("LOADER_DB_STATEMENT_TIMEOUT_MS", "1800000")
    assert LoaderConfig.from_env().db_statement_timeout_ms == 1800000


def test_databricks_warehouse_id_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_DATABRICKS_WAREHOUSE_ID", "wh-123")
    assert LoaderConfig.from_env().databricks_warehouse_id == "wh-123"


def test_databricks_warehouse_id_defaults_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOADER_DATABRICKS_WAREHOUSE_ID", raising=False)
    assert LoaderConfig.from_env().databricks_warehouse_id == ""


def test_bastion_defaults_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "LOADER_BASTION_HOST",
        "LOADER_BASTION_USER",
        "LOADER_BASTION_PRIVATE_KEY",
        "LOADER_BASTION_KEY_PASSPHRASE",
        "LOADER_BASTION_PORT",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = LoaderConfig.from_env()
    assert cfg.bastion_host == ""
    assert cfg.bastion_port == 22
    assert cfg.bastion_private_key_passphrase == ""
    assert cfg.bastion_enabled is False


def test_bastion_enabled_when_host_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_BASTION_HOST", "bastion.example.com")
    monkeypatch.setenv("LOADER_BASTION_USER", "ec2-user")
    monkeypatch.setenv("LOADER_BASTION_PRIVATE_KEY", "PEM")
    cfg = LoaderConfig.from_env()
    assert cfg.bastion_host == "bastion.example.com"
    assert cfg.bastion_user == "ec2-user"
    assert cfg.bastion_private_key == "PEM"
    assert cfg.bastion_enabled is True
