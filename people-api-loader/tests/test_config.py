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
