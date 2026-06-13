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


def test_from_env_placeholders_when_no_config_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    # Public repo: no infra identifiers are hardcoded. Absent the config secret + env
    # overrides, prod fields resolve to empty placeholders (no AWS call).
    for var in ("LOADER_PROD_CONFIG_SECRET_ID", "LOADER_PROD_CLUSTER_ID", "LOADER_AWS_ACCOUNT_ID"):
        monkeypatch.delenv(var, raising=False)
    cfg = LoaderConfig.from_env()
    assert cfg.prod_cluster_id == ""
    assert cfg.account_id == ""
    assert cfg.vpc_id == ""


def test_from_env_loads_infra_from_config_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_PROD_CONFIG_SECRET_ID", "some/loader-config")
    monkeypatch.setattr(
        "loader.people_api.config._fetch_prod_config_secret",
        lambda region, profile, secret_id: {
            "prod_cluster_id": "cluster-from-secret",
            "aws_account_id": "000000000000",
            "prod_db_user": "user-from-secret",
            "vpc_id": "vpc-from-secret",
        },
    )
    cfg = LoaderConfig.from_env()
    assert cfg.prod_cluster_id == "cluster-from-secret"
    assert cfg.account_id == "000000000000"
    assert cfg.prod_db_user == "user-from-secret"
    assert cfg.vpc_id == "vpc-from-secret"


def test_from_env_env_var_overrides_config_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_PROD_CONFIG_SECRET_ID", "some/loader-config")
    monkeypatch.setenv("LOADER_PROD_CLUSTER_ID", "from-env")
    monkeypatch.setattr(
        "loader.people_api.config._fetch_prod_config_secret",
        lambda *a: {"prod_cluster_id": "from-secret"},
    )
    assert LoaderConfig.from_env().prod_cluster_id == "from-env"
