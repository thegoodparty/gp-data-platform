"""Auth resolution for the shared Databricks client: M2M service-principal
credentials take priority over the PAT when both are configured, the PAT
still works alone, and construction refuses only when neither is present."""

import pytest

from shared.databricks_client import DatabricksClient


def test_m2m_env_selects_credentials_provider(monkeypatch):
    """Failure this catches: the client falling back to the PAT kwarg (or
    passing both) when M2M env vars are set, instead of preferring OAuth
    M2M -- the path the unattended daily loop ships with."""
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
    monkeypatch.delenv("DATABRICKS_API_KEY", raising=False)
    recorded = {}
    monkeypatch.setattr(
        "shared.databricks_client.sql.connect",
        lambda **kw: recorded.update(kw) or object(),
    )

    client = DatabricksClient(server_hostname="h", http_path="p")
    client.connect()

    assert "credentials_provider" in recorded
    assert "access_token" not in recorded


def test_pat_fallback_without_m2m(monkeypatch):
    """Failure this catches: requiring the new M2M env vars now that they
    exist as an option, breaking every caller still on a personal token."""
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    recorded = {}
    monkeypatch.setattr(
        "shared.databricks_client.sql.connect",
        lambda **kw: recorded.update(kw) or object(),
    )

    client = DatabricksClient(server_hostname="h", http_path="p", access_token="tok")
    client.connect()

    assert recorded["access_token"] == "tok"
    assert "credentials_provider" not in recorded


def test_no_credentials_at_all_raises(monkeypatch):
    """Failure this catches: an error that names only one of the two auth
    paths, leaving an operator to guess which env vars to set."""
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("DATABRICKS_API_KEY", raising=False)

    with pytest.raises(ValueError, match="DATABRICKS_API_KEY.*DATABRICKS_CLIENT_ID"):
        DatabricksClient(server_hostname="h", http_path="p")
