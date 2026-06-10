"""Tests for analytics/lib/databricks_conn.py.

Covers the testable units of the profile-auth connection helper: scheme
stripping, the cursor description+rows to DataFrame mapping, the
connect-kwargs builder (including the M2M vs U2M auth branch), and the
cold-start retry. The actual network ``databricks.sql.connect`` call inside
``run_query`` is thin glue and is not unit tested.
"""

from types import SimpleNamespace

import databricks_conn as dc
import pytest


def test_server_hostname_strips_https_scheme():
    assert dc._server_hostname("https://dbc-3d8ca484.cloud.databricks.com") == (
        "dbc-3d8ca484.cloud.databricks.com"
    )


def test_server_hostname_strips_trailing_slash():
    assert dc._server_hostname("https://dbc-x.cloud.databricks.com/") == ("dbc-x.cloud.databricks.com")


def test_server_hostname_passes_through_bare_host():
    assert dc._server_hostname("dbc-x.cloud.databricks.com") == ("dbc-x.cloud.databricks.com")


def test_server_hostname_rejects_empty():
    with pytest.raises(ValueError):
        dc._server_hostname(None)


def test_to_dataframe_uses_description_columns():
    # sql-connector cursor.description is a list of 7-tuples; col name is [0].
    description = [
        ("user_id", None, None, None, None, None, None),
        ("n_events", None, None, None, None, None, None),
    ]
    rows = [("u1", 3), ("u2", 7)]
    df = dc._to_dataframe(description, rows)
    assert list(df.columns) == ["user_id", "n_events"]
    assert df.shape == (2, 2)
    assert df.loc[1, "user_id"] == "u2"
    assert df.loc[1, "n_events"] == 7


def test_to_dataframe_empty_rows_keeps_columns():
    description = [("a", None, None, None, None, None, None), ("b", None, None, None, None, None, None)]
    df = dc._to_dataframe(description, [])
    assert list(df.columns) == ["a", "b"]
    assert df.empty


def _fake_oauth(config):
    return f"m2m-provider-for-{config.client_id}"


def test_build_connect_kwargs_strips_host_and_passes_http_path():
    config = SimpleNamespace(
        host="https://dbc-x.cloud.databricks.com",
        client_id=None,
        client_secret=None,
        authenticate="u2m-header-factory",
    )
    kwargs = dc._build_connect_kwargs(config, "/sql/1.0/warehouses/abc", oauth_m2m=_fake_oauth)
    assert kwargs["server_hostname"] == "dbc-x.cloud.databricks.com"
    assert kwargs["http_path"] == "/sql/1.0/warehouses/abc"
    assert callable(kwargs["credentials_provider"])


def test_build_connect_kwargs_rejects_empty_http_path():
    config = SimpleNamespace(host="https://h", client_id=None, client_secret=None, authenticate="x")
    with pytest.raises(ValueError):
        dc._build_connect_kwargs(config, "", oauth_m2m=_fake_oauth)


def test_build_connect_kwargs_u2m_uses_config_authenticate():
    config = SimpleNamespace(
        host="https://h",
        client_id=None,
        client_secret=None,
        authenticate="u2m-header-factory",
    )
    kwargs = dc._build_connect_kwargs(config, "/p", oauth_m2m=_fake_oauth)
    # U2M: the provider yields the SDK Config.authenticate header factory.
    assert kwargs["credentials_provider"]() == "u2m-header-factory"


def test_build_connect_kwargs_m2m_uses_oauth_service_principal():
    config = SimpleNamespace(
        host="https://h",
        client_id="sp-client",
        client_secret="sp-secret",
        authenticate="should-not-be-used",
    )
    kwargs = dc._build_connect_kwargs(config, "/p", oauth_m2m=_fake_oauth)
    # M2M: client_id + client_secret present, so the provider yields the
    # service-principal OAuth provider, not config.authenticate.
    assert kwargs["credentials_provider"]() == "m2m-provider-for-sp-client"


def test_connect_with_retry_succeeds_after_transient_failures():
    attempts = {"n": 0}
    slept = []

    def flaky_connect(**_kwargs):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("warehouse cold start")
        return "connection"

    conn = dc._connect_with_retry(
        flaky_connect,
        {"server_hostname": "h"},
        max_retries=5,
        retry_delay=7,
        sleep_fn=slept.append,
    )
    assert conn == "connection"
    assert attempts["n"] == 3
    assert slept == [7, 7]  # slept between the two failed attempts, not after success


def test_connect_with_retry_raises_after_max_retries():
    def always_fails(**_kwargs):
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        dc._connect_with_retry(
            always_fails,
            {},
            max_retries=3,
            retry_delay=0,
            sleep_fn=lambda _d: None,
        )
