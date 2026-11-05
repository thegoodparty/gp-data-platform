from __future__ import annotations

import pytest

from hsmerge.config import DEFAULT_HUBSPOT_BASE_URL, Config, MissingConfigError

FULL_ENV = {
    "HUBSPOT_TOKEN": "pat-na1-test",
    "HUBSPOT_EXPECTED_PORTAL_ID": "51780263",
    "MERGE_OBJECT_TYPE": "contacts",
    "MERGE_PAIR_RELATION": "goodparty_data_catalog.dbt_staging.hubspot_contact_merge_queue",
    "MERGE_LOG_TABLE": "goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log",
    "MERGE_MAX_PAIRS": "50",
    "MERGE_MAX_RPS": "8",
    "DATABRICKS_HOST": "https://example.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
    "DATABRICKS_CLIENT_ID": "client-id",
    "DATABRICKS_CLIENT_SECRET": "client-secret",
}


def test_from_env_reads_every_setting() -> None:
    """A setting read from the wrong variable, or a name drifting from the DAG's."""
    config = Config.from_env(FULL_ENV)
    assert config.hubspot_token == "pat-na1-test"
    assert config.expected_portal_id == "51780263"
    assert config.object_type == "contacts"
    assert config.pair_relation == "goodparty_data_catalog.dbt_staging.hubspot_contact_merge_queue"
    assert config.log_table == "goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log"
    assert config.max_pairs == 50
    assert config.max_rps == 8.0
    assert config.databricks_host == "https://example.cloud.databricks.com"
    assert config.databricks_http_path == "/sql/1.0/warehouses/abc"
    assert config.databricks_client_id == "client-id"
    assert config.databricks_client_secret == "client-secret"


def test_base_url_defaults_to_hubspots_own_unless_overridden() -> None:
    assert Config.from_env(FULL_ENV).hubspot_base_url == DEFAULT_HUBSPOT_BASE_URL
    env = {**FULL_ENV, "HUBSPOT_BASE_URL": "https://hubspot.example.test"}
    assert Config.from_env(env).hubspot_base_url == "https://hubspot.example.test"


def test_repr_does_not_carry_the_token() -> None:
    """The token reaching a log line or traceback through the dataclass repr."""
    assert "pat-na1-test" not in repr(Config.from_env(FULL_ENV))


@pytest.mark.parametrize(
    "missing",
    [
        "HUBSPOT_TOKEN",
        "HUBSPOT_EXPECTED_PORTAL_ID",
        "MERGE_OBJECT_TYPE",
        "MERGE_PAIR_RELATION",
        "MERGE_LOG_TABLE",
        "MERGE_MAX_PAIRS",
        "MERGE_MAX_RPS",
        "DATABRICKS_HOST",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
    ],
)
def test_a_missing_setting_raises_and_names_the_variable(missing: str) -> None:
    """An unset variable read as a default the run then acts on."""
    env = {key: value for key, value in FULL_ENV.items() if key != missing}
    with pytest.raises(MissingConfigError, match=missing):
        Config.from_env(env)


@pytest.mark.parametrize("cap", ["0", "-3"])
def test_a_non_positive_cap_is_rejected(cap: str) -> None:
    """A cap that merges nothing on every run while reporting success."""
    with pytest.raises(MissingConfigError, match="MERGE_MAX_PAIRS"):
        Config.from_env({**FULL_ENV, "MERGE_MAX_PAIRS": cap})


@pytest.mark.parametrize("value", ["lots", "1.5"])
def test_an_unparseable_cap_is_rejected(value: str) -> None:
    with pytest.raises(MissingConfigError, match="MERGE_MAX_PAIRS"):
        Config.from_env({**FULL_ENV, "MERGE_MAX_PAIRS": value})


@pytest.mark.parametrize("rate", ["0", "-1", "fast", "nan"])
def test_a_non_positive_or_unparseable_rate_is_rejected(rate: str) -> None:
    """A rate that floods HubSpot or never lets a call through; nan passes every bound."""
    with pytest.raises(MissingConfigError, match="MERGE_MAX_RPS"):
        Config.from_env({**FULL_ENV, "MERGE_MAX_RPS": rate})


@pytest.mark.parametrize("object_type", ["companies", "Contacts", ""])
def test_an_object_type_other_than_contacts_is_rejected(object_type: str) -> None:
    """A run pointed at an object type the log schema was not built for."""
    env = {**FULL_ENV, "MERGE_OBJECT_TYPE": object_type}
    with pytest.raises(MissingConfigError, match="MERGE_OBJECT_TYPE"):
        Config.from_env(env)
