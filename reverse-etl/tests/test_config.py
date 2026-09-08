from __future__ import annotations

import pytest

from retl.config import MissingFlowConfigError, load_flow_config, load_log_table

FULL_ENV = {
    "RETL_FLOW_HUBSPOT_LEADS_SOURCE_RELATION": "goodparty_data_catalog.mart_sales_reverse_etl.contact_desired_state",
    "RETL_FLOW_HUBSPOT_LEADS_KEY_COLUMN": "gp_person_id",
    "RETL_FLOW_HUBSPOT_LEADS_EXCLUDED_COLUMNS": "added_to_mart_at, candidate_id_source",
    "RETL_FLOW_HUBSPOT_LEADS_CAP": "80000",
}


def test_load_flow_config_reads_the_namespaced_environment_variables() -> None:
    """Catches: a flow's relation name being hardcoded instead of coming from config."""
    flow = load_flow_config("hubspot_leads", FULL_ENV)
    assert flow.source_relation == "goodparty_data_catalog.mart_sales_reverse_etl.contact_desired_state"
    assert flow.key_column == "gp_person_id"
    assert flow.cap == 80000


def test_load_flow_config_parses_excluded_columns_as_a_set() -> None:
    """Catches: excluded columns staying a single comma-joined string instead of a lookup set."""
    flow = load_flow_config("hubspot_leads", FULL_ENV)
    assert flow.excluded_columns == frozenset({"added_to_mart_at", "candidate_id_source"})


def test_load_flow_config_defaults_excluded_columns_to_empty() -> None:
    """Catches: a flow with no exclusions failing to load instead of defaulting to none."""
    env = {k: v for k, v in FULL_ENV.items() if not k.endswith("EXCLUDED_COLUMNS")}
    flow = load_flow_config("hubspot_leads", env)
    assert flow.excluded_columns == frozenset()


@pytest.mark.parametrize("missing_key", ["SOURCE_RELATION", "KEY_COLUMN", "CAP"])
def test_load_flow_config_raises_on_a_missing_required_variable(missing_key: str) -> None:
    """Catches: a silently-empty relation/key/cap being treated as valid config instead of failing fast."""
    env_var = f"RETL_FLOW_HUBSPOT_LEADS_{missing_key}"
    env = {k: v for k, v in FULL_ENV.items() if k != env_var}
    with pytest.raises(MissingFlowConfigError):
        load_flow_config("hubspot_leads", env)


@pytest.mark.parametrize("bad_cap", ["0", "-3", "not_a_number"])
def test_load_flow_config_rejects_a_nonpositive_or_unparseable_cap(bad_cap: str) -> None:
    """Catches: cap=0 (or negative/garbage) passing validation, which would make the
    overflow guard fire on every non-empty diff and permanently block the flow."""
    env = {**FULL_ENV, "RETL_FLOW_HUBSPOT_LEADS_CAP": bad_cap}
    with pytest.raises(MissingFlowConfigError):
        load_flow_config("hubspot_leads", env)


def test_load_flow_config_is_independent_per_flow_name() -> None:
    """Catches: two flows accidentally sharing one config namespace instead of being isolated."""
    env = {
        **FULL_ENV,
        "RETL_FLOW_OTHER_FLOW_SOURCE_RELATION": "goodparty_data_catalog.other.model",
        "RETL_FLOW_OTHER_FLOW_KEY_COLUMN": "some_key",
        "RETL_FLOW_OTHER_FLOW_CAP": "10",
    }
    other = load_flow_config("other_flow", env)
    assert other.source_relation == "goodparty_data_catalog.other.model"
    assert other.cap == 10


def test_load_log_table_requires_it_to_be_set() -> None:
    """Catches: a run silently reading/writing an unset (empty-string) log table."""
    with pytest.raises(ValueError, match="RETL_LOG_TABLE"):
        load_log_table({})
