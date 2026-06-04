# tests/test_entity_config.py
"""Tests for EntityConfig and the config registry."""

import pytest

from scripts.entity_config import EntityConfig, get_config


def _minimal_config(**overrides) -> EntityConfig:
    """Build an EntityConfig with minimal required fields, plus any overrides."""
    defaults = dict(
        entity_type="test",
        display_name="Test",
        default_input_table="catalog.schema.table",
        comparisons=[],
        blocking_rules_for_prediction=[],
        additional_columns_to_retain=[],
        em_training_blocks=[],
    )
    return EntityConfig(**{**defaults, **overrides})


def test_entity_config_is_frozen():
    """EntityConfig instances are immutable."""
    config = _minimal_config()
    with pytest.raises(AttributeError):
        config.entity_type = "changed"


def test_entity_config_defaults():
    """Default values are set correctly."""
    config = _minimal_config()
    assert config.predict_threshold == 0.01
    assert config.cluster_threshold == 0.95
    assert config.post_prediction_filters == []
    assert config.date_columns == []
    assert config.clustered_output_name == "clustered_records.csv"


def test_candidacy_config_comparisons():
    """Candidacy config has exactly 10 comparisons (regression guard)."""
    from scripts.configs.candidacy import CANDIDACY_CONFIG

    assert len(CANDIDACY_CONFIG.comparisons) == 10
    assert CANDIDACY_CONFIG.entity_type == "candidacy_stage"
    assert CANDIDACY_CONFIG.clustered_output_name == "clustered_candidacies.csv"


def test_candidacy_config_has_election_date():
    """Candidacy config uses election_date for training and date parsing."""
    from scripts.configs.candidacy import CANDIDACY_CONFIG

    assert "election_date" in CANDIDACY_CONFIG.date_columns
    assert any("election_date" in cols for cols in CANDIDACY_CONFIG.em_training_blocks)


def test_candidacy_config_blocking_rules():
    """Candidacy config has 7 blocking rules including br_race_id."""
    from scripts.configs.candidacy import CANDIDACY_CONFIG

    assert len(CANDIDACY_CONFIG.blocking_rules_for_prediction) == 7


def test_elected_official_config_comparisons():
    """Elected official config has 11 comparisons (no election_date)."""
    from scripts.configs.elected_official import ELECTED_OFFICIAL_CONFIG

    assert len(ELECTED_OFFICIAL_CONFIG.comparisons) == 11
    assert ELECTED_OFFICIAL_CONFIG.entity_type == "elected_official"
    assert (
        ELECTED_OFFICIAL_CONFIG.clustered_output_name
        == "clustered_elected_officials.csv"
    )


def test_elected_official_config_no_election_date():
    """Elected official config has no election_date in date_columns or EM blocks."""
    from scripts.configs.elected_official import ELECTED_OFFICIAL_CONFIG

    assert "election_date" not in ELECTED_OFFICIAL_CONFIG.date_columns
    for cols in ELECTED_OFFICIAL_CONFIG.em_training_blocks:
        assert "election_date" not in cols


def test_elected_official_config_no_race_id_blocking():
    """Elected official blocking rules don't use br_race_id."""
    from scripts.configs.elected_official import ELECTED_OFFICIAL_CONFIG

    for rule in ELECTED_OFFICIAL_CONFIG.blocking_rules_for_prediction:
        rule_str = str(rule)
        assert "br_race_id" not in rule_str


def test_get_config_registry():
    """get_config returns the correct config for each entity type."""
    c = get_config("candidacy_stage")
    assert c.entity_type == "candidacy_stage"

    eo = get_config("elected_official")
    assert eo.entity_type == "elected_official"


def test_get_config_unknown():
    """get_config raises ValueError for unknown entity types."""
    with pytest.raises(ValueError, match="Unknown entity type"):
        get_config("nonexistent")
