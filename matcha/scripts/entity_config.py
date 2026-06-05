# scripts/entity_config.py
"""Entity resolution configuration: dataclass + registry."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EntityConfig:
    """Complete Splink configuration for one entity type."""

    # Identity
    entity_type: str
    display_name: str
    default_input_table: str

    # Splink settings
    comparisons: list[Any]
    blocking_rules_for_prediction: list[Any]
    additional_columns_to_retain: list[str]

    # EM training — each tuple is a set of columns to block_on
    em_training_blocks: list[tuple[str, ...]]

    # Thresholds
    predict_threshold: float = 0.01
    cluster_threshold: float = 0.95

    # Post-prediction filter SQL — DuckDB WHERE clauses, AND-ed together
    post_prediction_filters: list[str] = field(default_factory=list)

    # Data preparation — columns requiring date parsing
    date_columns: list[str] = field(default_factory=list)

    # Output
    clustered_output_name: str = "clustered_records.csv"

    # Audit
    audit_display_columns: list[str] = field(default_factory=list)
    audit_gamma_columns: list[str] = field(default_factory=list)
    false_negative_group_cols: list[str] = field(default_factory=list)


ENTITY_TYPES: list[str] = ["candidacy_stage", "elected_official", "election_stage"]


def get_config(entity_type: str) -> EntityConfig:
    """Look up an EntityConfig by name. Raises ValueError for unknown types."""
    # Import lazily to avoid circular imports during config construction
    if entity_type == "candidacy_stage":
        from scripts.configs.candidacy import CANDIDACY_CONFIG

        return CANDIDACY_CONFIG
    if entity_type == "elected_official":
        from scripts.configs.elected_official import ELECTED_OFFICIAL_CONFIG

        return ELECTED_OFFICIAL_CONFIG
    if entity_type == "election_stage":
        from scripts.configs.election_stage import ELECTION_STAGE_CONFIG

        return ELECTION_STAGE_CONFIG
    raise ValueError(f"Unknown entity type '{entity_type}'. Available: {ENTITY_TYPES}")
