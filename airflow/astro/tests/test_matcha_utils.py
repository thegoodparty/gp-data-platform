"""Tests for the matcha ER gate and swap helpers."""

from dataclasses import FrozenInstanceError

import pytest
from include.custom_functions.matcha_utils import (
    ENTITIES,
    _ident,
    dated_name,
    fqn,
    old_name,
)


class TestEntitySpecs:
    """The declarative entity registry the DAG builds its task groups from."""

    def test_three_entities(self):
        """One spec per matcha --entity-type."""
        assert {e.entity_type for e in ENTITIES} == {
            "candidacy_stage",
            "elected_official",
            "election_stage",
        }

    def test_table_names_derive_from_stem(self):
        """Cluster and pairwise names share one plural stem."""
        candidacy = next(e for e in ENTITIES if e.entity_type == "candidacy_stage")
        assert candidacy.cluster_table == "clustered_candidacy_stages"
        assert candidacy.pairwise_table == "pairwise_candidacy_stages"

    def test_prematch_models_match_dbt(self):
        """Input model names must match the dbt models exactly."""
        assert {e.prematch_model for e in ENTITIES} == {
            "int__er_prematch_candidacy_stages",
            "int__er_prematch_elected_officials",
            "int__er_prematch_election_stages",
        }

    def test_cluster_gates_are_strict(self):
        """Cluster tables carry the identity and source-coverage checks."""
        for entity in ENTITIES:
            gate = entity.cluster_gate
            assert gate.min_prior_ratio == 0.8
            assert gate.id_column == "unique_id"
            assert gate.min_id_overlap == 0.8
            assert gate.not_null_columns == ("cluster_id", "unique_id")
            assert gate.expected_sources

    def test_pairwise_gates_are_loose(self):
        """Pairwise volume swings with model tuning, so no identity checks."""
        for entity in ENTITIES:
            gate = entity.pairwise_gate
            assert gate.min_prior_ratio == 0.5
            assert gate.id_column is None
            assert gate.min_id_overlap is None
            assert gate.expected_sources == ()
            assert gate.not_null_columns == ("unique_id_l", "unique_id_r")

    def test_expected_sources_per_entity(self):
        """Taken from the accepted_values tests on the staging models."""
        by_type = {e.entity_type: e.cluster_gate.expected_sources for e in ENTITIES}
        assert set(by_type["candidacy_stage"]) == {
            "ballotready",
            "techspeed",
            "ddhq",
            "gp_api",
        }
        assert set(by_type["elected_official"]) == {
            "ballotready_techspeed",
            "gp_api",
            "ddhq",
        }
        assert set(by_type["election_stage"]) == {"ballotready", "ddhq", "techspeed"}

    def test_specs_are_frozen(self):
        """Specs are module constants; mutation would leak across tasks."""
        with pytest.raises(FrozenInstanceError):
            ENTITIES[0].entity_type = "nope"


class TestIdentifierQuoting:
    """Identifiers reach SQL by string interpolation, so quoting is the guard."""

    def test_ident_backticks(self):
        assert _ident("er_source") == "`er_source`"

    def test_ident_rejects_backtick(self):
        """A backtick in a name could close the quote and inject SQL."""
        with pytest.raises(ValueError, match="Unsafe Databricks identifier"):
            _ident("er_source`; drop table x; --")

    def test_ident_rejects_empty(self):
        with pytest.raises(ValueError, match="Unsafe Databricks identifier"):
            _ident("")

    def test_fqn_quotes_all_three_parts(self):
        assert fqn("cat", "er_source", "clustered_x") == "`cat`.`er_source`.`clustered_x`"


class TestNaming:
    """Dated vintages and the renamed-aside table."""

    def test_dated_name(self):
        assert dated_name("clustered_candidacy_stages", "20260825") == ("clustered_candidacy_stages_20260825")

    def test_old_name(self):
        assert old_name("clustered_candidacy_stages") == "clustered_candidacy_stages_old"
