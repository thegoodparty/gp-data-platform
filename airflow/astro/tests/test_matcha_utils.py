"""Tests for the matcha ER gate and swap helpers."""

from dataclasses import FrozenInstanceError

import pytest
from include.custom_functions.matcha_utils import (
    ENTITIES,
    TableGate,
    _ident,
    check_counts,
    check_distinct_ids,
    check_id_overlap,
    check_nulls,
    check_sources,
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


class TestCheckCounts:
    """Row-count gate: ratio floor against live, cold-start floor without."""

    def test_passes_above_ratio(self):
        gate = TableGate(cold_start_floor=100, min_prior_ratio=0.8, not_null_columns=())
        check_counts(900, 1000, gate, "clustered_x")

    def test_passes_when_grown(self):
        """The prematch universe is cumulative; growth is normal."""
        gate = TableGate(cold_start_floor=100, min_prior_ratio=0.8, not_null_columns=())
        check_counts(5000, 1000, gate, "clustered_x")

    def test_fails_below_ratio(self):
        gate = TableGate(cold_start_floor=100, min_prior_ratio=0.8, not_null_columns=())
        with pytest.raises(ValueError, match="refusing to swap"):
            check_counts(700, 1000, gate, "clustered_x")

    def test_ratio_boundary_is_inclusive(self):
        """Exactly at the floor passes; the message should mean 'below'."""
        gate = TableGate(cold_start_floor=100, min_prior_ratio=0.8, not_null_columns=())
        check_counts(800, 1000, gate, "clustered_x")

    def test_cold_start_uses_floor(self):
        """No prior live table: fall back to the absolute floor."""
        gate = TableGate(cold_start_floor=100, min_prior_ratio=0.8, not_null_columns=())
        check_counts(150, 0, gate, "clustered_x")

    def test_cold_start_below_floor_fails(self):
        gate = TableGate(cold_start_floor=100, min_prior_ratio=0.8, not_null_columns=())
        with pytest.raises(ValueError, match="implausibly small"):
            check_counts(99, 0, gate, "clustered_x")

    def test_message_names_the_table(self):
        gate = TableGate(cold_start_floor=100, min_prior_ratio=0.8, not_null_columns=())
        with pytest.raises(ValueError, match="clustered_candidacy_stages"):
            check_counts(1, 1000, gate, "clustered_candidacy_stages")


class TestCheckDistinctIds:
    """The cluster tables promise a unique unique_id."""

    def test_passes_when_all_distinct(self):
        gate = TableGate(cold_start_floor=1, min_prior_ratio=0.5, not_null_columns=(), id_column="unique_id")
        check_distinct_ids(1000, 1000, gate, "clustered_x")

    def test_fails_on_duplicates(self):
        gate = TableGate(cold_start_floor=1, min_prior_ratio=0.5, not_null_columns=(), id_column="unique_id")
        with pytest.raises(ValueError, match="duplicate"):
            check_distinct_ids(1000, 990, gate, "clustered_x")

    def test_skipped_without_id_column(self):
        """Pairwise rows are pairs, so there is no single identity column."""
        gate = TableGate(cold_start_floor=1, min_prior_ratio=0.5, not_null_columns=())
        check_distinct_ids(1000, 1, gate, "pairwise_x")


class TestCheckIdOverlap:
    """Too few shared ids means a wholesale re-key, not a refresh."""

    def test_passes_above_floor(self):
        gate = TableGate(
            cold_start_floor=1,
            min_prior_ratio=0.5,
            not_null_columns=(),
            id_column="unique_id",
            min_id_overlap=0.8,
        )
        check_id_overlap(900, 1000, gate, "clustered_x")

    def test_fails_below_floor(self):
        gate = TableGate(
            cold_start_floor=1,
            min_prior_ratio=0.5,
            not_null_columns=(),
            id_column="unique_id",
            min_id_overlap=0.8,
        )
        with pytest.raises(ValueError, match="re-key"):
            check_id_overlap(500, 1000, gate, "clustered_x")

    def test_skipped_without_floor(self):
        gate = TableGate(cold_start_floor=1, min_prior_ratio=0.5, not_null_columns=())
        check_id_overlap(0, 1000, gate, "pairwise_x")

    def test_skipped_on_cold_start(self):
        """No prior rows means nothing to overlap with."""
        gate = TableGate(
            cold_start_floor=1,
            min_prior_ratio=0.5,
            not_null_columns=(),
            id_column="unique_id",
            min_id_overlap=0.8,
        )
        check_id_overlap(0, 0, gate, "clustered_x")


class TestCheckNulls:
    def test_passes_with_no_nulls(self):
        gate = TableGate(cold_start_floor=1, min_prior_ratio=0.5, not_null_columns=("cluster_id",))
        check_nulls(0, gate, "clustered_x")

    def test_fails_with_nulls(self):
        gate = TableGate(cold_start_floor=1, min_prior_ratio=0.5, not_null_columns=("cluster_id",))
        with pytest.raises(ValueError, match="NULL"):
            check_nulls(3, gate, "clustered_x")


class TestCheckSources:
    """A source silently dropping out of prematch is the failure this catches."""

    def test_passes_when_all_present(self):
        gate = TableGate(
            cold_start_floor=1,
            min_prior_ratio=0.5,
            not_null_columns=(),
            expected_sources=("ballotready", "ddhq"),
        )
        check_sources({"ballotready", "ddhq"}, gate, "clustered_x")

    def test_passes_with_extra_source(self):
        """A new source appearing is not a reason to block the swap."""
        gate = TableGate(
            cold_start_floor=1,
            min_prior_ratio=0.5,
            not_null_columns=(),
            expected_sources=("ballotready", "ddhq"),
        )
        check_sources({"ballotready", "ddhq", "newthing"}, gate, "clustered_x")

    def test_fails_on_missing_source(self):
        gate = TableGate(
            cold_start_floor=1,
            min_prior_ratio=0.5,
            not_null_columns=(),
            expected_sources=("ballotready", "ddhq"),
        )
        with pytest.raises(ValueError, match="ddhq"):
            check_sources({"ballotready"}, gate, "clustered_x")

    def test_skipped_when_none_expected(self):
        gate = TableGate(cold_start_floor=1, min_prior_ratio=0.5, not_null_columns=())
        check_sources(set(), gate, "pairwise_x")
