"""Tests for the matcha ER gate and swap helpers."""

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

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
    count_sql,
    dated_name,
    distinct_count_sql,
    distinct_sources_sql,
    drop_stale_vintages,
    fqn,
    null_probe_sql,
    old_name,
    overlap_sql,
    run_gate,
    stale_vintages,
    swap_statements,
    swap_table,
    table_exists,
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


class TestSqlBuilders:
    """Statement text the gate issues. Assert shape, not formatting."""

    def test_count_sql(self):
        assert count_sql("`c`.`s`.`t`") == "SELECT count(*) FROM `c`.`s`.`t`"

    def test_distinct_count_sql(self):
        sql = distinct_count_sql("`c`.`s`.`t`", "unique_id")
        assert "count(DISTINCT `unique_id`)" in sql
        assert "FROM `c`.`s`.`t`" in sql

    def test_null_probe_counts_rows_with_any_null(self):
        sql = null_probe_sql("`c`.`s`.`t`", ("cluster_id", "unique_id"))
        assert "`cluster_id` IS NULL" in sql
        assert "`unique_id` IS NULL" in sql
        assert " OR " in sql

    def test_overlap_sql_joins_on_the_id(self):
        sql = overlap_sql("`c`.`s`.`d`", "`c`.`s`.`l`", "unique_id")
        assert "`c`.`s`.`d`" in sql
        assert "`c`.`s`.`l`" in sql
        assert "`unique_id`" in sql

    def test_distinct_sources_sql(self):
        sql = distinct_sources_sql("`c`.`s`.`t`", "source_name")
        assert "DISTINCT `source_name`" in sql

    def test_builders_reject_unsafe_columns(self):
        """Column names are constants today, but the guard is cheap."""
        with pytest.raises(ValueError, match="Unsafe Databricks identifier"):
            distinct_count_sql("`c`.`s`.`t`", "id`; drop table x; --")


class TestSwapStatements:
    """Unity Catalog has no multi-statement transaction, so the sequence and
    its idempotency are the whole safety argument."""

    def test_order_when_live_exists(self):
        stmts = swap_statements("cat", "er_source", "clustered_x", "clustered_x_20260825", True)
        assert len(stmts) == 3
        assert stmts[0].startswith("DROP TABLE IF EXISTS")
        assert "`clustered_x_old`" in stmts[0]
        assert stmts[1] == (
            "ALTER TABLE `cat`.`er_source`.`clustered_x` RENAME TO " "`cat`.`er_source`.`clustered_x_old`"
        )
        assert stmts[2] == (
            "ALTER TABLE `cat`.`er_source`.`clustered_x_20260825` RENAME TO "
            "`cat`.`er_source`.`clustered_x`"
        )

    def test_pre_drop_comes_first(self):
        """A crash between swap and cleanup leaves _old behind; the next run's
        rename-aside would collide with it without this pre-drop."""
        stmts = swap_statements("cat", "er_source", "clustered_x", "clustered_x_20260825", True)
        assert stmts.index(next(s for s in stmts if s.startswith("DROP"))) == 0

    def test_cold_start_skips_the_rename_aside(self):
        """No live table yet: promote the dated table straight into place."""
        stmts = swap_statements("cat", "er_source", "clustered_x", "clustered_x_20260825", False)
        assert len(stmts) == 2
        assert stmts[0].startswith("DROP TABLE IF EXISTS")
        assert stmts[1] == (
            "ALTER TABLE `cat`.`er_source`.`clustered_x_20260825` RENAME TO "
            "`cat`.`er_source`.`clustered_x`"
        )


class TestStaleVintages:
    """Retention over the dated tables left behind by past runs."""

    def test_selects_only_older_vintages(self):
        existing = [
            "clustered_x_20260701",
            "clustered_x_20260801",
            "clustered_x_20260825",
            "clustered_x",
        ]
        assert stale_vintages(existing, "clustered_x", "20260801") == ["clustered_x_20260701"]

    def test_cutoff_is_exclusive(self):
        """A vintage exactly at the cutoff is retained."""
        existing = ["clustered_x_20260801"]
        assert stale_vintages(existing, "clustered_x", "20260801") == []

    def test_ignores_the_live_and_old_tables(self):
        existing = ["clustered_x", "clustered_x_old"]
        assert stale_vintages(existing, "clustered_x", "20990101") == []

    def test_ignores_other_tables_with_a_shared_prefix(self):
        """`clustered_x` must not match `clustered_xyz_20260701`."""
        existing = ["clustered_xyz_20260701"]
        assert stale_vintages(existing, "clustered_x", "20990101") == []

    def test_ignores_non_date_suffixes(self):
        existing = ["clustered_x_backup", "clustered_x_2026"]
        assert stale_vintages(existing, "clustered_x", "20990101") == []


@pytest.fixture
def mock_connection():
    """Mock Databricks connection whose cursor returns queued scalars."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestTableExists:
    def test_true_when_information_schema_has_a_row(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchone.return_value = (1,)
        assert table_exists(conn, "cat", "er_source", "clustered_x") is True

    def test_false_when_absent(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchone.return_value = None
        assert table_exists(conn, "cat", "er_source", "clustered_x") is False

    def test_queries_information_schema(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchone.return_value = None
        table_exists(conn, "cat", "er_source", "clustered_x")
        sql = cursor.execute.call_args[0][0]
        assert "information_schema.tables" in sql


class TestRunGate:
    """The wrapper's job is ordering the queries and feeding the pure checks."""

    def _gate(self):
        return TableGate(
            cold_start_floor=100,
            min_prior_ratio=0.8,
            not_null_columns=("cluster_id",),
            id_column="unique_id",
            min_id_overlap=0.8,
            expected_sources=("ballotready",),
        )

    # NOTE on the mocked sequences: `run_gate` calls `table_exists` first, and
    # that consumes the FIRST fetchone off the same mock cursor. So the order is
    # always: exists-probe, loaded, distinct, nulls, prior, overlap — with the
    # source check reading fetchall in between nulls and prior.

    def test_passes_a_healthy_table(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchone.side_effect = [(1,), (1000,), (1000,), (0,), (1000,), (1000,)]
        cursor.fetchall.return_value = [("ballotready",)]
        run_gate(conn, "cat", "er_source", "clustered_x", "clustered_x_20260825", self._gate())

    def test_raises_on_a_shrunken_table(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchone.side_effect = [(1,), (100,), (100,), (0,), (1000,), (100,)]
        cursor.fetchall.return_value = [("ballotready",)]
        with pytest.raises(ValueError, match="refusing to swap"):
            run_gate(conn, "cat", "er_source", "clustered_x", "clustered_x_20260825", self._gate())

    def test_raises_on_a_missing_source(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchone.side_effect = [(1,), (1000,), (1000,), (0,), (1000,), (1000,)]
        cursor.fetchall.return_value = [("techspeed",)]
        with pytest.raises(ValueError, match="ballotready"):
            run_gate(conn, "cat", "er_source", "clustered_x", "clustered_x_20260825", self._gate())

    def test_cold_start_skips_the_live_queries(self, mock_connection):
        """No live table: prior count is 0 and overlap is never queried."""
        conn, cursor = mock_connection
        # exists-probe (None), loaded, distinct, nulls
        cursor.fetchone.side_effect = [None, (1000,), (1000,), (0,)]
        cursor.fetchall.return_value = [("ballotready",)]
        run_gate(conn, "cat", "er_source", "clustered_x", "clustered_x_20260825", self._gate())
        # Overlap is never queried, so a fifth fetchone would raise StopIteration.


class TestSwapTable:
    def test_executes_the_swap_sequence(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchone.return_value = (1,)
        swap_table(conn, "cat", "er_source", "clustered_x", "clustered_x_20260825")
        executed = [c[0][0] for c in cursor.execute.call_args_list]
        renames = [s for s in executed if s.startswith("ALTER TABLE")]
        assert len(renames) == 2
        assert any(s.startswith("DROP TABLE IF EXISTS") for s in executed)


class TestDropStaleVintages:
    def test_drops_only_stale_ones(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchall.return_value = [
            ("clustered_x_20260701",),
            ("clustered_x_20260825",),
            ("clustered_x",),
        ]
        dropped = drop_stale_vintages(conn, "cat", "er_source", "clustered_x", "20260801")
        assert dropped == ["clustered_x_20260701"]
        drops = [c[0][0] for c in cursor.execute.call_args_list if "DROP TABLE" in c[0][0]]
        assert len(drops) == 1
        assert "`clustered_x_20260701`" in drops[0]

    def test_no_stale_vintages_issues_no_drops(self, mock_connection):
        conn, cursor = mock_connection
        cursor.fetchall.return_value = [("clustered_x_20260825",)]
        assert drop_stale_vintages(conn, "cat", "er_source", "clustered_x", "20260801") == []
        assert not [c for c in cursor.execute.call_args_list if "DROP TABLE" in c[0][0]]
