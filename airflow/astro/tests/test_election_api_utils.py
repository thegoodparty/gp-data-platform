"""Tests for election-api sync utilities (bulk_insert_from_databricks)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from include.custom_functions import election_api_utils
from include.custom_functions.election_api_utils import bulk_insert_from_databricks


def _spec():
    return SimpleNamespace(staging_schema="staging", new_table="ZipToPosition_new")


def _gen(batches):
    """A real generator (supports .close(), which the loader calls)."""

    def g():
        yield from batches

    return g()


class TestBulkInsertPartitioning:
    """Partitioned reads keep peak memory bounded by one partition, not the
    full mart, while preserving single-commit retry-safety."""

    def test_no_partition_runs_single_query(self):
        """Without partition_column, the source query runs once (unchanged)."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        reads = []

        def fake_read(query, batch_size=5000):
            reads.append(query)
            return (["a"], _gen([[(1,), (2,)]]))

        with (
            patch.object(election_api_utils, "read_databricks_table", side_effect=fake_read),
            patch.object(election_api_utils.psycopg2.extras, "execute_values") as ev,
        ):
            total = bulk_insert_from_databricks(conn, _spec(), "SELECT a FROM t", ["a"])

        assert total == 2
        assert reads == ["SELECT a FROM t"]
        assert ev.call_count == 1
        conn.commit.assert_called_once()

    def test_partition_chunks_by_distinct_value(self):
        """With partition_column, one DISTINCT query + one query per value, and
        every partition's rows are inserted under a single end-of-load commit."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        reads = []

        def fake_read(query, batch_size=5000):
            reads.append(query)
            if "DISTINCT" in query:
                return (["_pv"], _gen([[("CA",), ("OR",)]]))
            if "'CA'" in query:
                return (["a"], _gen([[(1,)]]))
            if "'OR'" in query:
                return (["a"], _gen([[(2,), (3,)]]))
            raise AssertionError(f"unexpected query: {query}")

        with (
            patch.object(election_api_utils, "read_databricks_table", side_effect=fake_read),
            patch.object(election_api_utils.psycopg2.extras, "execute_values"),
        ):
            total = bulk_insert_from_databricks(
                conn,
                _spec(),
                "SELECT a, state FROM t",
                ["a"],
                partition_column="state",
            )

        assert total == 3
        assert sum("DISTINCT" in q for q in reads) == 1
        assert any("'CA'" in q for q in reads)
        assert any("'OR'" in q for q in reads)
        # Single commit after all partitions: a mid-load failure rolls back the
        # whole staging load so a retry starts clean.
        conn.commit.assert_called_once()

    def test_partition_handles_null_value(self):
        """A NULL partition value is loaded via an IS NULL predicate (no row loss)."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        reads = []

        def fake_read(query, batch_size=5000):
            reads.append(query)
            if "DISTINCT" in query:
                return (["_pv"], _gen([[(None,)]]))
            return (["a"], _gen([[(9,)]]))

        with (
            patch.object(election_api_utils, "read_databricks_table", side_effect=fake_read),
            patch.object(election_api_utils.psycopg2.extras, "execute_values"),
        ):
            total = bulk_insert_from_databricks(
                conn,
                _spec(),
                "SELECT a, state FROM t",
                ["a"],
                partition_column="state",
            )

        assert total == 1
        assert any("IS NULL" in q for q in reads)

    def test_load_failure_rolls_back(self):
        """A mid-load error rolls the staging transaction back (so a retry starts
        clean) and does not commit, matching the documented guarantee."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()

        def fake_read(query, batch_size=5000):
            return (["a"], _gen([[(1,)]]))

        with (
            patch.object(election_api_utils, "read_databricks_table", side_effect=fake_read),
            patch.object(
                election_api_utils.psycopg2.extras,
                "execute_values",
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            bulk_insert_from_databricks(conn, _spec(), "SELECT a FROM t", ["a"])

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_distinct_read_closed_on_consumer_error(self):
        """If consuming the distinct-values result raises, the batches generator
        is still closed (no leaked Databricks connection)."""
        closed = {"v": False}

        def gen():
            try:
                yield [()]  # empty row -> row[0] raises IndexError in the caller
            finally:
                closed["v"] = True

        batches = gen()

        def fake_read(query, batch_size=5000):
            return (["_pv"], batches)

        with (
            patch.object(election_api_utils, "read_databricks_table", side_effect=fake_read),
            pytest.raises(IndexError),
        ):
            election_api_utils._partition_queries("SELECT a, state FROM t", "state", 5000)

        assert closed["v"] is True

    def test_partition_escapes_single_quotes(self):
        """Single quotes in a partition value are escaped in the predicate."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        reads = []

        def fake_read(query, batch_size=5000):
            reads.append(query)
            if "DISTINCT" in query:
                return (["_pv"], _gen([[("O'Brien",)]]))
            return (["a"], _gen([[(1,)]]))

        with (
            patch.object(election_api_utils, "read_databricks_table", side_effect=fake_read),
            patch.object(election_api_utils.psycopg2.extras, "execute_values"),
        ):
            bulk_insert_from_databricks(
                conn,
                _spec(),
                "SELECT a, county FROM t",
                ["a"],
                partition_column="county",
            )

        assert any("O''Brien" in q for q in reads)
