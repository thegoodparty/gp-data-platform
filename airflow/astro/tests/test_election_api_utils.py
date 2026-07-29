"""Tests for election-api sync utilities (bulk_insert_from_databricks)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
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


class TestBulkInsert:
    """bulk_insert routes partitioned reads through read_databricks_partitioned
    (single connection, bounded memory) and preserves single-commit
    retry-safety."""

    def test_no_partition_reads_whole_table_once(self):
        """Without partition_column, the source query is read via read_databricks_table."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()

        with (
            patch.object(
                election_api_utils,
                "read_databricks_table",
                return_value=(["a"], _gen([[(1,), (2,)]])),
            ) as mock_read,
            patch.object(election_api_utils.psycopg2.extras, "execute_values") as ev,
        ):
            total = bulk_insert_from_databricks(conn, _spec(), "SELECT a FROM t", ["a"])

        assert total == 2
        mock_read.assert_called_once()
        assert ev.call_count == 1
        conn.commit.assert_called_once()

    def test_partition_uses_partitioned_reader(self):
        """With partition_column, batches come from read_databricks_partitioned and
        all are inserted under a single end-of-load commit."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()

        with (
            patch.object(
                election_api_utils,
                "read_databricks_partitioned",
                return_value=_gen([[(1,)], [(2,), (3,)]]),
            ) as mock_part,
            patch.object(election_api_utils, "read_databricks_table") as mock_whole,
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
        mock_part.assert_called_once()
        assert mock_part.call_args.args[1] == "state"
        mock_whole.assert_not_called()  # partitioned path does not read the whole table
        conn.commit.assert_called_once()

    def test_load_failure_rolls_back(self):
        """A mid-load error rolls the staging transaction back and does not commit."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()

        with (
            patch.object(
                election_api_utils,
                "read_databricks_table",
                return_value=(["a"], _gen([[(1,)]])),
            ),
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

    def test_numpy_arrays_from_arrow_reads_become_python_lists(self):
        """The arrow-backed Databricks connector returns ARRAY columns as
        numpy arrays whose elements are numpy scalars; psycopg2 can only
        adapt Python lists of native values, so the loader must normalize
        every value before execute_values sees it."""
        conn = MagicMock()
        conn.cursor.return_value = MagicMock()
        batch = [(1, np.array([2, 4], dtype=np.int64), np.array(["Mayor", "Clerk"]))]

        with (
            patch.object(
                election_api_utils,
                "read_databricks_table",
                return_value=(["id", "frequency", "position_names"], _gen([batch])),
            ),
            patch.object(election_api_utils.psycopg2.extras, "execute_values") as ev,
        ):
            total = bulk_insert_from_databricks(
                conn,
                _spec(),
                "SELECT id, frequency, position_names FROM t",
                ["id", "frequency", "position_names"],
            )

        assert total == 1
        (row,) = ev.call_args.args[2]
        assert type(row[1]) is list
        assert type(row[2]) is list
        assert row[1] == [2, 4]
        assert row[2] == ["Mayor", "Clerk"]
        assert all(type(v) is int for v in row[1])
        assert all(type(v) is str for v in row[2])
