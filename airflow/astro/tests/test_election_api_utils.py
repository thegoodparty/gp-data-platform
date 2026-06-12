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
