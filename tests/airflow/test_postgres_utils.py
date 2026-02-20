"""Tests for PostgreSQL utility functions."""

from unittest.mock import MagicMock

import pytest
from include.custom_functions.postgres_utils import (
    delete_expired_voters,
    execute_query,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conn():
    """Create a mock psycopg2 connection with a mock cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ---------------------------------------------------------------------------
# execute_query
# ---------------------------------------------------------------------------


class TestExecuteQuery:
    """Tests for the generic query executor."""

    def test_execute_without_results(self, mock_conn):
        """Execute a mutation query and commit."""
        conn, cursor = mock_conn

        result = execute_query(conn, "DELETE FROM foo WHERE id = %s", (1,))

        cursor.execute.assert_called_once_with("DELETE FROM foo WHERE id = %s", (1,))
        conn.commit.assert_called_once()
        assert result == []

    def test_execute_with_results(self, mock_conn):
        """Execute a SELECT and return fetched rows."""
        conn, cursor = mock_conn
        cursor.rowcount = 2
        cursor.fetchall.return_value = [(1, "a"), (2, "b")]

        result = execute_query(
            conn,
            "SELECT * FROM foo",
            return_results=True,
        )

        assert result == [(1, "a"), (2, "b")]
        conn.commit.assert_called_once()

    def test_execute_with_results_empty(self, mock_conn):
        """Return empty list when query matches no rows."""
        conn, cursor = mock_conn
        cursor.fetchall.return_value = []

        result = execute_query(
            conn,
            "SELECT * FROM foo WHERE 1=0",
            return_results=True,
        )

        assert result == []
        cursor.fetchall.assert_called_once()

    def test_rollback_on_error(self, mock_conn):
        """Roll back and re-raise on query failure."""
        conn, cursor = mock_conn
        cursor.execute.side_effect = RuntimeError("DB error")

        with pytest.raises(RuntimeError, match="DB error"):
            execute_query(conn, "BAD SQL")

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_cursor_closed_on_success(self, mock_conn):
        """Cursor is closed after successful execution."""
        conn, cursor = mock_conn
        execute_query(conn, "SELECT 1")
        cursor.close.assert_called_once()

    def test_cursor_closed_on_error(self, mock_conn):
        """Cursor is closed even when execution fails."""
        conn, cursor = mock_conn
        cursor.execute.side_effect = RuntimeError("fail")
        with pytest.raises(RuntimeError):
            execute_query(conn, "BAD SQL")
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# delete_expired_voters
# ---------------------------------------------------------------------------


class TestDeleteExpiredVoters:
    """Tests for People-API voter deletion."""

    def test_single_batch_fk_order(self, mock_conn):
        """Delete DistrictVoter before Voter (FK dependency order)."""
        conn, cursor = mock_conn
        cursor.rowcount = 5

        result = delete_expired_voters(
            conn=conn,
            schema="green",
            lalvoterids=["LALMD0001", "LALMD0002"],
        )

        # Should have 2 execute calls: DistrictVoter first, then Voter
        assert cursor.execute.call_count == 2
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert '"DistrictVoter"' in calls[0]
        assert '"Voter"' in calls[1]

        # Both use parameterized queries
        for c in cursor.execute.call_args_list:
            assert c[0][1] == (["LALMD0001", "LALMD0002"],)

        conn.commit.assert_called_once()
        assert result["district_voter_deleted"] == 5
        assert result["voter_deleted"] == 5

    def test_multi_batch(self, mock_conn):
        """Multiple batches each get their own commit."""
        conn, cursor = mock_conn
        cursor.rowcount = 1

        ids = [f"LALCA{str(i).zfill(4)}" for i in range(5)]
        result = delete_expired_voters(
            conn=conn,
            schema="green",
            lalvoterids=ids,
            batch_size=2,
        )

        # 3 batches * 2 queries each = 6 execute calls
        assert cursor.execute.call_count == 6
        # 3 commits (one per batch)
        assert conn.commit.call_count == 3
        # Each call returns rowcount=1
        assert result["district_voter_deleted"] == 3
        assert result["voter_deleted"] == 3

    def test_uses_correct_schema(self, mock_conn):
        """Schema name appears in all generated SQL."""
        conn, cursor = mock_conn
        cursor.rowcount = 0

        delete_expired_voters(
            conn=conn,
            schema="my_schema",
            lalvoterids=["LALMD0001"],
        )

        for c in cursor.execute.call_args_list:
            assert "my_schema." in c[0][0]

    def test_parameterized_queries(self, mock_conn):
        """Verify IDs are passed as parameters, not interpolated into SQL."""
        conn, cursor = mock_conn
        cursor.rowcount = 0

        delete_expired_voters(
            conn=conn,
            schema="green",
            lalvoterids=["LALMD0001"],
        )

        for c in cursor.execute.call_args_list:
            sql = c[0][0]
            # IDs should NOT appear in the SQL string itself
            assert "LALMD0001" not in sql
            # Should use ANY(%s) parameterized pattern
            assert "ANY(%s)" in sql

    def test_empty_ids(self, mock_conn):
        """Empty ID list returns zeros without executing."""
        conn, cursor = mock_conn

        result = delete_expired_voters(
            conn=conn,
            schema="green",
            lalvoterids=[],
        )

        cursor.execute.assert_not_called()
        assert result["district_voter_deleted"] == 0
        assert result["voter_deleted"] == 0

    def test_cursor_closed_per_query(self, mock_conn):
        """Each query gets its own cursor that is closed after use."""
        conn, _ = mock_conn
        cursors = [MagicMock(rowcount=0), MagicMock(rowcount=0)]
        conn.cursor.side_effect = cursors

        delete_expired_voters(
            conn=conn,
            schema="green",
            lalvoterids=["LALMD0001"],
        )

        for c in cursors:
            c.close.assert_called_once()
