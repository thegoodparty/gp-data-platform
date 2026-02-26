"""Tests for Databricks utility functions."""

from unittest.mock import MagicMock

import pytest
from include.custom_functions.databricks_utils import (
    _validate_lalvoterids,
    get_processed_files,
    stage_expired_voter_ids,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_connection():
    """Create a mock Databricks connection with a mock cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ---------------------------------------------------------------------------
# _validate_lalvoterids
# ---------------------------------------------------------------------------


class TestValidateLalvoterids:
    """Tests for LALVOTERID format validation."""

    def test_valid_ids(self):
        """Well-formed LALVOTERIDs pass validation."""
        _validate_lalvoterids(["LALMD1207645", "LALCA22155264", "LALWY0001"])

    def test_empty_list(self):
        """Empty list passes validation."""
        _validate_lalvoterids([])

    def test_invalid_id_raises(self):
        """Mixed valid/invalid IDs raise ValueError."""
        with pytest.raises(ValueError, match="invalid LALVOTERID"):
            _validate_lalvoterids(["LALMD1207645", "BAD_ID", "LALCA0001"])

    def test_missing_digits_raises(self):
        """State code without trailing digits is rejected."""
        with pytest.raises(ValueError, match="invalid LALVOTERID"):
            _validate_lalvoterids(["LALMD"])

    def test_lowercase_raises(self):
        """Lowercase IDs are rejected."""
        with pytest.raises(ValueError, match="invalid LALVOTERID"):
            _validate_lalvoterids(["lalmd1207645"])

    def test_sql_injection_attempt_raises(self):
        """SQL metacharacters in IDs are rejected."""
        with pytest.raises(ValueError, match="invalid LALVOTERID"):
            _validate_lalvoterids(["LALMD1'; DROP TABLE --"])

    def test_error_shows_first_five(self):
        """Error message includes count and first 5 bad values."""
        bad_ids = [f"BAD{i}" for i in range(10)]
        with pytest.raises(ValueError, match="10 invalid") as exc_info:
            _validate_lalvoterids(bad_ids)
        assert "BAD0" in str(exc_info.value)
        assert "BAD4" in str(exc_info.value)


# ---------------------------------------------------------------------------
# get_processed_files
# ---------------------------------------------------------------------------


class TestGetProcessedFiles:
    """Tests for processed file retrieval from the loads metadata table."""

    def test_loads_table_exists_with_files(self, mock_connection):
        """Return set of completed file keys when loads table exists."""
        conn, cursor = mock_connection
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.return_value = [
            ("file1.tab|2026-01-15T10:00:00+00:00",),
            ("file2.tab|2026-01-16T10:00:00+00:00",),
        ]

        result = get_processed_files(conn, catalog="cat", schema="sch")

        assert result == {
            "file1.tab|2026-01-15T10:00:00+00:00",
            "file2.tab|2026-01-16T10:00:00+00:00",
        }
        assert cursor.execute.call_count == 2
        # Info schema check targets the loads table
        info_sql = cursor.execute.call_args_list[0][0][0]
        assert "l2_expired_voters_loads" in info_sql
        # Data query reads from the loads table
        data_sql = cursor.execute.call_args_list[1][0][0]
        assert "l2_expired_voters_loads" in data_sql
        assert "source_file_keys" in data_sql

    def test_loads_table_does_not_exist(self, mock_connection):
        """Return empty set when loads table does not exist yet."""
        conn, cursor = mock_connection
        cursor.fetchone.return_value = None

        result = get_processed_files(conn, catalog="cat", schema="sch")

        assert result == set()
        assert cursor.execute.call_count == 1

    def test_loads_table_exists_no_files(self, mock_connection):
        """Return empty set when loads table exists but has no rows."""
        conn, cursor = mock_connection
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.return_value = []

        result = get_processed_files(conn, catalog="cat", schema="sch")
        assert result == set()


# ---------------------------------------------------------------------------
# stage_expired_voter_ids
# ---------------------------------------------------------------------------


class TestStageExpiredVoterIds:
    """Tests for staging expired voter IDs to Databricks."""

    def test_basic_staging(self, mock_connection):
        """Stage IDs with schema/table creation and idempotent cleanup."""
        conn, cursor = mock_connection

        result = stage_expired_voter_ids(
            connection=conn,
            catalog="cat",
            schema="sch",
            lalvoterids=["LALMD0001", "LALMD0002"],
            source_files=["file1.tab"],
            dag_run_id="run_123",
        )

        assert result == 2
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS" in c for c in calls)
        create_tables = [c for c in calls if "CREATE TABLE IF NOT EXISTS" in c]
        assert len(create_tables) == 2
        assert any("source_file_keys STRING" in c for c in create_tables)
        assert any("row_count INT" in c for c in create_tables)
        # Idempotent cleanup of both data and loads tables
        delete_calls = [c for c in calls if "DELETE FROM" in c]
        assert len(delete_calls) == 2
        # Data insert + loads completion record
        insert_calls = [c for c in calls if "INSERT INTO" in c]
        assert len(insert_calls) == 2
        # Last INSERT is the loads table completion record
        assert "l2_expired_voters_loads" in insert_calls[-1]

    def test_batching(self, mock_connection):
        """Verify IDs exceeding batch_size are split across INSERT statements."""
        conn, cursor = mock_connection
        ids = [f"LALCA{str(i).zfill(4)}" for i in range(5)]

        result = stage_expired_voter_ids(
            connection=conn,
            catalog="cat",
            schema="sch",
            lalvoterids=ids,
            source_files=["file1.tab"],
            dag_run_id="run_123",
            batch_size=2,
        )

        assert result == 5
        insert_calls = [
            c[0][0] for c in cursor.execute.call_args_list if "INSERT INTO" in c[0][0]
        ]
        # 3 data batches [2, 2, 1] + 1 loads completion record
        assert len(insert_calls) == 4
        assert "l2_expired_voters_loads" in insert_calls[-1]

    def test_with_file_timestamps(self, mock_connection):
        """File timestamp is included in INSERT SQL when provided."""
        conn, cursor = mock_connection

        stage_expired_voter_ids(
            connection=conn,
            catalog="cat",
            schema="sch",
            lalvoterids=["LALMD0001"],
            source_files=["file1.tab"],
            dag_run_id="run_123",
            file_timestamps={"file1.tab": "2026-01-15T10:00:00+00:00"},
        )

        insert_sql = [
            c[0][0] for c in cursor.execute.call_args_list if "INSERT INTO" in c[0][0]
        ][0]
        assert "2026-01-15T10:00:00+00:00" in insert_sql
        # source_file_keys stores composite "filename|mtime" for idempotency
        assert "file1.tab|2026-01-15T10:00:00+00:00" in insert_sql
        assert "source_file_keys" in insert_sql

    def test_without_file_timestamps_uses_null(self, mock_connection):
        """NULL is used for file_modified_at when no timestamps provided."""
        conn, cursor = mock_connection

        stage_expired_voter_ids(
            connection=conn,
            catalog="cat",
            schema="sch",
            lalvoterids=["LALMD0001"],
            source_files=["file1.tab"],
            dag_run_id="run_123",
        )

        insert_sql = [
            c[0][0] for c in cursor.execute.call_args_list if "INSERT INTO" in c[0][0]
        ][0]
        assert "NULL" in insert_sql

    def test_dag_run_id_escaped(self, mock_connection):
        """Single quotes in dag_run_id are escaped in SQL."""
        conn, cursor = mock_connection

        stage_expired_voter_ids(
            connection=conn,
            catalog="cat",
            schema="sch",
            lalvoterids=["LALMD0001"],
            source_files=["file1.tab"],
            dag_run_id="run_with'quote",
        )

        delete_calls = [
            c[0][0] for c in cursor.execute.call_args_list if "DELETE FROM" in c[0][0]
        ]
        # Both data table and loads table DELETEs should have escaped dag_run_id
        assert len(delete_calls) == 2
        for sql in delete_calls:
            assert "run_with\\'quote" in sql

    def test_loads_record_is_last_insert(self, mock_connection):
        """Loads completion record is written after all data batch inserts."""
        conn, cursor = mock_connection
        ids = [f"LALCA{str(i).zfill(4)}" for i in range(3)]

        stage_expired_voter_ids(
            connection=conn,
            catalog="cat",
            schema="sch",
            lalvoterids=ids,
            source_files=["file1.tab"],
            dag_run_id="run_123",
            batch_size=2,
        )

        insert_calls = [
            c[0][0] for c in cursor.execute.call_args_list if "INSERT INTO" in c[0][0]
        ]
        # Data inserts come first, loads record comes last
        for sql in insert_calls[:-1]:
            assert "l2_expired_voters`" in sql
        assert "l2_expired_voters_loads" in insert_calls[-1]
        assert "3" in insert_calls[-1]  # row_count
