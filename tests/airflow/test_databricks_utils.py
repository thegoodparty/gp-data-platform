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
        # Each row is a DAG run's comma-joined source_file_keys
        cursor.fetchall.return_value = [
            (
                "file1.tab|2026-01-15T10:00:00+00:00, file2.tab|2026-01-16T10:00:00+00:00",
            ),
        ]

        result = get_processed_files(conn, catalog="cat", schema="sch")

        assert result == {
            "file1.tab|2026-01-15T10:00:00+00:00",
            "file2.tab|2026-01-16T10:00:00+00:00",
        }
        assert cursor.execute.call_count == 2
        # Info schema check uses raw catalog in backticks, escaped in WHERE
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
    """Tests for staging expired voter IDs to Databricks via MERGE (upsert)."""

    def test_basic_staging(self, mock_connection):
        """Stage IDs with schema/table creation, MERGE upsert, and loads record."""
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
        # Only loads table gets DELETE (MERGE handles data table idempotency)
        delete_calls = [c for c in calls if "DELETE FROM" in c]
        assert len(delete_calls) == 1
        assert "l2_expired_voters_loads" in delete_calls[0]
        # Data uses MERGE, loads uses INSERT
        merge_calls = [c for c in calls if "MERGE INTO" in c]
        assert len(merge_calls) == 1
        assert "WHEN MATCHED THEN UPDATE SET" in merge_calls[0]
        assert "source.source_files" in merge_calls[0]
        assert "WHEN NOT MATCHED THEN INSERT" in merge_calls[0]
        insert_calls = [c for c in calls if "INSERT INTO" in c]
        assert len(insert_calls) == 1
        assert "l2_expired_voters_loads" in insert_calls[0]

    def test_batching(self, mock_connection):
        """Verify IDs exceeding batch_size are split across MERGE statements."""
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
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        merge_calls = [c for c in calls if "MERGE INTO" in c]
        assert len(merge_calls) == 3  # batches: [2, 2, 1]
        # Loads completion record is a single INSERT
        insert_calls = [c for c in calls if "INSERT INTO" in c]
        assert len(insert_calls) == 1
        assert "l2_expired_voters_loads" in insert_calls[0]

    def test_with_file_timestamps(self, mock_connection):
        """File timestamp is included in MERGE SQL when provided."""
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

        merge_sql = [
            c[0][0] for c in cursor.execute.call_args_list if "MERGE INTO" in c[0][0]
        ][0]
        assert "2026-01-15T10:00:00+00:00" in merge_sql
        # source_file_keys stores composite "filename|mtime" for idempotency
        assert "file1.tab|2026-01-15T10:00:00+00:00" in merge_sql
        assert "source_file_keys" in merge_sql

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

        merge_sql = [
            c[0][0] for c in cursor.execute.call_args_list if "MERGE INTO" in c[0][0]
        ][0]
        assert "NULL" in merge_sql

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

        # Loads table DELETE has escaped dag_run_id
        delete_calls = [
            c[0][0] for c in cursor.execute.call_args_list if "DELETE FROM" in c[0][0]
        ]
        assert len(delete_calls) == 1
        assert "run_with\\'quote" in delete_calls[0]
        # MERGE also has escaped dag_run_id
        merge_sql = [
            c[0][0] for c in cursor.execute.call_args_list if "MERGE INTO" in c[0][0]
        ][0]
        assert "run_with\\'quote" in merge_sql

    def test_loads_record_is_last_execute(self, mock_connection):
        """Loads completion record is written after all data MERGE batches."""
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

        calls = [c[0][0] for c in cursor.execute.call_args_list]
        # Find positions of MERGE and loads INSERT
        merge_positions = [i for i, c in enumerate(calls) if "MERGE INTO" in c]
        loads_insert_pos = [i for i, c in enumerate(calls) if "INSERT INTO" in c]
        assert len(loads_insert_pos) == 1
        # All MERGEs come before the loads INSERT
        assert all(m < loads_insert_pos[0] for m in merge_positions)
        assert "3" in calls[loads_insert_pos[0]]  # row_count
