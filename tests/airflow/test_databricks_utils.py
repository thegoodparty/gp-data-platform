"""Tests for Databricks utility functions."""

from unittest.mock import MagicMock

import pytest
from include.custom_functions.databricks_utils import (
    _validate_lalvoterids,
    count_in_databricks_table,
    delete_from_databricks_table,
    get_processed_files,
    get_staged_voter_ids,
    mark_staging_complete,
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
# delete_from_databricks_table
# ---------------------------------------------------------------------------


class TestDeleteFromDatabricksTable:
    """Tests for Databricks DELETE operations."""

    def test_single_batch(self, mock_connection):
        """Delete a small batch in one query."""
        conn, cursor = mock_connection
        cursor.rowcount = 3

        result = delete_from_databricks_table(
            connection=conn,
            catalog="cat",
            schema="sch",
            table="tbl",
            column="LALVOTERID",
            values=["LALMD0001", "LALMD0002", "LALMD0003"],
        )

        assert result == 3
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "DELETE FROM `cat`.`sch`.`tbl`" in sql
        assert "'LALMD0001'" in sql
        assert "'LALMD0003'" in sql

    def test_multi_batch(self, mock_connection):
        """Values exceeding batch_size are split across queries."""
        conn, cursor = mock_connection
        cursor.rowcount = 2

        ids = [f"LALCA{str(i).zfill(4)}" for i in range(5)]
        result = delete_from_databricks_table(
            connection=conn,
            catalog="cat",
            schema="sch",
            table="tbl",
            column="LALVOTERID",
            values=ids,
            batch_size=2,
        )

        # 3 batches: [2, 2, 1], each returning rowcount=2
        assert cursor.execute.call_count == 3
        assert result == 6

    def test_empty_values(self, mock_connection):
        """Empty values list returns 0 without executing."""
        conn, cursor = mock_connection
        result = delete_from_databricks_table(
            connection=conn,
            catalog="cat",
            schema="sch",
            table="tbl",
            column="LALVOTERID",
            values=[],
        )
        assert result == 0
        cursor.execute.assert_not_called()

    def test_negative_rowcount_treated_as_zero(self, mock_connection):
        """Negative rowcount (driver quirk) is clamped to 0."""
        conn, cursor = mock_connection
        cursor.rowcount = -1

        result = delete_from_databricks_table(
            connection=conn,
            catalog="cat",
            schema="sch",
            table="tbl",
            column="LALVOTERID",
            values=["LALMD0001"],
        )
        assert result == 0

    def test_validation_failure_raises(self, mock_connection):
        """Invalid IDs raise ValueError before any SQL executes."""
        conn, cursor = mock_connection
        with pytest.raises(ValueError, match="invalid LALVOTERID"):
            delete_from_databricks_table(
                connection=conn,
                catalog="cat",
                schema="sch",
                table="tbl",
                column="LALVOTERID",
                values=["BAD_VALUE"],
            )
        cursor.execute.assert_not_called()


# ---------------------------------------------------------------------------
# count_in_databricks_table
# ---------------------------------------------------------------------------


class TestCountInDatabricksTable:
    """Tests for Databricks COUNT operations."""

    def test_single_batch(self, mock_connection):
        """Count matching rows in one query."""
        conn, cursor = mock_connection
        cursor.fetchone.return_value = (5,)

        result = count_in_databricks_table(
            connection=conn,
            catalog="cat",
            schema="sch",
            table="tbl",
            column="LALVOTERID",
            values=["LALMD0001", "LALMD0002"],
        )

        assert result == 5
        sql = cursor.execute.call_args[0][0]
        assert "SELECT COUNT(*)" in sql

    def test_multi_batch_sums(self, mock_connection):
        """Counts from multiple batches are summed."""
        conn, cursor = mock_connection
        cursor.fetchone.side_effect = [(3,), (2,)]

        ids = [f"LALCA{str(i).zfill(4)}" for i in range(4)]
        result = count_in_databricks_table(
            connection=conn,
            catalog="cat",
            schema="sch",
            table="tbl",
            column="LALVOTERID",
            values=ids,
            batch_size=2,
        )
        assert result == 5

    def test_no_results(self, mock_connection):
        """None from fetchone is treated as 0."""
        conn, cursor = mock_connection
        cursor.fetchone.return_value = None

        result = count_in_databricks_table(
            connection=conn,
            catalog="cat",
            schema="sch",
            table="tbl",
            column="LALVOTERID",
            values=["LALMD0001"],
        )
        assert result == 0


# ---------------------------------------------------------------------------
# get_processed_files
# ---------------------------------------------------------------------------


class TestGetProcessedFiles:
    """Tests for processed file retrieval."""

    def test_table_exists_with_files(self, mock_connection):
        """Return set of completed file keys when table exists."""
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
        # Verify query reads from source_file_keys column
        data_sql = cursor.execute.call_args_list[1][0][0]
        assert "source_file_keys" in data_sql

    def test_table_does_not_exist(self, mock_connection):
        """Return empty set when staging table does not exist yet."""
        conn, cursor = mock_connection
        cursor.fetchone.return_value = None

        result = get_processed_files(conn, catalog="cat", schema="sch")

        assert result == set()
        # Only the information_schema check should have been executed
        assert cursor.execute.call_count == 1

    def test_table_exists_no_completed_files(self, mock_connection):
        """Return empty set when table exists but has no completed rows."""
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
        create_table = [c for c in calls if "CREATE TABLE IF NOT EXISTS" in c][0]
        assert "source_file_keys STRING" in create_table
        # Idempotent cleanup of previous pending rows
        assert any("DELETE FROM" in c and "pending" in c for c in calls)
        assert any("INSERT INTO" in c for c in calls)

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
        assert len(insert_calls) == 3  # batches: [2, 2, 1]

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

        delete_sql = [
            c[0][0]
            for c in cursor.execute.call_args_list
            if "DELETE FROM" in c[0][0] and "pending" in c[0][0]
        ][0]
        assert "run_with\\'quote" in delete_sql


# ---------------------------------------------------------------------------
# get_staged_voter_ids
# ---------------------------------------------------------------------------


class TestGetStagedVoterIds:
    """Tests for reading staged voter IDs."""

    def test_returns_ids(self, mock_connection):
        """Return list of LALVOTERIDs for the given dag_run_id."""
        conn, cursor = mock_connection
        cursor.fetchall.return_value = [("LALMD0001",), ("LALMD0002",)]

        result = get_staged_voter_ids(
            conn, catalog="cat", schema="sch", dag_run_id="run_123"
        )

        assert result == ["LALMD0001", "LALMD0002"]
        sql = cursor.execute.call_args[0][0]
        assert "run_123" in sql

    def test_empty_result(self, mock_connection):
        """Return empty list when no rows match the dag_run_id."""
        conn, cursor = mock_connection
        cursor.fetchall.return_value = []

        result = get_staged_voter_ids(
            conn, catalog="cat", schema="sch", dag_run_id="run_123"
        )
        assert result == []


# ---------------------------------------------------------------------------
# mark_staging_complete
# ---------------------------------------------------------------------------


class TestMarkStagingComplete:
    """Tests for marking staged rows as completed."""

    def test_updates_rows(self, mock_connection):
        """Update pending rows to completed and return count."""
        conn, cursor = mock_connection
        cursor.rowcount = 10

        result = mark_staging_complete(
            conn, catalog="cat", schema="sch", dag_run_id="run_123"
        )

        assert result == 10
        sql = cursor.execute.call_args[0][0]
        assert "SET status = 'completed'" in sql
        assert "status = 'pending'" in sql
        assert "run_123" in sql

    def test_no_rows_to_update(self, mock_connection):
        """Return 0 when no pending rows exist for the dag_run_id."""
        conn, cursor = mock_connection
        cursor.rowcount = 0

        result = mark_staging_complete(
            conn, catalog="cat", schema="sch", dag_run_id="run_123"
        )
        assert result == 0

    def test_scoped_to_lalvoterids(self, mock_connection):
        """Only mark rows matching the given LALVOTERIDs as completed."""
        conn, cursor = mock_connection
        cursor.rowcount = 2

        result = mark_staging_complete(
            conn,
            catalog="cat",
            schema="sch",
            dag_run_id="run_123",
            lalvoterids=["LALMD0001", "LALMD0002"],
        )

        assert result == 2
        sql = cursor.execute.call_args[0][0]
        assert "SET status = 'completed'" in sql
        assert "lalvoterid IN (" in sql
        assert "'LALMD0001'" in sql
        assert "'LALMD0002'" in sql

    def test_scoped_empty_list_skips(self, mock_connection):
        """Empty lalvoterids list returns 0 without executing."""
        conn, cursor = mock_connection

        result = mark_staging_complete(
            conn,
            catalog="cat",
            schema="sch",
            dag_run_id="run_123",
            lalvoterids=[],
        )

        assert result == 0
        cursor.execute.assert_not_called()

    def test_scoped_batching(self, mock_connection):
        """Scoped update batches large ID lists."""
        conn, cursor = mock_connection
        cursor.rowcount = 2

        ids = [f"LALCA{str(i).zfill(4)}" for i in range(5)]
        result = mark_staging_complete(
            conn,
            catalog="cat",
            schema="sch",
            dag_run_id="run_123",
            lalvoterids=ids,
            batch_size=2,
        )

        # 3 batches: [2, 2, 1], each returning rowcount=2
        assert cursor.execute.call_count == 3
        assert result == 6
