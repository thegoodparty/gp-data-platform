"""Tests for Databricks utility functions."""

from unittest.mock import MagicMock, patch

import pytest
from include.custom_functions import databricks_utils
from include.custom_functions.databricks_utils import (
    _is_non_retryable_auth_error,
    execute_with_retry,
    get_databricks_connection,
    read_databricks_partitioned,
    read_databricks_table,
)


class TestExecuteWithRetry:
    """The classifier decides whether a failed statement is worth another 30s."""

    def test_parameters_are_forwarded(self):
        cursor = MagicMock()
        execute_with_retry(cursor, "SELECT :x", parameters={"x": 1})
        cursor.execute.assert_called_once_with("SELECT :x", {"x": 1})

    def test_no_parameters_means_a_single_argument(self):
        """The connector treats an explicit None differently from an omitted argument."""
        cursor = MagicMock()
        execute_with_retry(cursor, "SELECT 1")
        cursor.execute.assert_called_once_with("SELECT 1")

    def test_transient_error_is_retried(self):
        cursor = MagicMock()
        cursor.execute.side_effect = [Exception("status code 503"), None]
        execute_with_retry(cursor, "SELECT 1", retry_delay=0)
        assert cursor.execute.call_count == 2

    def test_other_errors_are_not_retried(self):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")
        with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
            execute_with_retry(cursor, "SELECT 1", retry_delay=0)
        assert cursor.execute.call_count == 1

    def test_retries_are_bounded(self):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Service Unavailable")
        with pytest.raises(Exception, match="Service Unavailable"):
            execute_with_retry(cursor, "SELECT 1", max_retries=3, retry_delay=0)
        assert cursor.execute.call_count == 3


class TestIsNonRetryableAuthError:
    """The classifier that decides whether to fail fast."""

    @pytest.mark.parametrize(
        "message",
        [
            "invalid_client: Client authentication failed",
            "Error during request to server. invalid_client: ...",
            "CLIENT AUTHENTICATION FAILED",  # case-insensitive
        ],
    )
    def test_auth_errors_are_non_retryable(self, message):
        """Credential auth failures are classified as non-retryable."""
        assert _is_non_retryable_auth_error(Exception(message)) is True

    @pytest.mark.parametrize(
        "message",
        [
            "warehouse is starting",
            "connection timed out",
            "temporary network failure",
        ],
    )
    def test_other_errors_are_retryable(self, message):
        """Transient / cold-start errors are not classified as non-retryable."""
        assert _is_non_retryable_auth_error(Exception(message)) is False


# ---------------------------------------------------------------------------
# get_databricks_connection
# ---------------------------------------------------------------------------


class TestGetDatabricksConnection:
    """Retry behavior of get_databricks_connection."""

    def _kwargs(self, **overrides):
        """Build connection kwargs with a zero retry delay for fast tests."""
        base = {
            "host": "https://example.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc",
            "client_id": "cid",
            "client_secret": "secret",
            "retry_delay": 0,
        }
        base.update(overrides)
        return base

    def test_fails_fast_on_auth_error_without_retrying(self):
        """An invalid_client error raises immediately with no retry sleeps."""
        auth_error = Exception("invalid_client: Client authentication failed")
        with (
            patch.object(databricks_utils.databricks_sql, "connect", side_effect=auth_error) as mock_connect,
            patch.object(databricks_utils.time, "sleep") as mock_sleep,
            pytest.raises(Exception, match="invalid_client"),
        ):
            get_databricks_connection(**self._kwargs(max_retries=20))

        assert mock_connect.call_count == 1
        mock_sleep.assert_not_called()

    def test_retries_transient_errors_then_succeeds(self):
        """A transient error is retried and the eventual connection returned."""
        conn = MagicMock()
        with (
            patch.object(
                databricks_utils.databricks_sql,
                "connect",
                side_effect=[Exception("warehouse may be starting"), conn],
            ) as mock_connect,
            patch.object(databricks_utils.time, "sleep") as mock_sleep,
        ):
            result = get_databricks_connection(**self._kwargs(max_retries=5))

        assert result is conn
        assert mock_connect.call_count == 2
        mock_sleep.assert_called_once()

    def test_raises_after_exhausting_retries_on_transient_errors(self):
        """Persistent transient errors raise after exhausting max_retries."""
        with (
            patch.object(
                databricks_utils.databricks_sql,
                "connect",
                side_effect=Exception("warehouse may be starting"),
            ) as mock_connect,
            patch.object(databricks_utils.time, "sleep"),
            pytest.raises(Exception, match="warehouse"),
        ):
            get_databricks_connection(**self._kwargs(max_retries=3))

        assert mock_connect.call_count == 3


# ---------------------------------------------------------------------------
# read_databricks_table
# ---------------------------------------------------------------------------


class TestReadDatabricksTable:
    """Validation, error handling, and happy path of read_databricks_table."""

    def _db_conn(
        self,
        host="https://example.cloud.databricks.com",
        login="cid",
        password="secret",
        http_path="/sql/1.0/warehouses/abc",
    ):
        """A mock Airflow Connection with Databricks fields."""
        db_conn = MagicMock()
        db_conn.host = host
        db_conn.login = login
        db_conn.password = password
        db_conn.extra_dejson = {"http_path": http_path} if http_path else {}
        return db_conn

    @pytest.mark.parametrize("missing", ["host", "login", "password", "http_path"])
    def test_missing_required_field_raises_value_error(self, missing):
        """A connection missing host/login/password/http_path fails fast with a clear error."""
        fields = {"host": "h", "login": "lg", "password": "pw", "http_path": "/sql/p"}
        fields[missing] = "" if missing == "http_path" else None
        with (
            patch.object(databricks_utils.Variable, "get", return_value="conn-id"),
            patch.object(databricks_utils.BaseHook, "get_connection", return_value=self._db_conn(**fields)),
            pytest.raises(ValueError, match="missing a required"),
        ):
            read_databricks_table("SELECT 1")

    def test_none_cursor_description_raises_runtime_error_and_closes(self):
        """A cursor with no description after execute raises and closes the connection."""
        cursor = MagicMock()
        cursor.description = None
        connection = MagicMock()
        connection.cursor.return_value = cursor
        with (
            patch.object(databricks_utils.Variable, "get", return_value="conn-id"),
            patch.object(databricks_utils.BaseHook, "get_connection", return_value=self._db_conn()),
            patch.object(databricks_utils, "get_databricks_connection", return_value=connection),
            pytest.raises(RuntimeError, match="no description"),
        ):
            read_databricks_table("SELECT 1")

        connection.close.assert_called_once()

    def test_happy_path_returns_columns_and_streams_batches(self):
        """Valid credentials yield column names eagerly and stream row batches lazily."""
        cursor = MagicMock()
        cursor.description = [("col_a",), ("col_b",)]
        cursor.fetchmany.side_effect = [[(1, 2)], []]  # one batch, then exhausted
        connection = MagicMock()
        connection.cursor.return_value = cursor
        with (
            patch.object(databricks_utils.Variable, "get", return_value="conn-id"),
            patch.object(databricks_utils.BaseHook, "get_connection", return_value=self._db_conn()),
            patch.object(
                databricks_utils, "get_databricks_connection", return_value=connection
            ) as mock_get_conn,
        ):
            column_names, batches = read_databricks_table("SELECT 1")
            assert column_names == ["col_a", "col_b"]  # available before iterating
            rows = list(batches)

        assert rows == [[(1, 2)]]
        # http_path is extracted from the connection's extra and forwarded
        assert mock_get_conn.call_args.kwargs["http_path"] == "/sql/1.0/warehouses/abc"
        cursor.close.assert_called_once()
        connection.close.assert_called_once()


# ---------------------------------------------------------------------------
# read_databricks_partitioned
# ---------------------------------------------------------------------------


class TestReadDatabricksPartitioned:
    """One Databricks connection for the whole read; one distinct value at a time."""

    def _db_conn(
        self,
        host="https://example.cloud.databricks.com",
        login="cid",
        password="secret",
        http_path="/sql/1.0/warehouses/abc",
    ):
        db_conn = MagicMock()
        db_conn.host = host
        db_conn.login = login
        db_conn.password = password
        db_conn.extra_dejson = {"http_path": http_path} if http_path else {}
        return db_conn

    def _patches(self, connection):
        return (
            patch.object(databricks_utils.Variable, "get", return_value="conn-id"),
            patch.object(databricks_utils.BaseHook, "get_connection", return_value=self._db_conn()),
            patch.object(databricks_utils, "get_databricks_connection", return_value=connection),
        )

    def test_streams_partitions_over_a_single_connection(self):
        """Distinct values drive one filtered query each; all over one connection."""
        cursor = MagicMock()
        cursor.fetchall.return_value = [("CA",), ("OR",)]
        cursor.fetchmany.side_effect = [[(1,)], [], [(2,), (3,)], []]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        p_var, p_hook, p_conn = self._patches(connection)
        with p_var, p_hook, p_conn as mock_get_conn:
            batches = list(read_databricks_partitioned("SELECT a, state FROM t", "state"))

        assert batches == [[(1,)], [(2,), (3,)]]
        assert mock_get_conn.call_count == 1  # one connection for all partitions
        executed = [c.args[0] for c in cursor.execute.call_args_list]
        assert sum("DISTINCT" in q for q in executed) == 1
        assert any("'CA'" in q for q in executed)
        assert any("'OR'" in q for q in executed)
        connection.close.assert_called_once()

    def test_null_partition_value_uses_is_null(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(None,)]
        cursor.fetchmany.side_effect = [[(9,)], []]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        p_var, p_hook, p_conn = self._patches(connection)
        with p_var, p_hook, p_conn:
            list(read_databricks_partitioned("SELECT a, state FROM t", "state"))

        executed = [c.args[0] for c in cursor.execute.call_args_list]
        assert any("IS NULL" in q for q in executed)

    def test_escapes_single_quotes_in_partition_value(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("O'Brien",)]
        cursor.fetchmany.side_effect = [[(1,)], []]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        p_var, p_hook, p_conn = self._patches(connection)
        with p_var, p_hook, p_conn:
            list(read_databricks_partitioned("SELECT a, county FROM t", "county"))

        executed = [c.args[0] for c in cursor.execute.call_args_list]
        assert any("O''Brien" in q for q in executed)

    def test_connection_closed_when_consumer_stops_early(self):
        """Abandoning the generator mid-iteration still closes the connection."""
        cursor = MagicMock()
        cursor.fetchall.return_value = [("CA",), ("OR",)]
        cursor.fetchmany.side_effect = [[(1,)], [], [(2,)], []]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        p_var, p_hook, p_conn = self._patches(connection)
        with p_var, p_hook, p_conn:
            gen = read_databricks_partitioned("SELECT a, state FROM t", "state")
            next(gen)
            gen.close()

        connection.close.assert_called_once()

    def test_missing_required_field_raises_eagerly(self):
        db_conn = self._db_conn(http_path="")
        with (
            patch.object(databricks_utils.Variable, "get", return_value="conn-id"),
            patch.object(databricks_utils.BaseHook, "get_connection", return_value=db_conn),
            pytest.raises(ValueError, match="missing a required"),
        ):
            read_databricks_partitioned("SELECT a, state FROM t", "state")
