"""Unit tests for databricks_utils connection handling.

These tests mock all external dependencies (airflow, databricks) so they
can run without the Astro runtime installed.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Add the astro directory to sys.path so `include.custom_functions` resolves
# when pytest runs from the repo root (outside the Astro runtime).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

# Stub external modules so the test file can be collected by pytest in any
# environment (the Astro runtime, the dbt poetry env, or bare). Direct
# assignment (not setdefault) because the dbt poetry env ships an incompatible
# airflow.sdk that lacks BaseHook/Variable.
_STUBS = (
    "airflow",
    "airflow.sdk",
    "databricks",
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sdk",
    "databricks.sdk.core",
)
for _mod in _STUBS:
    sys.modules[_mod] = MagicMock()

from include.custom_functions import databricks_utils  # noqa: E402
from include.custom_functions.databricks_utils import (  # noqa: E402
    _is_non_retryable_auth_error,
    get_databricks_connection,
)


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
        with patch.object(
            databricks_utils.databricks_sql, "connect", side_effect=auth_error
        ) as mock_connect, patch.object(databricks_utils.time, "sleep") as mock_sleep:
            with pytest.raises(Exception, match="invalid_client"):
                get_databricks_connection(**self._kwargs(max_retries=20))

        # One attempt, no retry sleeps — the whole point of the change.
        assert mock_connect.call_count == 1
        mock_sleep.assert_not_called()

    def test_retries_transient_errors_then_succeeds(self):
        """A transient error is retried and the eventual connection returned."""
        conn = MagicMock()
        with patch.object(
            databricks_utils.databricks_sql,
            "connect",
            side_effect=[Exception("warehouse may be starting"), conn],
        ) as mock_connect, patch.object(databricks_utils.time, "sleep") as mock_sleep:
            result = get_databricks_connection(**self._kwargs(max_retries=5))

        assert result is conn
        assert mock_connect.call_count == 2
        mock_sleep.assert_called_once()

    def test_raises_after_exhausting_retries_on_transient_errors(self):
        """Persistent transient errors raise after exhausting max_retries."""
        with patch.object(
            databricks_utils.databricks_sql,
            "connect",
            side_effect=Exception("warehouse may be starting"),
        ) as mock_connect, patch.object(databricks_utils.time, "sleep"):
            with pytest.raises(Exception, match="warehouse"):
                get_databricks_connection(**self._kwargs(max_retries=3))

        assert mock_connect.call_count == 3
