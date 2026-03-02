"""Unit tests for postgres_utils shared utilities.

These tests mock all external dependencies (airflow, sshtunnel) so they
can run without the Astro runtime installed.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Add the astro directory to sys.path so `include.custom_functions` resolves
# when pytest runs from the repo root (outside the Astro runtime).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

# Stub external modules so the test file can be collected by pytest
# in any environment (the Astro runtime, the dbt poetry env, or bare).
# Using direct assignment (not setdefault) because the dbt poetry env
# ships an incompatible airflow.sdk that lacks BaseHook/Variable.
_STUBS = (
    "airflow",
    "airflow.sdk",
    "databricks",
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sdk",
    "databricks.sdk.core",
    "sshtunnel",
    "psycopg2",
    "psycopg2.extras",
)
for _mod in _STUBS:
    sys.modules[_mod] = MagicMock()

from include.custom_functions.postgres_utils import (  # noqa: E402
    get_postgres_via_ssh,
    upsert_rows,
)

# ---------------------------------------------------------------------------
# upsert_rows
# ---------------------------------------------------------------------------


class TestUpsertRows:
    """Tests for the upsert_rows helper."""

    def _make_conn(self):
        """Return a mock psycopg2 connection and its cursor."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_single_conflict_column(self):
        """ON CONFLICT clause with a single primary key."""
        conn, cursor = self._make_conn()
        rows = [(1, "a"), (2, "b")]

        with patch(
            "include.custom_functions.postgres_utils.psycopg2.extras.execute_values"
        ) as mock_ev:
            upsert_rows(
                conn=conn,
                schema="public",
                table="MyTable",
                columns=["id", "name"],
                conflict_columns=["id"],
                rows=rows,
            )

            sql_arg = mock_ev.call_args[0][1]
            assert 'ON CONFLICT ("id")' in sql_arg
            assert conn.commit.called

    def test_composite_conflict_columns(self):
        """Composite keys like (voter_id, district_id) for DistrictVoter."""
        conn, cursor = self._make_conn()
        rows = [("v1", "d1", 100)]

        with patch(
            "include.custom_functions.postgres_utils.psycopg2.extras.execute_values"
        ) as mock_ev:
            upsert_rows(
                conn=conn,
                schema="public",
                table="DistrictVoter",
                columns=["voter_id", "district_id", "score"],
                conflict_columns=["voter_id", "district_id"],
                rows=rows,
            )

            sql_arg = mock_ev.call_args[0][1]
            assert '"voter_id", "district_id"' in sql_arg
            assert "ON CONFLICT" in sql_arg

    def test_excludes_conflict_cols_from_update(self):
        """Conflict columns should NOT appear in the UPDATE SET clause."""
        conn, cursor = self._make_conn()
        rows = [("d1", "2024-01-01", 100, 50, "{}")]

        with patch(
            "include.custom_functions.postgres_utils.psycopg2.extras.execute_values"
        ) as mock_ev:
            upsert_rows(
                conn=conn,
                schema="public",
                table="DistrictStats",
                columns=[
                    "district_id",
                    "updated_at",
                    "total_constituents",
                    "total_constituents_with_cell_phone",
                    "buckets",
                ],
                conflict_columns=["district_id"],
                rows=rows,
            )

            sql_arg = mock_ev.call_args[0][1]
            # district_id should be in ON CONFLICT but not in UPDATE SET
            assert 'ON CONFLICT ("district_id")' in sql_arg
            update_part = sql_arg.split("DO UPDATE SET")[1]
            assert '"district_id"' not in update_part
            assert '"updated_at"' in update_part
            assert '"buckets"' in update_part

    def test_batching(self):
        """Rows should be split into correct batch count."""
        conn, cursor = self._make_conn()
        rows = [(i, f"name_{i}") for i in range(12)]

        with patch(
            "include.custom_functions.postgres_utils.psycopg2.extras.execute_values"
        ) as mock_ev:
            total = upsert_rows(
                conn=conn,
                schema="public",
                table="T",
                columns=["id", "name"],
                conflict_columns=["id"],
                rows=rows,
                batch_size=5,
            )

        assert total == 12
        assert mock_ev.call_count == 3  # batches of 5, 5, 2
        assert conn.commit.call_count == 3

    def test_empty_rows(self):
        """No-op on empty input."""
        conn, cursor = self._make_conn()
        total = upsert_rows(
            conn=conn,
            schema="public",
            table="T",
            columns=["id"],
            conflict_columns=["id"],
            rows=[],
        )

        assert total == 0
        assert not conn.cursor.called


# ---------------------------------------------------------------------------
# get_postgres_via_ssh context manager
# ---------------------------------------------------------------------------


class TestGetPostgresViaSsh:
    """Tests for the get_postgres_via_ssh context manager."""

    @patch("include.custom_functions.postgres_utils.psycopg2.connect")
    @patch("include.custom_functions.postgres_utils.SSHTunnelForwarder")
    @patch("include.custom_functions.postgres_utils.BaseHook.get_connection")
    def test_lifecycle(self, mock_get_conn, mock_tunnel_cls, mock_pg_connect):
        """Tunnel starts, connection opens, both close on exit."""
        bastion_conn = MagicMock(
            host="bastion.example.com", port=22, login="user", password="pw"
        )
        pg_conn = MagicMock(
            host="pg.internal",
            port=5432,
            login="pguser",
            password="pgpw",
            schema="mydb",
        )
        pg_conn.extra_dejson = {}
        mock_get_conn.side_effect = [bastion_conn, pg_conn]

        tunnel_instance = MagicMock()
        tunnel_instance.local_bind_port = 15432
        mock_tunnel_cls.return_value = tunnel_instance

        pg_connection = MagicMock()
        mock_pg_connect.return_value = pg_connection

        with get_postgres_via_ssh("gp_bastion_host", "people_api_db") as conn:
            assert conn is pg_connection
            tunnel_instance.start.assert_called_once()

        pg_connection.close.assert_called_once()
        tunnel_instance.stop.assert_called_once()

    @patch("include.custom_functions.postgres_utils.psycopg2.connect")
    @patch("include.custom_functions.postgres_utils.SSHTunnelForwarder")
    @patch("include.custom_functions.postgres_utils.BaseHook.get_connection")
    def test_cleanup_on_error(self, mock_get_conn, mock_tunnel_cls, mock_pg_connect):
        """Tunnel and connection close even when task raises."""
        bastion_conn = MagicMock(
            host="bastion.example.com", port=22, login="user", password="pw"
        )
        pg_conn = MagicMock(
            host="pg.internal",
            port=5432,
            login="pguser",
            password="pgpw",
            schema="mydb",
        )
        pg_conn.extra_dejson = {}
        mock_get_conn.side_effect = [bastion_conn, pg_conn]

        tunnel_instance = MagicMock()
        tunnel_instance.local_bind_port = 15432
        mock_tunnel_cls.return_value = tunnel_instance

        pg_connection = MagicMock()
        mock_pg_connect.return_value = pg_connection

        with pytest.raises(RuntimeError, match="task failed"):
            with get_postgres_via_ssh() as _conn:  # noqa: F841
                raise RuntimeError("task failed")

        pg_connection.close.assert_called_once()
        tunnel_instance.stop.assert_called_once()
