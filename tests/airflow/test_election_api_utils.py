"""Tests for swap_staging_into_target's resilience to target/spec drift.

The swap's archive-rename loop used to fail hard if a `spec.indexes` entry
didn't exist on the target table. That's how the DATA-1896 sync first broke
in prod: a prior swap with a narrower spec carried the new composite index
into `_old` and `drop_old` removed it, so the next swap with the wider spec
couldn't find the index to archive-rename. These tests pin the resilient
behavior so the same shape of failure can't return silently.
"""

import sys
from unittest.mock import MagicMock

# Stub airflow / databricks / psycopg2 so the module collects without the
# Astro runtime installed. Same pattern as airflow/astro/tests/test_sync_election_api.py.
for _mod in (
    "airflow",
    "airflow.sdk",
    "databricks",
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sdk",
    "databricks.sdk.core",
    "paramiko",
    "pendulum",
    "sshtunnel",
    "psycopg2",
    "psycopg2.extras",
):
    sys.modules.setdefault(_mod, MagicMock())

import pytest  # noqa: E402
from include.custom_functions.election_api_utils import (  # noqa: E402
    TableSyncSpec,
    swap_staging_into_target,
)


@pytest.fixture
def mock_conn():
    """psycopg2-style conn/cursor whose `fetchone`/`fetchall` are scriptable."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _executed_ddl(cur) -> list:
    """Return only the DDL statements (single-arg execute calls), in order."""
    return [call.args[0] for call in cur.execute.call_args_list if len(call.args) == 1]


SPEC = TableSyncSpec(
    target_table="ZipToPosition",
    indexes=(
        "ZipToPosition_zip_code_idx",
        "ZipToPosition_position_id_idx",
        "ZipToPosition_zip_code_pct_districtzip_to_zip_idx",
    ),
)


def test_swap_skips_archive_rename_for_missing_target_index(mock_conn):
    """If a spec.indexes entry is absent from the target, skip its archive
    rename instead of failing the whole swap.
    """
    conn, cur = mock_conn
    # First execute: pg_tables existence check → target exists.
    cur.fetchone.return_value = (1,)
    # Second execute: pg_indexes → only two of three spec indexes present.
    cur.fetchall.return_value = [
        ("ZipToPosition_zip_code_idx",),
        ("ZipToPosition_position_id_idx",),
    ]

    swap_staging_into_target(conn, SPEC)

    ddl = _executed_ddl(cur)
    # The missing index must not be archive-renamed.
    assert not any(
        '"ZipToPosition_zip_code_pct_districtzip_to_zip_idx"' in s
        and "RENAME TO" in s
        and '"ZipToPosition_old_zip_code_pct_districtzip_to_zip_idx"' in s
        for s in ddl
    ), "Should not emit archive-rename for a missing target index"
    # Present indexes are archive-renamed.
    assert any(
        'ALTER INDEX "public"."ZipToPosition_zip_code_idx" '
        'RENAME TO "ZipToPosition_old_zip_code_idx"' == s
        for s in ddl
    )
    assert any(
        'ALTER INDEX "public"."ZipToPosition_position_id_idx" '
        'RENAME TO "ZipToPosition_old_position_id_idx"' == s
        for s in ddl
    )
    # Staging-side restore loop still names every spec index canonically,
    # including the one that was missing from the target.
    assert any(
        'ALTER INDEX "public"."ZipToPosition_new_zip_code_pct_districtzip_to_zip_idx" '
        'RENAME TO "ZipToPosition_zip_code_pct_districtzip_to_zip_idx"' == s
        for s in ddl
    )
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()


def test_swap_archive_renames_all_present_indexes(mock_conn):
    """When every spec.indexes entry exists on the target, every one is
    archive-renamed (regression guard so the filter doesn't accidentally
    skip valid indexes).
    """
    conn, cur = mock_conn
    cur.fetchone.return_value = (1,)
    cur.fetchall.return_value = [(idx,) for idx in SPEC.indexes]

    swap_staging_into_target(conn, SPEC)

    ddl = _executed_ddl(cur)
    for idx in SPEC.indexes:
        assert any(
            f'ALTER INDEX "public"."{idx}" RENAME TO "{SPEC.archive_name(idx)}"' == s
            for s in ddl
        ), f"Expected archive-rename for {idx}"


def test_swap_skips_index_lookup_when_target_absent(mock_conn):
    """First-run path: no target table → no pg_indexes lookup, no archive
    renames. Staging table still gets promoted.
    """
    conn, cur = mock_conn
    # Target does not exist.
    cur.fetchone.return_value = None

    swap_staging_into_target(conn, SPEC)

    ddl = _executed_ddl(cur)
    assert not any(
        "RENAME TO" in s and "_old" in s for s in ddl
    ), "First-run swap must not attempt any _old renames"
    # Staging promotion still happens.
    assert any(
        'ALTER TABLE "staging"."ZipToPosition_new" SET SCHEMA "public"' == s
        for s in ddl
    )
    assert any(
        'ALTER TABLE "public"."ZipToPosition_new" RENAME TO "ZipToPosition"' == s
        for s in ddl
    )
