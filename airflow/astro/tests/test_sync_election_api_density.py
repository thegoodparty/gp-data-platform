"""Unit tests for the sync_election_api_density DAG declarations.

These guard the two things that make the density sync different from the
nightly one: it must not hold a foreign key (the nightly swap would drop it),
and it must not overlap the nightly DAG's table set. Mocks airflow/databricks
so the DAG files collect without the Astro runtime installed.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Stub external modules so the DAG files can be imported in any environment.
_STUBS = (
    "airflow",
    "airflow.decorators",
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
)
for _mod in _STUBS:
    sys.modules[_mod] = MagicMock()

from dags.sync_election_api import TABLES as NIGHTLY_TABLES  # noqa: E402
from dags.sync_election_api_density import (  # noqa: E402
    TABLES,
    VOTER_DENSITY_K,
    _k_anonymity_checks,
)


def test_density_tables_declare_no_foreign_keys():
    """A District FK here would not survive the nightly sync. That swap renames
    District aside and `drop_old` drops it CASCADE, which silently takes any FK
    referencing it with no error on either DAG. PK-only is the only safe shape
    for a table the nightly set does not swap alongside District."""
    for table in TABLES:
        assert table.spec.fkeys == (), table.group_id
        assert table.parents == (), table.group_id


def test_density_pks_match_the_mart_grain():
    """The marts publish one row per (district_id, resolution, h3_index) and per
    (district_id, resolution). A PK narrower than the grain fails the load on a
    duplicate; wider would let a duplicate through."""
    pks = {t.spec.target_table: t.spec.pk_columns for t in TABLES}
    assert pks == {
        "DistrictVoterDensity": ("district_id", "resolution", "h3_index"),
        "DistrictVoterDensityMeta": ("district_id", "resolution"),
    }


def test_cell_table_reads_one_state_at_a_time():
    """~55M rows. An unpartitioned read holds one server-side result set for the
    whole table, which is what OOM-kills these tasks on the A5 worker queue."""
    (cells,) = (t for t in TABLES if t.spec.target_table == "DistrictVoterDensity")
    assert cells.partition_column == "state"


def _conn_returning(value):
    """A conn whose query yields `value`, so the real check body runs."""
    cur = MagicMock()
    cur.fetchone.return_value = (value,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def test_k_anonymity_check_refuses_any_cell_below_k():
    """The suppression floor is what makes this data publishable at all, and no
    generic gate can see it: a mart regression that dropped the K filter would
    load more rows, not fewer, and sail through the count ratio."""
    spec = SimpleNamespace(staging_schema="staging", new_table="DistrictVoterDensity_new")
    _k_anonymity_checks(_conn_returning(0), spec, 55_000_000)
    with pytest.raises(ValueError, match="below K"):
        _k_anonymity_checks(_conn_returning(1), spec, 55_000_000)


def test_k_anonymity_floor_matches_the_dbt_var():
    """`voter_density_k` in dbt_project.yml is the value the marts suppress at.
    If it is raised there and not here the check silently stops biting; if it is
    lowered there this check fails the swap closed, which is the right way to
    find out."""
    assert VOTER_DENSITY_K == 10


def test_no_table_is_swapped_by_both_dags():
    """Both DAGs rename their set into public on their own schedule. A table in
    both would have two writers racing the same swap, and the loser's staging
    rename would collide with a live table it did not build."""
    nightly = {t.spec.target_table for t in NIGHTLY_TABLES}
    density = {t.spec.target_table for t in TABLES}
    assert nightly & density == set()
