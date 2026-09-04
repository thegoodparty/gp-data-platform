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
    _district_reference_checks,
    _k_anonymity_checks,
)


def test_density_tables_declare_no_foreign_keys():
    """A District FK here does not survive the nightly sync. Verified against
    real Postgres: that swap renames District aside, which leaves this FK
    pointing at the stale `District_old`, and `drop_old` then drops it CASCADE
    with only a NOTICE. Rows survive and orphans become insertable. The
    referential guarantee is delivered by `_district_reference_checks` at load
    time instead, which is the moment the FK was doing real work anyway."""
    for table in TABLES:
        assert table.spec.fkeys == (), table.group_id
        assert table.parents == (), table.group_id


def _spec():
    return SimpleNamespace(
        staging_schema="staging",
        new_table="District_Voter_Density_new",
        target_schema="public",
    )


def _conn_returning_counts(staged_districts, orphan_districts):
    cur = MagicMock()
    cur.fetchone.side_effect = [(staged_districts,), (orphan_districts,)]
    cur.rowcount = orphan_districts
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_a_few_unlanded_districts_are_pruned_not_failed():
    """A district can reach the density mart before the nightly sync lands its
    District row. The handoff doc is explicit that skipping those rows is
    correct and failing the whole load on them is not: the next run re-offers
    them, and the other half-million districts still get fresh data."""
    conn, cur = _conn_returning_counts(500_000, 12)

    _district_reference_checks(conn, _spec(), 55_000_000)

    assert any("DELETE" in c.args[0] for c in cur.execute.call_args_list)


def test_no_delete_is_issued_when_every_district_landed():
    """The expected case is zero orphans, and it should not pay for a 55M-row
    anti-join delete to discover that."""
    conn, cur = _conn_returning_counts(500_000, 0)

    _district_reference_checks(conn, _spec(), 55_000_000)

    assert not any("DELETE" in c.args[0] for c in cur.execute.call_args_list)


def test_wholesale_divergence_fails_the_load():
    """Pruning is right for a timing gap, wrong for a mart that has started
    minting its own district ids. Past the threshold the rows are not late,
    the key is broken, and quietly dropping most of the map would hide it."""
    conn, _ = _conn_returning_counts(500_000, 50_000)

    with pytest.raises(ValueError, match="no matching District"):
        _district_reference_checks(conn, _spec(), 55_000_000)


def test_every_density_table_checks_its_district_references():
    """Both tables key on district_id, so both can orphan. The cells table also
    carries the K floor; neither check should displace the other."""
    for table in TABLES:
        assert table.extra_checks is not None, table.group_id


def test_target_tables_match_the_prisma_table_names():
    """election-api's models carry @@map("District_Voter_Density"), so the
    Postgres tables are underscore-separated, not the Prisma model names.
    `build_staging` clones `LIKE` the live table, so a wrong name here fails
    the first task of every run on a missing relation."""
    assert {t.spec.target_table for t in TABLES} == {
        "District_Voter_Density",
        "District_Voter_Density_Meta",
    }


def test_density_pks_match_the_mart_grain():
    """The marts publish one row per (district_id, resolution, h3_index) and per
    (district_id, resolution). A PK narrower than the grain fails the load on a
    duplicate; wider would let a duplicate through."""
    pks = {t.spec.target_table: t.spec.pk_columns for t in TABLES}
    assert pks == {
        "District_Voter_Density": ("district_id", "resolution", "h3_index"),
        "District_Voter_Density_Meta": ("district_id", "resolution"),
    }


def test_declared_indexes_match_the_prisma_migration():
    """The staging clone is `LIKE ... INCLUDING DEFAULTS`, which copies no
    indexes, so this tuple is the complete set the live table has after a swap.
    An index Prisma declares but this omits is dropped by the first sync and
    then reads as schema drift."""
    indexes = {t.spec.target_table: t.spec.index_names for t in TABLES}
    assert indexes == {
        "District_Voter_Density": ("District_Voter_Density_district_id_resolution_idx",),
        "District_Voter_Density_Meta": (),
    }


def test_cell_table_reads_one_state_at_a_time():
    """~55M rows. An unpartitioned read holds one server-side result set for the
    whole table, which is what OOM-kills these tasks on the A5 worker queue."""
    (cells,) = (t for t in TABLES if t.spec.target_table == "District_Voter_Density")
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
