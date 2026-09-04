"""Unit tests for the sync_election_api DAG declarations.

The declaration-consistency test guards the cross-group FK wiring: a
MartSync whose fkeys and parents disagree would break the staging FK build
in production. Mocks airflow/databricks/etc. so the file collects without
the Astro runtime installed.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Captured before the stubbing below poisons sys.modules, so the array
# adaptation test exercises the real adapter regardless of collection order.
from psycopg2.extensions import adapt as psycopg2_adapt

# Stub external modules so the DAG file can be imported in any environment.
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

from dags.sync_election_api import (  # noqa: E402
    TABLES,
    _district_extra_checks,
    _pt_extra_checks,
    _ztp_extra_checks,
)
from include.custom_functions.election_api_utils import TableSyncSpec  # noqa: E402


def test_parents_match_fkey_references():
    """Every non-self FK must have its referenced table's group in `parents`
    (and vice versa): a missing edge can build a staging FK before its
    referenced staging table is loaded; a stale edge points at a group that
    no longer exists."""
    table_to_group = {t.spec.target_table: t.group_id for t in TABLES}
    for t in TABLES:
        referenced_groups = {
            table_to_group[fk.ref_table] for fk in t.spec.fkeys if fk.ref_table != t.spec.target_table
        }
        assert referenced_groups == set(t.parents), t.group_id


def _conn_returning(value):
    """A conn whose query yields `value`. Each extra check reads one scalar, so
    this drives the real function body, not a reimplementation of its decision."""
    cur = MagicMock()
    cur.fetchone.return_value = (value,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _spec(table):
    return SimpleNamespace(staging_schema="staging", new_table=f"{table}_new")


def test_pt_extra_checks_refuse_duplicate_keys():
    """The election-api consumer does not disambiguate model_version, so a
    duplicate (district_id, election_year, election_code) key would make it
    serve an arbitrary row. Row counts and NULL probes cannot see this."""
    _pt_extra_checks(_conn_returning(0), _spec("Projected_Turnout"), 1_000_000)
    with pytest.raises(ValueError, match="duplicate"):
        _pt_extra_checks(_conn_returning(1), _spec("Projected_Turnout"), 1_000_000)


def test_ztp_extra_checks_refuse_partial_state_coverage():
    """A load that silently drops whole states can still clear the row-count
    ratio, so coverage is checked separately."""
    _ztp_extra_checks(_conn_returning(51), _spec("ZipToPosition"), 1_300_000)
    with pytest.raises(ValueError, match="distinct states"):
        _ztp_extra_checks(_conn_returning(29), _spec("ZipToPosition"), 1_300_000)


def _conn_returning_sequence(values):
    """A conn whose successive queries yield `values`, one scalar each."""
    cur = MagicMock()
    cur.fetchone.side_effect = [(v,) for v in values]
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class TestDistrictDensityReferences:
    """The density tables key on District.id but hold no FK — an FK here cannot
    survive this DAG's own swap. This is the other half of that replacement:
    the density DAG checks the relationship when it loads, monthly, and this
    checks it on the ~30 nightly District swaps in between, which is the only
    other thing that can break it."""

    def test_skipped_before_the_density_tables_are_deployed(self):
        """election-api owns that DDL and deploys on its own schedule, so this
        must be a no-op until the tables exist rather than failing every
        nightly run until then."""
        # Neither table exists.
        conn = _conn_returning_sequence([None, None])
        _district_extra_checks(conn, TableSyncSpec(target_table="District"), 500_000)

    def test_pre_existing_orphans_do_not_block_the_swap(self):
        """Density is a monthly vintage against a nightly District, so rows
        already orphaned are expected staleness, not damage this swap is doing.
        Blocking on them would wedge the nightly sync until the next monthly
        density load, inverting which of the two is authoritative."""
        # Per table: regclass, density districts, newly orphaned by THIS swap.
        conn = _conn_returning_sequence(["t", 500_000, 0, "t", 500_000, 0])
        _district_extra_checks(conn, TableSyncSpec(target_table="District"), 500_000)

    def test_refuses_a_swap_that_newly_orphans_much_of_the_map(self):
        """A District vintage that drops districts the live map depends on
        passes the 0.90 id-overlap gate — that gate compares District to
        District and cannot see the density tables at all."""
        conn = _conn_returning_sequence(["t", 500_000, 25_000])
        with pytest.raises(ValueError, match="newly orphan"):
            _district_extra_checks(conn, TableSyncSpec(target_table="District"), 500_000)


def test_psycopg2_adapts_python_lists_to_postgres_arrays():
    """Array round-trips (Race frequency int[], Candidacy urls text[]): the
    arrow-backed connector returns numpy arrays, the loader normalizes them
    to Python lists, and those lists must adapt to ARRAY literals."""
    assert psycopg2_adapt([1, 2]).getquoted() == b"ARRAY[1,2]"
    assert psycopg2_adapt(["a", "b"]).getquoted() == b"ARRAY['a','b']"
