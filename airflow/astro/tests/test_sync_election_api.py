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
    VOTER_DENSITY_K,
    _dvd_extra_checks,
    _prune_unlanded_districts,
    _pt_extra_checks,
    _ztp_extra_checks,
)


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


class TestVoterDensity:
    """The density tables live in this DAG specifically so their FK to District
    can survive. An FK from outside the set points at District_old after the
    rename and is dropped by drop_old's CASCADE with only a NOTICE."""

    def _density(self):
        return [t for t in TABLES if t.spec.target_table.startswith("District_Voter_Density")]

    def test_both_density_tables_are_in_this_swap_set(self):
        """Moving either one out silently un-enforces its FK rather than
        failing, which is the whole reason they are here."""
        assert {t.spec.target_table for t in self._density()} == {
            "District_Voter_Density",
            "District_Voter_Density_Meta",
        }

    def test_each_holds_an_fk_to_district(self):
        for t in self._density():
            assert [fk.ref_table for fk in t.spec.fkeys] == ["District"], t.group_id

    def test_target_tables_match_the_prisma_table_names(self):
        """election-api @@maps these models to underscore-separated tables, and
        build_staging clones LIKE the live table — the model names would fail
        the first task of every run on a missing relation."""
        assert all("_Voter_Density" in t.spec.target_table for t in self._density())

    def test_declared_indexes_match_the_prisma_migration(self):
        """The staging clone copies no indexes, so this is the complete set the
        live table has after a swap. One Prisma declares and this omits is
        dropped by the first sync and then reads as drift."""
        indexes = {t.spec.target_table: t.spec.index_names for t in self._density()}
        assert indexes == {
            "District_Voter_Density": ("District_Voter_Density_district_id_resolution_idx",),
            "District_Voter_Density_Meta": (),
        }

    def test_both_prune_before_their_fk_is_added(self):
        """The FK add is the point of no return: a stale reference there fails
        the whole 15-table swap, not just density."""
        for t in self._density():
            assert t.pre_index is _prune_unlanded_districts, t.group_id

    def test_k_anonymity_floor_matches_the_dbt_var(self):
        """`voter_density_k` in dbt_project.yml is what the marts suppress at.
        Raised there and not here, this check stops biting; lowered there, it
        fails the swap closed, which is the right way to find out."""
        assert VOTER_DENSITY_K == 10

    def test_cells_below_k_refuse_the_swap(self):
        """No generic gate can see this: dropping the suppression filter
        upstream loads MORE rows and sails through the count ratio."""
        _dvd_extra_checks(_conn_returning(0), _spec("District_Voter_Density"), 59_000_000)
        with pytest.raises(ValueError, match="below K"):
            _dvd_extra_checks(_conn_returning(1), _spec("District_Voter_Density"), 59_000_000)


def test_psycopg2_adapts_python_lists_to_postgres_arrays():
    """Array round-trips (Race frequency int[], Candidacy urls text[]): the
    arrow-backed connector returns numpy arrays, the loader normalizes them
    to Python lists, and those lists must adapt to ARRAY literals."""
    assert psycopg2_adapt([1, 2]).getquoted() == b"ARRAY[1,2]"
    assert psycopg2_adapt(["a", "b"]).getquoted() == b"ARRAY['a','b']"
