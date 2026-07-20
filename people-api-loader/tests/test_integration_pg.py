"""Integration test against a REAL Postgres — covers the DB-semantic surface that the
FakeConn unit tests structurally cannot: native LIST partitioning + routing, the
composite PK / unique that partitioning forces (partition key in every unique), the
explicit-column-list load contract, the pg_advisory_lock overload, and validate's
per-state GROUP BY count.

Boundary: the `copy` step's actual loader uses Aurora-only `aws_s3.table_import_from_s3`
and `create-schema` installs the `aws_s3`/`aws_commons` extensions — neither exists in
vanilla Postgres. So this test loads rows with a plain INSERT (the same explicit column
list the loader builds) rather than the aws_s3 import. The aws_s3 path remains a
staging/Aurora smoke-test concern.

Skipped unless real Postgres is available, so CI and normal `pytest` runs stay green:
  - set LOADER_TEST_PG_DSN to point at a throwaway database, OR
  - set LOADER_INTEGRATION_PG=1 with local initdb/pg_ctl on PATH (a temp cluster is
    spun up on a unix socket and torn down).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import uuid
from collections.abc import Iterator
from datetime import datetime
from types import SimpleNamespace
from typing import cast

import psycopg
import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.schema import _serving_seed_extra
from loader.people_api.schema.index_specs import IndexDef, PrimaryKey
from loader.people_api.schema.table_ddl import extract_column_names, extract_create_tables
from loader.people_api.steps import build_indexes, copy_s3, inspect_prod, validate
from loader.people_api.steps.create_schema import build_partitioned_ddl
from tests._fakes import fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))

# A small but representative Voter shape: quoted L2 columns plus the mid-table Prisma
# columns (created_at default / id PK no-default / updated_at no-default).
_DDL = (
    'CREATE TABLE public."Voter" (\n'
    '    "LALVOTERID" text NOT NULL,\n'
    '    "State" text NOT NULL,\n'
    "    created_at timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,\n"
    "    id uuid NOT NULL,\n"
    "    updated_at timestamp(3) without time zone NOT NULL,\n"
    '    "Voters_Active" text\n'
    ");"
)
_STATES = ["TX", "CA"]

# A partitioned Voter carrying the "FirstName"/"LastName" columns the CRM name-search extras
# index. Separate from _DDL (which omits them) so the extras test can drive the real committed
# EXTRA_INDEXES SQL through the partitioned build path against a table that actually has the
# indexed columns.
_DDL_NAMES = (
    'CREATE TABLE public."Voter" (\n'
    '    "id" uuid NOT NULL,\n'
    '    "State" text NOT NULL,\n'
    '    "FirstName" text,\n'
    '    "LastName" text\n'
    ");"
)


def _exec(cur: psycopg.Cursor, sql: str, params: object = None) -> None:
    """Execute dynamic SQL (test drives raw strings; same ignore the prod code uses)."""
    cur.execute(sql, params)  # ty: ignore[invalid-argument-type]


def _explain(cur: psycopg.Cursor, sql: str) -> str:
    """Return the full EXPLAIN plan text for a query (all rows joined)."""
    cur.execute("EXPLAIN " + sql)  # ty: ignore[no-matching-overload]
    return "\n".join(row[0] for row in cur.fetchall())


def _scalar(cur: psycopg.Cursor, sql: str) -> object:
    """Run a single-value query and return the first column of the one row."""
    cur.execute(sql)  # ty: ignore[no-matching-overload]
    row = cur.fetchone()
    assert row is not None
    return row[0]


@pytest.fixture(scope="module")
def pg_conn() -> Iterator[psycopg.Connection]:
    dsn = os.environ.get("LOADER_TEST_PG_DSN")
    if dsn:
        conn = psycopg.connect(dsn, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
        return

    if os.environ.get("LOADER_INTEGRATION_PG") != "1":
        pytest.skip("integration: set LOADER_INTEGRATION_PG=1 (local initdb/pg_ctl) or LOADER_TEST_PG_DSN")
    initdb, pg_ctl = shutil.which("initdb"), shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("integration: no local postgres binaries (initdb/pg_ctl) on PATH")

    tmp = tempfile.mkdtemp(prefix="loader_pg_")
    datadir, sock = os.path.join(tmp, "data"), os.path.join(tmp, "sock")
    logfile = os.path.join(tmp, "pg.log")
    os.makedirs(sock)
    try:
        subprocess.run(
            [initdb, "-D", datadir, "-U", "postgres", "--auth=trust", "-E", "UTF8"],
            check=True,
            capture_output=True,
        )
        # -l redirects the postmaster's output to a file; without it the daemon inherits
        # this subprocess's stdout pipe and communicate() blocks forever on EOF.
        # Socket-only (listen_addresses empty) so we never collide with a local PG on 5432.
        subprocess.run(
            [pg_ctl, "-D", datadir, "-l", logfile, "-o", f"-k {sock} -c listen_addresses=", "-w", "start"],
            check=True,
            capture_output=True,
        )
        conn = psycopg.connect(host=sock, dbname="postgres", user="postgres", autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
    except subprocess.CalledProcessError as e:  # surface initdb/pg_ctl failure (we opted in)
        pytest.fail(f"postgres setup failed: {e.stderr.decode(errors='replace')}")
    finally:
        subprocess.run([pg_ctl, "-D", datadir, "-w", "stop"], capture_output=True)
        shutil.rmtree(tmp, ignore_errors=True)


def _row(lalvoterid: str, state: str, active: str = "Y") -> tuple:
    # Order matches extract_column_names(_DDL): LALVOTERID, State, created_at, id, updated_at, Voters_Active
    ts = datetime(2020, 1, 1)
    return (lalvoterid, state, ts, str(uuid.uuid4()), ts, active)


def test_partitioned_lifecycle(pg_conn: psycopg.Connection, monkeypatch: pytest.MonkeyPatch) -> None:
    create_sql = extract_create_tables(_DDL)["Voter"]

    # 1. create-schema: partitioned parent + per-state children apply, and partitions attach.
    parent, children = build_partitioned_ddl(create_sql, "Voter", "State", _STATES)
    with pg_conn.cursor() as cur:
        _exec(cur, 'DROP TABLE IF EXISTS public."Voter" CASCADE')  # idempotent vs a reused DB
        _exec(cur, parent)
        for child in children:
            _exec(cur, child)
        assert (
            _scalar(cur, "SELECT count(*) FROM pg_inherits WHERE inhparent='public.\"Voter\"'::regclass") == 2
        )

    # 2. load via the explicit column list (the contract), and confirm State routing.
    column_list = ", ".join(f'"{c}"' for c in extract_column_names(create_sql))
    insert = f'INSERT INTO public."Voter" ({column_list}) VALUES (%s, %s, %s, %s, %s, %s)'
    with pg_conn.cursor() as cur:
        for i in range(5):
            _exec(cur, insert, _row(f"L-TX-{i}", "TX"))
        for i in range(3):
            _exec(cur, insert, _row(f"L-CA-{i}", "CA"))
        assert _scalar(cur, 'SELECT count(*) FROM ONLY public."Voter_TX"') == 5  # routed to TX partition
        assert _scalar(cur, 'SELECT count(*) FROM ONLY public."Voter_CA"') == 3

    # 2b. a state with no partition (no DEFAULT partition) must be rejected, not silently dropped.
    with pg_conn.cursor() as cur, pytest.raises(psycopg.errors.CheckViolation):
        _exec(cur, insert, _row("L-ZZ-0", "ZZ"))

    # 3. build-indexes: the composite PK (id, State) and unique (LALVOTERID, State) must apply
    #    on the partitioned table — this is the partition-key-in-constraint requirement that
    #    only a real PG enforces. The builder helpers now take a live connection directly.
    build_indexes._add_primary_key(
        pg_conn, PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])
    )
    build_indexes._create_index(
        pg_conn,
        IndexDef(
            table="Voter",
            name="Voter_LALVOTERID_key",
            sql="",
            unique=True,
            columns=["LALVOTERID"],
            where=None,
        ),
        partition_key="State",  # Voter is partitioned -> unique is (LALVOTERID, State)
    )
    with pg_conn.cursor() as cur:
        pk = _scalar(
            cur,
            "SELECT count(*) FROM pg_constraint WHERE conrelid='public.\"Voter\"'::regclass AND contype='p'",
        )
        assert pk == 1  # composite PK created
        uq = _scalar(
            cur,
            "SELECT count(*) FROM pg_indexes WHERE tablename='Voter' AND indexname='Voter_LALVOTERID_key'",
        )
        assert uq == 1  # unique index created

    # 3b. the unique is on (LALVOTERID, State): same LALVOTERID in a different state is allowed,
    #     a true duplicate (same LALVOTERID + State) is rejected.
    with pg_conn.cursor() as cur:
        _exec(cur, insert, _row("L-TX-0", "CA"))  # same LALVOTERID as a TX row, different state -> OK
    with pg_conn.cursor() as cur, pytest.raises(psycopg.errors.UniqueViolation):
        _exec(cur, insert, _row("L-TX-0", "TX"))  # duplicate within TX -> rejected

    # 4. advisory lock: the exact SQL the loader runs must resolve to a valid overload.
    #    Without the ::int4 cast this raises UndefinedFunction against real PG.
    with pg_conn.cursor() as cur:
        copy_s3._acquire_unit_lock(cur, "Voter", "TX")

    # 5. validate: the per-state GROUP BY count runs against the real partitioned table.
    fc = fake_connect(pg_conn)
    monkeypatch.setattr(validate, "connect_new", fc)
    counts = validate._new_counts_by_state(_CFG, "20260609", "Voter")
    assert counts == {"TX": 5, "CA": 4}  # TX=5, CA=4 (3 + the cross-state row added in 3b)
    assert validate._compare_counts("prod_row_counts", counts, {"TX": 5, "CA": 4}).passed is True

    # 6. inspect-prod: real information_schema column detection + per-state GROUP BY.
    #    Voter has "State" + "updated_at" -> per-state counts and snapshot dates.
    with pg_conn.cursor() as cur:
        voter_ti = inspect_prod._inspect_table(cur, "Voter")
    assert voter_ti.total_row_count == 9  # 5 TX + 4 CA
    assert voter_ti.per_state_row_counts == {"TX": 5, "CA": 4}
    assert set(voter_ti.per_state_snapshot_dates) == {"TX", "CA"}

    #    A table without a "State" column -> total count only, no per-state breakdown.
    with pg_conn.cursor() as cur:
        _exec(cur, 'CREATE TABLE public."NoStateTbl" (k int)')
        _exec(cur, 'INSERT INTO public."NoStateTbl" (k) VALUES (1), (2)')
        nostate_ti = inspect_prod._inspect_table(cur, "NoStateTbl")
    assert nostate_ti.total_row_count == 2
    assert nostate_ti.per_state_row_counts == {}


def _name_search_sql(pattern: str) -> str:
    """people-api's exact name-search predicate shape (buildVoterWhereSql.utils.ts): an OR of
    lower() LIKE on both name columns, with LIKE metacharacters escaped via ESCAPE '\\'. The
    caller passes a fully-formed pattern ('%tok%' for substring, 'tok%' for anchored prefix)."""
    esc = "ESCAPE '\\'"
    # SELECT * (not a single indexed column): production selects the full voter row, so there is
    # no covering-index-only shortcut — the planner must locate rows via a name index and heap-fetch.
    return (
        'SELECT * FROM public."Voter" v WHERE '
        f"(lower(v.\"FirstName\") LIKE '{pattern}' {esc} OR lower(v.\"LastName\") LIKE '{pattern}' {esc})"
    )


def test_extra_name_search_indexes_build_and_are_planner_usable(pg_conn: psycopg.Connection) -> None:
    """Every committed name-search extra builds through the real partitioned parent-only +
    per-partition child + ATTACH path, and the trigram GIN indexes serve people-api's actual
    name-search predicates on public."Voter" (the schema the loader targets).

    This is the DB-semantic coverage the FakeConn unit tests structurally cannot give: the build
    path is a string rewrite of the seed SQL (build_indexes._plain_parent_only_sql /
    _plain_child_sql), and only a real Postgres proves that (a) pg_trgm + gin_trgm_ops is installed
    and valid, (b) a GIN/expression/multicolumn index attaches to a partitioned parent, and (c) —
    the point of the indexes — the stored expression matches people-api's lower(col) LIKE predicate
    so the query is index-served instead of silently falling back to a seq scan.

    Locale note: a plain lower() b-tree cannot serve LIKE-'prefix%' outside a C-locale DB, and the
    serving cluster is en_US.UTF-8 — so the seed's lower() b-trees carry the text_pattern_ops
    opclass, which makes the anchored-prefix path (lower(col) LIKE 'tok%') use the b-tree here. The
    trigram GIN serves the substring path (lower(col) LIKE '%tok%'), which no b-tree can. This test
    asserts each path lands on the index type intended for it.
    """
    create_sql = extract_create_tables(_DDL_NAMES)["Voter"]
    parent, children = build_partitioned_ddl(create_sql, "Voter", "State", _STATES)
    with pg_conn.cursor() as cur:
        _exec(cur, 'DROP TABLE IF EXISTS public."Voter" CASCADE')  # idempotent vs a reused DB
        _exec(cur, "CREATE EXTENSION IF NOT EXISTS pg_trgm")  # what build_indexes installs
        _exec(cur, parent)
        for child in children:
            _exec(cur, child)
        insert = 'INSERT INTO public."Voter" ("id", "State", "FirstName", "LastName") VALUES (%s, %s, %s, %s)'
        # Enough varied rows per partition that the planner clearly prefers a selective name index
        # over scanning a tiny table/index — so the plan assertions below are stable, not artifacts
        # of a 5-row relation. Names include "oh"/"jo" matches so the predicates are non-empty.
        names = ["John", "Johnny", "Jane", "Bob", "Bobby", "Smith", "Jones", "Adams", "Cohen", "Baker"]
        for i in range(300):
            state = _STATES[i % len(_STATES)]
            first, last = names[i % len(names)], names[(i * 3) % len(names)]
            _exec(cur, insert, (str(uuid.uuid4()), state, first, last))
        _exec(cur, 'ANALYZE public."Voter"')

    # Drive ALL committed extras (2 trigram GIN, 2 lower() b-tree, 1 multicolumn b-tree) through
    # the exact path build_indexes.run uses for a partitioned plain index: the empty parent
    # (ON ONLY), then a child per state, each attached to the parent index. Building every one
    # proves each real committed SQL string rewrites and attaches correctly.
    extras = [i for i in _serving_seed_extra.EXTRA_INDEXES if i.table == "Voter"]
    assert len(extras) == 5  # guard: all five reach the build path
    for idx in extras:
        build_indexes._create_plain_parent_only(pg_conn, idx)
        for state in _STATES:
            build_indexes._build_and_attach_child(pg_conn, (idx, state))

    with pg_conn.cursor() as cur:
        for idx in extras:
            # Postgres only flips a partitioned index's indisvalid to true once a matching child
            # is attached for EVERY partition, so this asserts all attaches actually ran.
            valid = _scalar(
                cur,
                "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                f"WHERE c.relname = '{idx.name}'",
            )
            assert valid is True, f"{idx.name}: parent index not valid (a child attach is missing)"
            attached = _scalar(
                cur,
                "SELECT count(*) FROM pg_inherits ih JOIN pg_class c ON c.oid = ih.inhparent "
                f"WHERE c.relname = '{idx.name}'",
            )
            want = len(_STATES)
            assert attached == want, f"{idx.name}: {attached} children attached, want {want}"

    # The point of the indexes: the planner must index-serve people-api's real name-search
    # predicates. Forcing seqscan off makes the plan reveal whether an index is even usable for
    # the query shape — a mismatched index expression would leave only a seq scan available.
    with pg_conn.cursor() as cur:
        _exec(cur, "SET enable_seqscan = off")
        try:
            # substring (CRM typeahead): only the trigram GIN can serve lower(col) LIKE '%tok%',
            # so both name trigram indexes must appear (BitmapOr across the FirstName/LastName OR).
            sub_plan = _explain(cur, _name_search_sql("%oh%"))
            assert "Voter_firstname_lower_trgm_idx" in sub_plan, sub_plan
            assert "Voter_lastname_lower_trgm_idx" in sub_plan, sub_plan
            # anchored prefix: the text_pattern_ops lower() b-trees serve lower(col) LIKE 'tok%'
            # (a default-opclass b-tree could NOT in this en_US.UTF-8 cluster). Both name b-trees
            # appear via the BitmapOr across the FirstName/LastName OR; no seq scan. This proves the
            # text_pattern_ops opclass survived the seed -> partitioned build path end to end.
            pre_plan = _explain(cur, _name_search_sql("jo%"))
            assert "Seq Scan" not in pre_plan, pre_plan
            assert "Voter_firstname_lower_idx" in pre_plan, pre_plan
            assert "Voter_lastname_lower_idx" in pre_plan, pre_plan
        finally:
            _exec(cur, "SET enable_seqscan = on")
