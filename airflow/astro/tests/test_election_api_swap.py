"""Crash-recovery tests for the shared election-api build-and-swap lifecycle.

``swap_staging_into_target`` renames every live table aside (``<Table>_old``)
and the staging set in, all in one transaction; ``drop_old_tables`` runs
later as a separate task/transaction. The dangerous window: a crash after
the swap commits but before drop_old completes leaves ``_old`` tables
behind. Without the in-transaction pre-drop, the next run's swap then fails
on the rename collision — run after run, until a human drops the leftovers —
and five consecutive failed runs auto-pause the whole DAG. These tests prove
the lifecycle self-heals from a crash at every point in the sequence, and
that cross-table and self-referencing staging FKs ride the renames into
their canonical shape.

FakePostgres models just enough real Postgres semantics for the proofs to be
honest: transactional DDL (rollback restores everything, including a
rolled-back pre-drop), duplicate-name errors on CREATE/RENAME targets,
DROP TABLE rejecting inbound dependents unless CASCADE (which drops the
dependent constraints), and SET SCHEMA moving a table's indexes along with
it. It parses the exact closed set of statements the lifecycle emits and
fails loudly on anything it does not recognize, so drift between the fake
and the real SQL shows up as a test error rather than a silent pass.
"""

import re
from copy import deepcopy

import pytest
from include.custom_functions.election_api_utils import (
    ForeignKey,
    Index,
    TableSyncSpec,
    apply_ddl,
    create_staging_table,
    drop_old_tables,
    swap_staging_into_target,
)


class FakePostgresError(Exception):
    """Stand-in for psycopg2 errors (duplicate/missing relations)."""


class FakePostgres:
    """In-memory model of the Postgres objects the swap lifecycle touches.

    State:
      - ``tables``: (schema, table) -> set of constraint names on the table.
      - ``indexes``: (schema, index_name) -> (schema, table) owning it.
        Index names are schema-scoped (as in Postgres); constraint names are
        table-scoped.
      - ``fk_refs``: (child_schema, child_table, constraint_name) -> the
        (ref_schema, ref_table) the constraint points at.
      - ``statements``: every statement executed, normalized, in order (for
        order assertions).

    ``arm_crash(after=k)`` makes the k-th subsequent statement raise before
    executing, simulating a worker crash at that exact point; the lifecycle's
    error handling then rolls the transaction back.
    """

    def __init__(self):
        self.tables: dict[tuple[str, str], set[str]] = {}
        self.indexes: dict[tuple[str, str], tuple[str, str]] = {}
        # (child_schema, child_table, constraint_name) -> (ref_schema, ref_table)
        self.fk_refs: dict[tuple[str, str, str], tuple[str, str]] = {}
        # every statement executed, normalized, in order (for order asserts)
        self.statements: list[str] = []
        self._durable = self._copy_state()
        self._crash_countdown: int | None = None
        self.crash_fired = False

    # -- setup / inspection -------------------------------------------------

    def seed_table(self, schema, table, constraints=(), indexes=(), fk_refs=None):
        """Create a table durably, outside any transaction (test setup)."""
        self.tables[(schema, table)] = set(constraints)
        for idx in indexes:
            self.indexes[(schema, idx)] = (schema, table)
        if fk_refs:
            self.fk_refs.update(fk_refs)
        self._durable = self._copy_state()

    def connect(self):
        return _FakeConnection(self)

    def has_table(self, schema, table):
        return (schema, table) in self.tables

    def constraints(self, schema, table):
        return set(self.tables[(schema, table)])

    def index_names(self, schema):
        return {idx for (s, idx) in self.indexes if s == schema}

    def state(self):
        return self._copy_state()

    def _copy_state(self):
        return (deepcopy(self.tables), deepcopy(self.indexes), deepcopy(self.fk_refs))

    # -- crash injection ----------------------------------------------------

    def arm_crash(self, after: int):
        """Raise on the ``after``-th subsequent statement, before executing it."""
        self._crash_countdown = after
        self.crash_fired = False

    # -- transaction control ------------------------------------------------

    def _commit(self):
        self._durable = self._copy_state()

    def _rollback(self):
        self.tables, self.indexes, self.fk_refs = deepcopy(self._durable)

    # -- statement execution --------------------------------------------------

    def _execute(self, sql, params=None):
        if self._crash_countdown is not None:
            self._crash_countdown -= 1
            if self._crash_countdown <= 0:
                self._crash_countdown = None
                self.crash_fired = True
                raise RuntimeError("simulated crash")

        stmt = " ".join(sql.split())
        self.statements.append(stmt)

        if stmt == "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s":
            schema, table = params
            return [(1,)] if self.has_table(schema, table) else []

        m = re.fullmatch(r'DROP TABLE IF EXISTS "([^"]+)"\."([^"]+)"( CASCADE)?', stmt)
        if m:
            self._drop_table(m.group(1), m.group(2), missing_ok=True, cascade=bool(m.group(3)))
            return []

        m = re.fullmatch(
            r'CREATE TABLE "([^"]+)"\."([^"]+)" \(LIKE "([^"]+)"\."([^"]+)" INCLUDING DEFAULTS\)',
            stmt,
        )
        if m:
            schema, table, like_schema, like_table = m.groups()
            self._require_table(like_schema, like_table)
            self._require_no_table(schema, table)
            # LIKE without INCLUDING INDEXES copies no indexes/constraints.
            self.tables[(schema, table)] = set()
            return []

        m = re.fullmatch(r'ALTER TABLE "([^"]+)"\."([^"]+)" RENAME TO "([^"]+)"', stmt)
        if m:
            schema, table, new_name = m.groups()
            self._require_table(schema, table)
            self._require_no_table(schema, new_name)
            self.tables[(schema, new_name)] = self.tables.pop((schema, table))
            self._reown_indexes((schema, table), (schema, new_name))
            new_refs = {}
            for (c_schema, c_table, con), (r_schema, r_table) in self.fk_refs.items():
                if (c_schema, c_table) == (schema, table):
                    c_table = new_name
                if (r_schema, r_table) == (schema, table):
                    r_schema, r_table = schema, new_name
                new_refs[(c_schema, c_table, con)] = (r_schema, r_table)
            self.fk_refs = new_refs
            return []

        m = re.fullmatch(r'ALTER TABLE "([^"]+)"\."([^"]+)" SET SCHEMA "([^"]+)"', stmt)
        if m:
            schema, table, new_schema = m.groups()
            self._require_table(schema, table)
            self._require_no_table(new_schema, table)
            self.tables[(new_schema, table)] = self.tables.pop((schema, table))
            # A table's indexes move to the new schema with it.
            for (idx_schema, idx), owner in list(self.indexes.items()):
                if owner == (schema, table):
                    if (new_schema, idx) in self.indexes:
                        raise FakePostgresError(f'index "{idx}" already exists in schema "{new_schema}"')
                    del self.indexes[(idx_schema, idx)]
                    self.indexes[(new_schema, idx)] = (new_schema, table)
            new_refs = {}
            for (c_schema, c_table, con), (r_schema, r_table) in self.fk_refs.items():
                if (c_schema, c_table) == (schema, table):
                    c_schema = new_schema
                if (r_schema, r_table) == (schema, table):
                    r_schema, r_table = new_schema, table
                new_refs[(c_schema, c_table, con)] = (r_schema, r_table)
            self.fk_refs = new_refs
            return []

        m = re.fullmatch(r'ALTER INDEX "([^"]+)"\."([^"]+)" RENAME TO "([^"]+)"', stmt)
        if m:
            schema, idx, new_idx = m.groups()
            if (schema, idx) not in self.indexes:
                raise FakePostgresError(f'index "{schema}"."{idx}" does not exist')
            if (schema, new_idx) in self.indexes:
                raise FakePostgresError(f'index "{new_idx}" already exists')
            owner = self.indexes.pop((schema, idx))
            self.indexes[(schema, new_idx)] = owner
            # Renaming an index that backs a constraint (e.g. a PK) renames
            # the constraint with it — Postgres keeps the two names equal.
            owner_cons = self.tables[owner]
            if idx in owner_cons:
                owner_cons.remove(idx)
                owner_cons.add(new_idx)
            return []

        m = re.fullmatch(
            r'ALTER TABLE "([^"]+)"\."([^"]+)" RENAME CONSTRAINT "([^"]+)" TO "([^"]+)"',
            stmt,
        )
        if m:
            schema, table, con, new_con = m.groups()
            self._require_table(schema, table)
            table_cons = self.tables[(schema, table)]
            if con not in table_cons:
                raise FakePostgresError(f'constraint "{con}" of "{schema}"."{table}" does not exist')
            if new_con in table_cons:
                raise FakePostgresError(f'constraint "{new_con}" already exists')
            table_cons.remove(con)
            table_cons.add(new_con)
            if (schema, table, con) in self.fk_refs:
                self.fk_refs[(schema, table, new_con)] = self.fk_refs.pop((schema, table, con))
            return []

        m = re.fullmatch(
            r'ALTER TABLE "([^"]+)"\."([^"]+)" ADD CONSTRAINT "([^"]+)" PRIMARY KEY \(.+\)',
            stmt,
        )
        if m:
            schema, table, con = m.groups()
            self._require_table(schema, table)
            self._add_constraint(schema, table, con)
            # A PK constraint creates a same-named backing index.
            if (schema, con) in self.indexes:
                raise FakePostgresError(f'index "{con}" already exists')
            self.indexes[(schema, con)] = (schema, table)
            return []

        m = re.fullmatch(
            r'ALTER TABLE "([^"]+)"\."([^"]+)" ADD CONSTRAINT "([^"]+)" '
            r'FOREIGN KEY \("?[^")]+"?\) REFERENCES "?([^".(]+)"?(?:\."([^"]+)")?.*',
            stmt,
        )
        if m:
            schema, table, con, ref_a, ref_b = m.groups()
            ref_schema, ref_table = (ref_a, ref_b) if ref_b else ("public", ref_a)
            self._require_table(schema, table)
            self._require_table(ref_schema, ref_table)
            self._add_constraint(schema, table, con)
            self.fk_refs[(schema, table, con)] = (ref_schema, ref_table)
            return []

        m = re.fullmatch(r'CREATE (?:UNIQUE )?INDEX "([^"]+)" ON "([^"]+)"\."([^"]+)" ?\(?.*', stmt)
        if m:
            idx, schema, table = m.groups()
            self._require_table(schema, table)
            if (schema, idx) in self.indexes:
                raise FakePostgresError(f'index "{idx}" already exists')
            self.indexes[(schema, idx)] = (schema, table)
            return []

        if re.fullmatch(r"SET LOCAL (lock_timeout|statement_timeout) = '[^']+'", stmt):
            return []

        m = re.fullmatch(r'LOCK TABLE "([^"]+)"\."([^"]+)" IN ACCESS EXCLUSIVE MODE', stmt)
        if m:
            self._require_table(m.group(1), m.group(2))
            return []

        raise AssertionError(f"FakePostgres: unhandled SQL: {stmt}")

    # -- internals ------------------------------------------------------------

    def _require_table(self, schema, table):
        if (schema, table) not in self.tables:
            raise FakePostgresError(f'relation "{schema}"."{table}" does not exist')

    def _require_no_table(self, schema, table):
        if (schema, table) in self.tables:
            raise FakePostgresError(f'relation "{table}" already exists')

    def _add_constraint(self, schema, table, con):
        if con in self.tables[(schema, table)]:
            raise FakePostgresError(f'constraint "{con}" already exists')
        self.tables[(schema, table)].add(con)

    def _drop_table(self, schema, table, missing_ok=False, cascade=False):
        if (schema, table) not in self.tables:
            if missing_ok:
                return
            raise FakePostgresError(f'relation "{schema}"."{table}" does not exist')
        # DROP TABLE rejects inbound dependents unless CASCADE, which drops
        # the dependent CONSTRAINT on the other table (not the table itself).
        dependents = [
            key
            for key, ref in self.fk_refs.items()
            if ref == (schema, table) and (key[0], key[1]) != (schema, table)
        ]
        if dependents:
            if not cascade:
                names = ", ".join(k[2] for k in dependents)
                raise FakePostgresError(
                    f'cannot drop table "{schema}"."{table}" because constraint {names} '
                    f"on another table depends on it"
                )
            for c_schema, c_table, con in dependents:
                self.tables[(c_schema, c_table)].discard(con)
                del self.fk_refs[(c_schema, c_table, con)]
        del self.tables[(schema, table)]
        self.indexes = {k: v for k, v in self.indexes.items() if v != (schema, table)}
        self.fk_refs = {k: v for k, v in self.fk_refs.items() if (k[0], k[1]) != (schema, table)}

    def _reown_indexes(self, old_owner, new_owner):
        for key, owner in self.indexes.items():
            if owner == old_owner:
                self.indexes[key] = new_owner


class _FakeConnection:
    def __init__(self, pg):
        self._pg = pg

    def cursor(self):
        return _FakeCursor(self._pg)

    def commit(self):
        self._pg._commit()

    def rollback(self):
        self._pg._rollback()


class _FakeCursor:
    def __init__(self, pg):
        self._pg = pg
        self._rows = []

    def execute(self, sql, params=None):
        self._rows = self._pg._execute(sql, params)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Specs mirroring the sync_election_api table set (representative shapes)
# ---------------------------------------------------------------------------

PT_SPEC = TableSyncSpec(
    target_table="Projected_Turnout",
    indexes=(
        Index(
            "Projected_Turnout_district_id_election_year_idx",
            "(district_id, election_year)",
        ),
    ),
    fkeys=(
        ForeignKey(
            "Projected_Turnout_district_id_fkey",
            "district_id",
            "District",
            on_delete="RESTRICT",
        ),
    ),
)

DISTRICT_SPEC = TableSyncSpec(target_table="District")

# Self-referencing FK: created against the staging table, rides the renames.
PLACE_SPEC = TableSyncSpec(
    target_table="Place",
    fkeys=(ForeignKey("Place_parent_id_fkey", "parent_id", "Place"),),
)

# The FK-linked pair (PT staging references District staging) plus the
# self-referencing Place: the representative multi-table swap set.
SET_SPECS = (DISTRICT_SPEC, PT_SPEC, PLACE_SPEC)


def _seed_live_set(pg):
    """The live tables as the election-api Prisma migrations create them."""
    pg.seed_table("public", "District", constraints={"District_pkey"}, indexes={"District_pkey"})
    pg.seed_table(
        "public",
        "Projected_Turnout",
        constraints={PT_SPEC.pk_name, *PT_SPEC.fkey_names},
        indexes={PT_SPEC.pk_name, *PT_SPEC.index_names},
        fk_refs={
            ("public", "Projected_Turnout", "Projected_Turnout_district_id_fkey"): ("public", "District")
        },
    )
    pg.seed_table(
        "public",
        "Place",
        constraints={"Place_pkey", "Place_parent_id_fkey"},
        indexes={"Place_pkey"},
        fk_refs={("public", "Place", "Place_parent_id_fkey"): ("public", "Place")},
    )


def _seed_leftover_old_set(pg):
    """Exactly what a swap that committed without drop_old leaves behind:
    archive-named tables whose FKs reference each other."""
    pg.seed_table("public", "District_old", constraints={"District_old_pkey"}, indexes={"District_old_pkey"})
    pg.seed_table(
        "public",
        "Projected_Turnout_old",
        constraints={PT_SPEC.archive_name(PT_SPEC.pk_name)}
        | {PT_SPEC.archive_name(fk) for fk in PT_SPEC.fkey_names},
        indexes={PT_SPEC.archive_name(PT_SPEC.pk_name)}
        | {PT_SPEC.archive_name(idx) for idx in PT_SPEC.index_names},
        fk_refs={
            ("public", "Projected_Turnout_old", "Projected_Turnout_old_district_id_fkey"): (
                "public",
                "District_old",
            )
        },
    )
    pg.seed_table(
        "public",
        "Place_old",
        constraints={"Place_old_pkey", "Place_old_parent_id_fkey"},
        indexes={"Place_old_pkey"},
        fk_refs={("public", "Place_old", "Place_old_parent_id_fkey"): ("public", "Place_old")},
    )


def _build_staging_set(pg):
    """build_staging + build_indexes_and_fk for the whole set, in FK order
    (District before Projected_Turnout, as the DAG's parents edges enforce)."""
    conn = pg.connect()
    for spec in SET_SPECS:
        create_staging_table(conn, spec)
    for spec in SET_SPECS:
        apply_ddl(conn, spec.constraint_ddl())
    return conn


def _run_cycle(pg, skip_drop_old=False):
    """One DAG-run's build -> stage-DDL -> swap [-> drop_old] sequence."""
    conn = _build_staging_set(pg)
    swap_staging_into_target(conn, SET_SPECS)
    if not skip_drop_old:
        drop_old_tables(conn, SET_SPECS)


def _assert_canonical_shape(pg):
    """Post-cycle invariant: the whole set live under canonical Prisma names,
    FKs pointing at the fresh siblings, no debris in either schema."""
    for spec in SET_SPECS:
        assert pg.has_table(spec.target_schema, spec.target_table)
        assert not pg.has_table(spec.target_schema, spec.old_table)
        assert not pg.has_table(spec.staging_schema, spec.new_table)
        assert pg.constraints(spec.target_schema, spec.target_table) == {spec.pk_name, *spec.fkey_names}
    expected_indexes = set()
    for spec in SET_SPECS:
        expected_indexes |= {spec.pk_name, *spec.index_names}
    assert pg.index_names("public") == expected_indexes
    assert pg.index_names("staging") == set()
    assert pg.fk_refs[("public", "Projected_Turnout", "Projected_Turnout_district_id_fkey")] == (
        "public",
        "District",
    )
    assert pg.fk_refs[("public", "Place", "Place_parent_id_fkey")] == ("public", "Place")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_happy_path_cycle_leaves_canonical_shape():
    pg = FakePostgres()
    _seed_live_set(pg)

    _run_cycle(pg)

    _assert_canonical_shape(pg)


def test_swap_is_one_transaction_with_locks_first():
    """All live targets are locked (in spec order) before any rename, under
    lock/statement timeouts, and one commit covers the whole set."""
    pg = FakePostgres()
    _seed_live_set(pg)
    _run_cycle(pg)

    stmts = pg.statements
    locks = [s for s in stmts if s.startswith("LOCK TABLE")]
    assert locks == [
        'LOCK TABLE "public"."District" IN ACCESS EXCLUSIVE MODE',
        'LOCK TABLE "public"."Projected_Turnout" IN ACCESS EXCLUSIVE MODE',
        'LOCK TABLE "public"."Place" IN ACCESS EXCLUSIVE MODE',
    ]
    first_lock = stmts.index(locks[0])
    assert any(s.startswith("SET LOCAL lock_timeout") for s in stmts[:first_lock])
    assert any(s.startswith("SET LOCAL statement_timeout") for s in stmts[:first_lock])
    first_rename = next(i for i, s in enumerate(stmts) if "RENAME TO" in s)
    assert stmts.index(locks[-1]) < first_rename


def test_next_run_recovers_after_crash_between_swap_and_drop_old():
    """THE wedge: swap commits, worker dies before drop_old, `_old` tables
    linger (with FKs to each other). The next scheduled run must clear the
    leftovers inside its own swap transaction and complete normally."""
    pg = FakePostgres()
    _seed_live_set(pg)

    _run_cycle(pg, skip_drop_old=True)  # crash window: drop_old never ran
    assert pg.has_table("public", "Projected_Turnout_old")

    _run_cycle(pg)  # next daily run must self-heal

    _assert_canonical_shape(pg)


def test_recovers_after_repeated_crashes_between_swap_and_drop_old():
    """Back-to-back crashed runs each replace the leftovers; any full run heals."""
    pg = FakePostgres()
    _seed_live_set(pg)

    _run_cycle(pg, skip_drop_old=True)
    _run_cycle(pg, skip_drop_old=True)
    _run_cycle(pg)

    _assert_canonical_shape(pg)


def test_swap_rolls_back_cleanly_at_every_crash_point():
    """Crash before each statement of the set-wise swap, in the worst-case
    state (leftover `_old` set present): the transaction must roll back to
    exactly the pre-swap state — including resurrecting the pre-dropped
    leftovers — and an immediate retry plus drop_old must complete the cycle."""
    crash_points_covered = 0
    k = 1
    while True:
        pg = FakePostgres()
        _seed_live_set(pg)
        _seed_leftover_old_set(pg)
        conn = _build_staging_set(pg)
        state_before_swap = pg.state()

        pg.arm_crash(after=k)
        try:
            swap_staging_into_target(conn, SET_SPECS)
        except RuntimeError:
            assert pg.crash_fired
            # Transactional DDL: everything (incl. the pre-drops) rolled back.
            assert pg.state() == state_before_swap
            # A task retry right after the crash must succeed.
            swap_staging_into_target(conn, SET_SPECS)
        else:
            # k exceeded the swap's statement count: sequence fully covered.
            assert not pg.crash_fired
            break

        drop_old_tables(conn, SET_SPECS)
        _assert_canonical_shape(pg)
        crash_points_covered += 1
        k += 1

    # 2 SET LOCAL + 3 existence probes + 3 locks + 3 pre-drops + per-table
    # renames make >25 statements for this set; more only add points.
    assert crash_points_covered >= 25


def test_swap_retry_after_committed_swap_fails_closed_then_next_run_heals():
    """Worker dies after the swap COMMITs but before the task is marked done,
    so Airflow retries the swap task. The retry must fail closed (roll back,
    leaving the freshly swapped set live and untouched) rather than rename
    the fresh tables aside with nothing to put in their place; the next full
    run then heals the leftover `_old` set."""
    pg = FakePostgres()
    _seed_live_set(pg)

    _run_cycle(pg, skip_drop_old=True)
    state_after_commit = pg.state()

    # The retry: staging `_new` tables no longer exist, so the transaction
    # must fail partway and roll back without touching the durable state.
    with pytest.raises(FakePostgresError):
        swap_staging_into_target(pg.connect(), SET_SPECS)
    assert pg.state() == state_after_commit

    _run_cycle(pg)  # next daily run heals the leftovers

    _assert_canonical_shape(pg)


def test_next_run_recovers_after_crash_before_swap():
    """A run that dies after building staging (before the swap) leaves
    committed staging tables with FKs among them; the next run's
    create_staging_table must replace them (CASCADE) and the cycle completes."""
    pg = FakePostgres()
    _seed_live_set(pg)

    _build_staging_set(pg)  # crash after this point: no swap

    _run_cycle(pg)

    _assert_canonical_shape(pg)


def test_first_swap_without_live_targets():
    """Cold start: no live tables yet — the swap takes the rename-old-skipping
    branch for every table, and the pre-drops of nonexistent `_old` are no-ops.
    LIKE targets are seeded bare so create_staging_table works."""
    pg = FakePostgres()
    _seed_live_set(pg)
    conn = _build_staging_set(pg)
    # Drop the live set to simulate cold start after staging was cloned.
    for spec in SET_SPECS:
        pg._drop_table("public", spec.target_table, cascade=True)
    pg._commit()

    swap_staging_into_target(conn, SET_SPECS)
    drop_old_tables(conn, SET_SPECS)

    _assert_canonical_shape(pg)


def test_drop_old_is_idempotent():
    pg = FakePostgres()
    _seed_live_set(pg)
    _run_cycle(pg)

    drop_old_tables(pg.connect(), SET_SPECS)  # second drop: IF EXISTS no-op

    _assert_canonical_shape(pg)
