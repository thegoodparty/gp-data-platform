"""Crash-recovery tests for the shared election-api build-and-swap lifecycle.

``swap_staging_into_target`` renames the live table aside (``<Table>_old``)
inside its transaction, and ``drop_old_table`` runs later as a separate
task/transaction. The dangerous window: a crash after the swap commits but
before drop_old completes leaves ``public."<Table>_old"`` behind. Without the
in-transaction pre-drop, the next run's swap then fails on the rename
collision — run after run, until a human drops the leftover — and five
consecutive failed runs auto-pause the whole DAG, freezing every table group.
These tests prove the lifecycle self-heals from a crash at every point in the
sequence.

FakePostgres models just enough real Postgres semantics for the proofs to be
honest: transactional DDL (rollback restores everything, including a
rolled-back pre-drop), duplicate-name errors on CREATE/RENAME targets,
DROP TABLE cascading to the table's indexes and constraints, and SET SCHEMA
moving a table's indexes along with it. It parses the exact closed set of
statements the lifecycle emits and fails loudly on anything it does not
recognize, so drift between the fake and the real SQL shows up as a test
error rather than a silent pass.
"""

import re
from copy import deepcopy

import pytest
from include.custom_functions.election_api_utils import (
    InboundForeignKey,
    TableSyncSpec,
    apply_ddl,
    create_staging_table,
    drop_old_table,
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

    ``arm_crash(after=k)`` makes the k-th subsequent statement raise before
    executing, simulating a worker crash at that exact point; the lifecycle's
    error handling then rolls the transaction back.
    """

    def __init__(self):
        self.tables: dict[tuple[str, str], set[str]] = {}
        self.indexes: dict[tuple[str, str], tuple[str, str]] = {}
        # (child_schema, child_table, constraint_name) -> (ref_schema, ref_table)
        self.fk_refs: dict[tuple[str, str, str], tuple[str, str]] = {}
        # normalized "SELECT count(*) ..." statement -> value the fake returns
        self.scalar_results: dict[str, int] = {}
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

    def seed_scalar(self, stmt: str, value: int):
        self.scalar_results[" ".join(stmt.split())] = value

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

        m = re.fullmatch(r'DROP TABLE IF EXISTS "([^"]+)"\."([^"]+)"', stmt)
        if m:
            self._drop_table(m.group(1), m.group(2), missing_ok=True)
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

        if stmt.startswith("SELECT count(*)"):
            # Honest relation checks: a gate SELECT against a missing staging
            # table must raise (this is what makes a committed-swap retry
            # fail closed), so require every referenced relation to exist.
            for ref in re.finditer(r'"([^"]+)"\."([^"]+)"', stmt):
                self._require_table(ref.group(1), ref.group(2))
            if stmt in self.scalar_results:
                return [(self.scalar_results[stmt],)]
            raise AssertionError(f"FakePostgres: unseeded scalar query: {stmt}")

        m = re.fullmatch(r'UPDATE "([^"]+)"\."([^"]+)" AS c SET "([^"]+)" = NULL WHERE .+', stmt)
        if m:
            for ref in re.finditer(r'"([^"]+)"\."([^"]+)"', stmt):
                self._require_table(ref.group(1), ref.group(2))
            return []

        m = re.fullmatch(r'ALTER TABLE "([^"]+)"\."([^"]+)" DROP CONSTRAINT IF EXISTS "([^"]+)"', stmt)
        if m:
            schema, table, con = m.groups()
            self._require_table(schema, table)
            self.tables[(schema, table)].discard(con)
            self.fk_refs.pop((schema, table, con), None)
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

    def _drop_table(self, schema, table, missing_ok=False):
        if (schema, table) not in self.tables:
            if missing_ok:
                return
            raise FakePostgresError(f'relation "{schema}"."{table}" does not exist')
        # DROP TABLE rejects inbound dependents (as Postgres does) and takes
        # the table's indexes, constraints, and FK bookkeeping with it.
        dependents = [
            key
            for key, ref in self.fk_refs.items()
            if ref == (schema, table) and (key[0], key[1]) != (schema, table)
        ]
        if dependents:
            names = ", ".join(k[2] for k in dependents)
            raise FakePostgresError(
                f'cannot drop table "{schema}"."{table}" because constraint {names} '
                f"on another table depends on it"
            )
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
# Lifecycle helpers mirroring the sync_election_api task groups
# ---------------------------------------------------------------------------

SPEC = TableSyncSpec(
    target_table="Projected_Turnout",
    indexes=("Projected_Turnout_district_id_election_year_idx",),
    fkeys=("Projected_Turnout_district_id_fkey",),
)

RACE_SPEC = TableSyncSpec(
    target_table="Race",
    indexes=(
        "Race_br_hash_id_idx",
        "Race_place_id_idx",
        "Race_position_id_idx",
        "Race_slug_idx",
    ),
    fkeys=("Race_place_id_fkey", "Race_position_id_fkey"),
    inbound_fkeys=(
        InboundForeignKey(
            child_table="Candidacy",
            constraint_name="Candidacy_race_id_fkey",
            child_column="race_id",
        ),
    ),
)


def _seed_race_world(pg, live_count=100, overlap=100, referencing=80, orphans=0):
    """Live Race + Candidacy with the inbound FK, plus seeded gate scalars."""
    pg.seed_table("public", "Place", constraints={"Place_pkey"}, indexes={"Place_pkey"})
    pg.seed_table("public", "Position", constraints={"Position_pkey"}, indexes={"Position_pkey"})
    _seed_live(pg, RACE_SPEC)
    pg.seed_table(
        "public",
        "Candidacy",
        constraints={"Candidacy_pkey", "Candidacy_race_id_fkey"},
        indexes={"Candidacy_pkey", "Candidacy_race_id_idx"},
        fk_refs={("public", "Candidacy", "Candidacy_race_id_fkey"): ("public", "Race")},
    )
    t = f'"{RACE_SPEC.target_schema}"."{RACE_SPEC.target_table}"'
    s = f'"{RACE_SPEC.staging_schema}"."{RACE_SPEC.new_table}"'
    c = '"public"."Candidacy"'
    pg.seed_scalar(f"SELECT count(*) FROM {t}", live_count)
    pg.seed_scalar(f'SELECT count(*) FROM {t} live JOIN {s} stg ON live."id" = stg."id"', overlap)
    pg.seed_scalar(f'SELECT count(*) FROM {c} c WHERE c."race_id" IS NOT NULL', referencing)
    pg.seed_scalar(
        f'SELECT count(*) FROM {c} c WHERE c."race_id" IS NOT NULL AND NOT EXISTS '
        f'(SELECT 1 FROM {s} stg WHERE stg."id" = c."race_id")',
        orphans,
    )


def _race_stage_ddl(spec):
    sn, nt = spec.staging_schema, spec.new_table
    ddl = [f'ALTER TABLE "{sn}"."{nt}" ADD CONSTRAINT "{spec.stage_name(spec.pk_name)}" PRIMARY KEY (id)']
    for idx, col in zip(spec.indexes, ["br_hash_id", "place_id", "position_id", "slug"], strict=True):
        ddl.append(f'CREATE INDEX "{spec.stage_name(idx)}" ON "{sn}"."{nt}" ({col})')
    for fk, (col, parent) in zip(
        spec.fkeys, [("place_id", "Place"), ("position_id", "Position")], strict=True
    ):
        ddl.append(
            f'ALTER TABLE "{sn}"."{nt}" ADD CONSTRAINT "{spec.stage_name(fk)}" '
            f'FOREIGN KEY ("{col}") REFERENCES "public"."{parent}"(id) '
            f"ON UPDATE CASCADE ON DELETE SET NULL"
        )
    return ddl


def _stage_ddl(spec):
    """Staging constraint DDL exactly as the DAG's per-table builders emit it."""
    sn, nt = spec.staging_schema, spec.new_table
    return [
        f'ALTER TABLE "{sn}"."{nt}" ADD CONSTRAINT "{spec.stage_name(spec.pk_name)}" PRIMARY KEY (id)',
        (
            f'CREATE INDEX "{spec.stage_name(spec.indexes[0])}" '
            f'ON "{sn}"."{nt}" (district_id, election_year)'
        ),
        (
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{spec.stage_name(spec.fkeys[0])}" '
            f'FOREIGN KEY (district_id) REFERENCES "public"."District"(id)'
        ),
    ]


def _seed_fk_parents(pg):
    """Referenced parents for the PT stage DDL's FK; the fake validates
    REFERENCES targets exist (as Postgres does). Seeded bare (no indexes)
    so the PT tests' exact-equality index assertions stay intact."""
    pg.seed_table("public", "District")


def _seed_live(pg, spec):
    """The live table as the election-api Prisma migration creates it."""
    _seed_fk_parents(pg)
    pg.seed_table(
        spec.target_schema,
        spec.target_table,
        constraints={spec.pk_name, *spec.fkeys},
        indexes={spec.pk_name, *spec.indexes},
    )


def _seed_leftover_old(pg, spec):
    """Exactly what a swap that committed without drop_old leaves behind."""
    pg.seed_table(
        spec.target_schema,
        spec.old_table,
        constraints={spec.archive_name(spec.pk_name)} | {spec.archive_name(fk) for fk in spec.fkeys},
        indexes={spec.archive_name(spec.pk_name)} | {spec.archive_name(idx) for idx in spec.indexes},
    )


def _run_cycle(pg, spec, skip_drop_old=False):
    """One DAG-run's build -> stage-DDL -> swap [-> drop_old] sequence."""
    conn = pg.connect()
    create_staging_table(conn, spec)
    apply_ddl(conn, _stage_ddl(spec))
    swap_staging_into_target(conn, spec)
    if not skip_drop_old:
        drop_old_table(conn, spec)


def _assert_canonical_shape(pg, spec):
    """Post-cycle invariant: live table under canonical Prisma names, no debris."""
    assert pg.has_table(spec.target_schema, spec.target_table)
    assert not pg.has_table(spec.target_schema, spec.old_table)
    assert not pg.has_table(spec.staging_schema, spec.new_table)
    assert pg.constraints(spec.target_schema, spec.target_table) == {spec.pk_name, *spec.fkeys}
    assert pg.index_names(spec.target_schema) == {spec.pk_name, *spec.indexes}
    assert pg.index_names(spec.staging_schema) == set()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_happy_path_cycle_leaves_canonical_shape():
    pg = FakePostgres()
    _seed_live(pg, SPEC)

    _run_cycle(pg, SPEC)

    _assert_canonical_shape(pg, SPEC)


def test_next_run_recovers_after_crash_between_swap_and_drop_old():
    """THE wedge: swap commits, worker dies before drop_old, `_old` lingers.

    The next scheduled run must clear the leftover inside its own swap
    transaction and complete normally, instead of failing the rename
    collision forever until a human drops the leftover.
    """
    pg = FakePostgres()
    _seed_live(pg, SPEC)

    _run_cycle(pg, SPEC, skip_drop_old=True)  # crash window: drop_old never ran
    assert pg.has_table(SPEC.target_schema, SPEC.old_table)

    _run_cycle(pg, SPEC)  # next daily run must self-heal

    _assert_canonical_shape(pg, SPEC)


def test_recovers_after_repeated_crashes_between_swap_and_drop_old():
    """Back-to-back crashed runs each replace the leftover; any full run heals."""
    pg = FakePostgres()
    _seed_live(pg, SPEC)

    _run_cycle(pg, SPEC, skip_drop_old=True)
    _run_cycle(pg, SPEC, skip_drop_old=True)
    _run_cycle(pg, SPEC)

    _assert_canonical_shape(pg, SPEC)


def test_swap_rolls_back_cleanly_at_every_crash_point():
    """Crash before each statement of the swap, in the worst-case state
    (leftover `_old` present): the transaction must roll back to exactly the
    pre-swap state — including resurrecting the pre-dropped leftover — and an
    immediate retry plus drop_old must complete the cycle."""
    crash_points_covered = 0
    k = 1
    while True:
        pg = FakePostgres()
        _seed_live(pg, SPEC)
        _seed_leftover_old(pg, SPEC)
        conn = pg.connect()
        create_staging_table(conn, SPEC)
        apply_ddl(conn, _stage_ddl(SPEC))
        state_before_swap = pg.state()

        pg.arm_crash(after=k)
        try:
            swap_staging_into_target(conn, SPEC)
        except RuntimeError:
            assert pg.crash_fired
            # Transactional DDL: everything (incl. the pre-drop) rolled back.
            assert pg.state() == state_before_swap
            # A task retry right after the crash must succeed.
            swap_staging_into_target(conn, SPEC)
        else:
            # k exceeded the swap's statement count: sequence fully covered.
            assert not pg.crash_fired
            break

        drop_old_table(conn, SPEC)
        _assert_canonical_shape(pg, SPEC)
        crash_points_covered += 1
        k += 1

    # SELECT + pre-drop + 4 archive renames + 5 stage renames = 11 statements
    # for a spec with one index and one fkey; more statements only add points.
    assert crash_points_covered >= 11


def test_swap_retry_after_committed_swap_fails_closed_then_next_run_heals():
    """Worker dies after the swap COMMITs but before the task is marked done,
    so Airflow retries the swap task. The retry must fail closed (roll back,
    leaving the freshly swapped table live and untouched) rather than rename
    the fresh table aside with nothing to put in its place; the next full
    run then heals the leftover `_old`."""
    pg = FakePostgres()
    _seed_live(pg, SPEC)

    _run_cycle(pg, SPEC, skip_drop_old=True)
    state_after_commit = pg.state()

    # The retry: staging `_new` no longer exists, so the transaction must fail
    # partway and roll back without touching the durable state.
    with pytest.raises(FakePostgresError):
        swap_staging_into_target(pg.connect(), SPEC)
    assert pg.state() == state_after_commit

    _run_cycle(pg, SPEC)  # next daily run heals the leftover

    _assert_canonical_shape(pg, SPEC)


def test_next_run_recovers_after_crash_before_swap():
    """A run that dies after building staging (before the swap) leaves a
    committed staging table; the next run's create_staging_table must
    replace it and the cycle completes."""
    pg = FakePostgres()
    _seed_live(pg, SPEC)

    conn = pg.connect()
    create_staging_table(conn, SPEC)
    apply_ddl(conn, _stage_ddl(SPEC))  # crash after this point: no swap

    _run_cycle(pg, SPEC)

    _assert_canonical_shape(pg, SPEC)


def test_first_swap_without_live_target():
    """Cold start: no live table yet — the swap takes the rename-old-skipping
    branch, and the pre-drop of a nonexistent `_old` is a no-op."""
    pg = FakePostgres()
    _seed_fk_parents(pg)
    pg.seed_table(SPEC.staging_schema, SPEC.new_table)
    conn = pg.connect()
    apply_ddl(conn, _stage_ddl(SPEC))

    swap_staging_into_target(conn, SPEC)
    drop_old_table(conn, SPEC)

    _assert_canonical_shape(pg, SPEC)


def test_drop_old_is_idempotent():
    pg = FakePostgres()
    _seed_live(pg, SPEC)
    _run_cycle(pg, SPEC)

    drop_old_table(pg.connect(), SPEC)  # second drop: IF EXISTS no-op

    _assert_canonical_shape(pg, SPEC)


# ---------------------------------------------------------------------------
# Inbound-FK swap tests (Race + Candidacy shape)
# ---------------------------------------------------------------------------


def _run_race_cycle(pg, skip_drop_old=False):
    conn = pg.connect()
    create_staging_table(conn, RACE_SPEC)
    apply_ddl(conn, _race_stage_ddl(RACE_SPEC))
    swap_staging_into_target(conn, RACE_SPEC)
    if not skip_drop_old:
        drop_old_table(conn, RACE_SPEC)


def _assert_race_canonical_shape(pg):
    """Race-world variant of _assert_canonical_shape: this world also holds
    Candidacy/Place/Position (their pkeys and indexes live in public too),
    so assert subset-plus-no-debris instead of exact equality, and pin the
    re-pointed inbound FK."""
    spec = RACE_SPEC
    assert pg.has_table(spec.target_schema, spec.target_table)
    assert not pg.has_table(spec.target_schema, spec.old_table)
    assert not pg.has_table(spec.staging_schema, spec.new_table)
    assert pg.constraints(spec.target_schema, spec.target_table) == {spec.pk_name, *spec.fkeys}
    assert {spec.pk_name, *spec.indexes} <= pg.index_names(spec.target_schema)
    assert not any(name.startswith(("Race_new_", "Race_old_")) for name in pg.index_names(spec.target_schema))
    assert pg.index_names(spec.staging_schema) == set()
    assert pg.fk_refs[("public", "Candidacy", "Candidacy_race_id_fkey")] == ("public", "Race")


def test_drop_old_fails_without_inbound_repoint():
    """The trap this work exists for: constraints follow the renamed table,
    so if the swap did NOT re-point the child FK, drop_old must fail on the
    dependency instead of deleting the referenced table out from under it."""
    pg = FakePostgres()
    _seed_race_world(pg)
    pg.seed_table(RACE_SPEC.staging_schema, RACE_SPEC.new_table)
    conn = pg.connect()
    apply_ddl(conn, _race_stage_ddl(RACE_SPEC))

    spec_without_inbound = TableSyncSpec(
        target_table="Race",
        indexes=RACE_SPEC.indexes,
        fkeys=RACE_SPEC.fkeys,
    )
    swap_staging_into_target(conn, spec_without_inbound)
    with pytest.raises(FakePostgresError, match="depends on it"):
        drop_old_table(conn, spec_without_inbound)


def test_inbound_repoint_happy_path():
    pg = FakePostgres()
    _seed_race_world(pg)
    _run_race_cycle(pg)
    _assert_race_canonical_shape(pg)


def test_child_locked_before_target_and_timeouts_set():
    pg = FakePostgres()
    _seed_race_world(pg)
    _run_race_cycle(pg)
    stmts = pg.statements
    lock_child = stmts.index('LOCK TABLE "public"."Candidacy" IN ACCESS EXCLUSIVE MODE')
    lock_target = stmts.index('LOCK TABLE "public"."Race" IN ACCESS EXCLUSIVE MODE')
    assert lock_child < lock_target
    assert any(s.startswith("SET LOCAL lock_timeout") for s in stmts[:lock_child])
    assert any(s.startswith("SET LOCAL statement_timeout") for s in stmts[:lock_child])


def test_overlap_floor_aborts_before_any_mutation():
    """A wholesale id re-key passes count-ratio gates; the in-transaction
    overlap floor must refuse before the orphan UPDATE or FK drop runs."""
    pg = FakePostgres()
    _seed_race_world(pg, live_count=100, overlap=10, referencing=80, orphans=80)
    pg.seed_table(RACE_SPEC.staging_schema, RACE_SPEC.new_table)
    conn = pg.connect()
    apply_ddl(conn, _race_stage_ddl(RACE_SPEC))
    state_before = pg.state()

    with pytest.raises(ValueError, match="id overlap"):
        swap_staging_into_target(conn, RACE_SPEC)

    assert pg.state() == state_before
    assert not any(s.startswith("UPDATE") for s in pg.statements)
    assert not any("DROP CONSTRAINT" in s for s in pg.statements)


def test_orphan_budget_aborts():
    pg = FakePostgres()
    _seed_race_world(pg, live_count=100, overlap=100, referencing=100, orphans=5)
    pg.seed_table(RACE_SPEC.staging_schema, RACE_SPEC.new_table)
    conn = pg.connect()
    apply_ddl(conn, _race_stage_ddl(RACE_SPEC))

    with pytest.raises(ValueError, match="orphan"):
        swap_staging_into_target(conn, RACE_SPEC)


def test_orphans_within_budget_are_nulled_and_swap_completes():
    pg = FakePostgres()
    _seed_race_world(pg, live_count=100, overlap=99, referencing=100, orphans=1)
    _run_race_cycle(pg)
    _assert_race_canonical_shape(pg)
    assert any(s.startswith('UPDATE "public"."Candidacy" AS c SET "race_id" = NULL') for s in pg.statements)


def test_race_swap_rolls_back_cleanly_at_every_crash_point():
    """Same exhaustive crash sweep as the PT spec, now with the inbound-FK
    statements in the transaction: every crash point must roll back to the
    exact pre-swap state (FK intact on Candidacy, no orphan nulls kept)."""
    k = 1
    covered = 0
    while True:
        pg = FakePostgres()
        _seed_race_world(pg)
        _seed_leftover_old(pg, RACE_SPEC)
        conn = pg.connect()
        create_staging_table(conn, RACE_SPEC)
        apply_ddl(conn, _race_stage_ddl(RACE_SPEC))
        state_before = pg.state()

        pg.arm_crash(after=k)
        try:
            swap_staging_into_target(conn, RACE_SPEC)
        except RuntimeError:
            assert pg.crash_fired
            assert pg.state() == state_before
            swap_staging_into_target(conn, RACE_SPEC)
        else:
            assert not pg.crash_fired
            break
        drop_old_table(conn, RACE_SPEC)
        _assert_race_canonical_shape(pg)
        covered += 1
        k += 1
    assert covered >= 15


def test_race_committed_swap_retry_fails_closed():
    """After a committed swap, an Airflow task retry must fail closed (the
    gate SELECTs reference the now-missing staging table) without mutating
    the fresh live table or the re-pointed FK; next full run heals _old."""
    pg = FakePostgres()
    _seed_race_world(pg)
    _run_race_cycle(pg, skip_drop_old=True)
    state_after_commit = pg.state()

    with pytest.raises(FakePostgresError):
        swap_staging_into_target(pg.connect(), RACE_SPEC)
    assert pg.state() == state_after_commit

    _run_race_cycle(pg)  # scalar seeds persist; same values reused
    _assert_race_canonical_shape(pg)
