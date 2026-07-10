"""build-indexes: applies PK + indexes (with State partition key) to public."Voter", then ANALYZE."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from botocore.exceptions import ClientError

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import build_indexes as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        s3_bucket="b",
        index_instance_class="db.r8g.48xlarge",
        new_writer_instance_id=lambda rd: f"gp-people-db-{rd}-dev-writer",
    ),
)

# schema_spec records build_indexes reads (PK on id; a unique index + a plain index).
_PK = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id"])
_IDXS = [
    step.IndexDef(
        table="Voter", name="Voter_LALVOTERID_key", sql="", unique=True, columns=["LALVOTERID"], where=None
    ),
    step.IndexDef(
        table="Voter",
        name="Voter_Active_idx",
        sql='CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");',
        unique=False,
        columns=["Active"],
        where=None,
    ),
]


class _FakeWaiter:
    def wait(self, **kwargs: object) -> None:
        return None


class _FakeRds:
    """RDS double for the scale-up: records describe/modify calls, can raise a one-shot in-progress fault.

    `describe_db_instances` returns items from `describe_sequence` in order, holding on the last
    item once exhausted (mirrors a real poll settling and staying settled). Defaults to a single
    steady state (available, `current_class`, no pending) built from `current_class` so callers
    that don't care about the poll sequence (e.g. the no-op test) are unaffected.
    """

    def __init__(
        self,
        current_class: str,
        *,
        raise_once: str | None = None,
        describe_sequence: list[dict] | None = None,
    ) -> None:
        self.current_class = current_class
        self.describe_calls: list[dict] = []
        self.modify_calls: list[dict] = []
        self._raise_once = raise_once  # ClientError code to raise on the first modify, then clear
        self._raised = False
        self._describe_sequence = describe_sequence or [
            {
                "DBInstanceClass": current_class,
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            }
        ]

    def describe_db_instances(self, **kw: object) -> dict:
        self.describe_calls.append(dict(kw))
        idx = min(len(self.describe_calls) - 1, len(self._describe_sequence) - 1)
        return {"DBInstances": [self._describe_sequence[idx]]}

    def modify_db_instance(self, **kw: object) -> None:
        self.modify_calls.append(dict(kw))
        if self._raise_once and not self._raised:
            self._raised = True
            raise ClientError(
                {"Error": {"Code": self._raise_once, "Message": "in progress"}}, "ModifyDBInstance"
            )

    def get_waiter(self, name: str) -> _FakeWaiter:
        return _FakeWaiter()


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test in this module patches step.time.sleep to a no-op; none relies on real sleep."""
    monkeypatch.setattr(step.time, "sleep", lambda *a, **k: None)


def test_ensure_instance_class_scales_up_when_smaller(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRds(
        current_class="db.r8g.16xlarge",
        describe_sequence=[
            # initial current-class check
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
            # _wait_class_applied poll: settled on the target class
            {
                "DBInstanceClass": "db.r8g.48xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
        ],
    )
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    step._ensure_instance_class(_CFG, "20260709")
    modify = fake.modify_calls[0]
    assert modify["DBInstanceClass"] == "db.r8g.48xlarge"
    assert modify["DBInstanceIdentifier"] == "gp-people-db-20260709-dev-writer"
    assert modify["ApplyImmediately"] is True


def test_ensure_instance_class_noop_when_already_index_class(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRds(current_class="db.r8g.48xlarge")
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    step._ensure_instance_class(_CFG, "20260709")
    assert not fake.modify_calls  # already scaled => no modify


def test_ensure_instance_class_retries_after_in_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRds(
        current_class="db.r8g.16xlarge",
        raise_once="InvalidDBInstanceStateFault",
        describe_sequence=[
            # initial current-class check
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
            # _wait_class_applied poll (after the fault settles and the modify is re-issued): settled
            {
                "DBInstanceClass": "db.r8g.48xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
        ],
    )
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    step._ensure_instance_class(_CFG, "20260709")
    assert len(fake.modify_calls) == 2  # first raised the fault, settled, then re-issued


def test_ensure_instance_class_reraises_other_client_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # A non-InvalidDBInstanceStateFault error is a genuine bad state, not an in-progress modify:
    # it must propagate, never be swallowed (which would let a misconfigured box look like success).
    fake = _FakeRds(current_class="db.r8g.16xlarge", raise_once="InvalidParameterCombination")
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    with pytest.raises(ClientError):
        step._ensure_instance_class(_CFG, "20260709")
    # the tolerated-fault retry path was NOT taken: only the first modify was attempted
    assert len(fake.modify_calls) == 1


def test_ensure_instance_class_waits_until_class_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    # Aurora keeps reporting the instance 'available' for a few seconds after modify_db_instance
    # before it flips to 'modifying' and reboots. Assert we ride through that stale-available /
    # pending-modify state and only return once the poll observes the settled, applied class.
    fake = _FakeRds(
        current_class="db.r8g.16xlarge",
        describe_sequence=[
            # initial current-class check
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
            # first poll: modify has been issued but not yet applied
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "modifying",
                "PendingModifiedValues": {"DBInstanceClass": "db.r8g.48xlarge"},
            },
            # second poll: settled on the target class
            {
                "DBInstanceClass": "db.r8g.48xlarge",
                "DBInstanceStatus": "available",
                "PendingModifiedValues": {},
            },
        ],
    )
    monkeypatch.setattr(step, "rds", lambda cfg: fake)
    step._ensure_instance_class(_CFG, "20260709")
    assert len(fake.modify_calls) == 1
    # initial check + at least two polls (the not-yet-applied state, then the settled state)
    assert len(fake.describe_calls) >= 3


def test_wait_class_applied_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    # After _CLASS_APPLY_MAX_POLLS, if the class has not been applied, _wait_class_applied raises
    # RuntimeError. _CLASS_APPLY_MAX_POLLS is patched to a small number so the test completes
    # quickly, and use a _FakeRds that never reaches the target class.
    monkeypatch.setattr(step, "_CLASS_APPLY_MAX_POLLS", 3)
    # The describe_sequence holds the last state forever (mirrors real poll behavior), so set the
    # last item to a never-settling state: still in the old class with a pending class change.
    fake = _FakeRds(
        current_class="db.r8g.16xlarge",
        describe_sequence=[
            {
                "DBInstanceClass": "db.r8g.16xlarge",
                "DBInstanceStatus": "modifying",
                "PendingModifiedValues": {"DBInstanceClass": "db.r8g.48xlarge"},
            },
        ],
    )
    with pytest.raises(RuntimeError, match="did not reach class"):
        step._wait_class_applied(fake, "writer", "db.r8g.48xlarge")  # ty: ignore[invalid-argument-type]


def test_build_session_sql_fills_the_box() -> None:
    # The index box is db.r8g.48xlarge (192 vCPU). Aurora defaults max_parallel_workers to ~96
    # (~vCPU/2), which caps the build at ~125 active backends and leaves ~67 cores idle. Raise the
    # pool so the index build uses the box it pays for, and widen per-build maintenance workers so
    # the long-pole giant partition builds spread wider.
    sql = step._BUILD_SESSION_SQL
    assert "SET max_parallel_workers = 176" in sql
    assert "SET max_parallel_maintenance_workers = 16" in sql


def test_rewrite_injects_if_not_exists() -> None:
    assert "CREATE INDEX IF NOT EXISTS" in step._rewrite_index_sql(
        'CREATE INDEX "x" ON public."Voter" ("Active");'
    )
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in step._rewrite_index_sql(
        'CREATE UNIQUE INDEX "u" ON public."Voter" ("LALVOTERID");'
    )


def test_child_index_name_short_and_hashed() -> None:
    assert step._child_index_name("Voter_Active_idx", "CA") == "Voter_Active_idx_CA"
    # A very long parent name + state would blow the 63-char identifier limit -> hashed fallback.
    long = "Voter_" + "Really_Long_District_Name_" * 3 + "idx"
    child = step._child_index_name(long, "WY")
    assert len(child) <= 63 and child.startswith("ix_") and child.endswith("_WY")


def test_plain_parent_only_and_child_sql() -> None:
    idx = step.IndexDef(
        table="Voter",
        name="Voter_Active_idx",
        sql='CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");',
        unique=False,
        columns=["Active"],
        where=None,
    )
    parent = step._plain_parent_only_sql(idx)
    assert (
        'CREATE INDEX IF NOT EXISTS "Voter_Active_idx" ON ONLY public."Voter" USING btree ("Active")'
        in parent
    )
    child_name, child_sql = step._plain_child_sql(idx, "CA")
    assert child_name == "Voter_Active_idx_CA"
    assert (
        'CREATE INDEX IF NOT EXISTS "Voter_Active_idx_CA" ON public."Voter_CA" USING btree ("Active")'
        in child_sql
    )
    assert "ON ONLY" not in child_sql  # the child targets the leaf partition, not the parent


def test_build_and_attach_child_skips_reattach_when_already_attached() -> None:
    # pg_inherits returns a row => the child is already a partition of the parent index; a
    # partial-rerun must build (IF NOT EXISTS, no-op) but NOT re-issue ATTACH (which would error).
    conn = FakeConn().queue_result((1,))
    step._build_and_attach_child(conn, (_IDXS[1], "CA"))  # ty: ignore[invalid-argument-type]
    sql = executed_sql(conn)
    assert any('"Voter_Active_idx_CA" ON public."Voter_CA"' in s for s in sql)
    assert not any("ATTACH PARTITION" in s for s in sql)


def test_order_children_largest_first_sorts_by_partition_size() -> None:
    a = step.IndexDef(table="Voter", name="Voter_A_idx", sql="", unique=False, columns=["A"], where=None)
    b = step.IndexDef(table="Voter", name="Voter_B_idx", sql="", unique=False, columns=["B"], where=None)
    units = [(a, "WY"), (a, "CA"), (b, "TX"), (b, "WY")]
    sizes = {"CA": 4_000_000, "TX": 3_000_000, "WY": 10_000}
    ordered = step._order_children_largest_first(units, sizes)
    # Biggest partition first, smallest last; the two WY units are a size tie and must keep input
    # order (A before B) — asserting on the index name, not just the state, so a non-stable or
    # reversed-tie sort is actually caught.
    assert [(idx.name, s) for idx, s in ordered] == [
        ("Voter_A_idx", "CA"),
        ("Voter_B_idx", "TX"),
        ("Voter_A_idx", "WY"),
        ("Voter_B_idx", "WY"),
    ]


def test_order_children_largest_first_unknown_size_sorts_last() -> None:
    a = step.IndexDef(table="Voter", name="Voter_A_idx", sql="", unique=False, columns=["A"], where=None)
    units = [(a, "ZZ"), (a, "CA")]  # ZZ has no known size -> treated as 0 -> last
    ordered = step._order_children_largest_first(units, {"CA": 100})
    assert [s for _, s in ordered] == ["CA", "ZZ"]


def test_partition_sizes_maps_state_to_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    conn.queue_result([("Voter_CA", 4_000_000), ("Voter_TX", 3_000_000)])  # one fetchall result set
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    sizes = step._partition_sizes(_CFG, "20260709", forward=None)
    assert sizes == {"CA": 4_000_000, "TX": 3_000_000}


def test_run_builds_pk_indexes_and_analyzes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))  # no bastion in unit tests
    monkeypatch.setattr(step, "primary_key_for", lambda t: _PK)
    monkeypatch.setattr(step, "indexes_for", lambda t: _IDXS)
    monkeypatch.setattr(step, "_l2type_coverage", lambda cfg, rd, **_k: [])
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "STATES", ("CA", "TX"))
    # Already at the index class: _ensure_instance_class is a no-op describe, no real RDS calls.
    monkeypatch.setattr(step, "rds", lambda cfg: _FakeRds(current_class=cfg.index_instance_class))

    # parallelism=1 keeps the persistent-connection pool single-threaded, so all stages run on the
    # one shared FakeConn deterministically (no cross-thread races on its recorded-SQL list).
    manifest = step.run(_CFG, "20260609", parallelism=1)
    sql = executed_sql(conn)

    # PK must include "State"
    assert any("ADD CONSTRAINT" in s and 'PRIMARY KEY ("id", "State")' in s for s in sql)
    # Unique index stays a parent-level build, with "State" appended
    assert any(
        'CREATE UNIQUE INDEX IF NOT EXISTS "Voter_LALVOTERID_key" ON public."Voter" ("LALVOTERID", "State")'
        in s
        for s in sql
    )
    # Largest-partition-first scheduling queries partition sizes before building children (the
    # FakeConn's fetchall defaults to [] when nothing is queued, so this runs with unknown sizes
    # and falls back to input order -- exercised for real by test_partition_sizes_maps_state_to_bytes).
    assert any("pg_relation_size" in s for s in sql)
    # Plain index is built PER PARTITION: empty parent (ON ONLY) + a child per state + ATTACH.
    assert any('CREATE INDEX IF NOT EXISTS "Voter_Active_idx" ON ONLY public."Voter"' in s for s in sql)
    assert any('"Voter_Active_idx_CA" ON public."Voter_CA"' in s for s in sql)
    assert any('"Voter_Active_idx_TX" ON public."Voter_TX"' in s for s in sql)
    assert any(
        'ALTER INDEX public."Voter_Active_idx" ATTACH PARTITION public."Voter_Active_idx_CA"' in s
        for s in sql
    )
    # ...and NOT built directly on the parent (that's the slow serial-per-partition path we removed).
    assert not any('"Voter_Active_idx" ON public."Voter" USING' in s for s in sql)
    assert any('ANALYZE public."Voter"' in s for s in sql)
    assert manifest.status == "complete"
    assert manifest.analyzed_tables == ["Voter"]
    assert "Voter_pkey" in manifest.constraints_added
    assert {i.index_name for i in manifest.indexes} == {"Voter_LALVOTERID_key", "Voter_Active_idx"}


def test_create_index_unique_preserves_where(monkeypatch: pytest.MonkeyPatch) -> None:
    # A partial unique index keeps its WHERE predicate (and gets State appended).
    conn = FakeConn()
    idx = step.IndexDef(
        table="Voter",
        name="Voter_u_idx",
        sql="(verbatim unused for unique)",
        unique=True,
        columns=["LALVOTERID"],
        where='"x" IS NOT NULL',
    )
    step._create_index(conn, idx)  # ty: ignore[invalid-argument-type]
    sql = " ".join(executed_sql(conn))
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "Voter_u_idx"' in sql
    assert '("LALVOTERID", "State")' in sql
    assert 'WHERE "x" IS NOT NULL' in sql


def test_create_index_unique_functional_raises() -> None:
    # A functional unique index can't be safely requoted from parsed columns with the
    # partition key — fail loudly instead of emitting invalid DDL.
    conn = FakeConn()
    idx = step.IndexDef(
        table="Voter",
        name="Voter_fn_uniq",
        sql="(unused)",
        unique=True,
        columns=['lower("Email")'],
        where=None,
    )
    with pytest.raises(RuntimeError, match="expression column"):
        step._create_index(conn, idx)  # ty: ignore[invalid-argument-type]


def test_create_index_unique_empty_columns_raises() -> None:
    # A unique index with no parsed columns must NOT silently rebuild to UNIQUE("State") —
    # guards the extraction regression the seed once had (columns=[]).
    conn = FakeConn()
    idx = step.IndexDef(
        table="Voter", name="Voter_LALVOTERID_key", sql="(unused)", unique=True, columns=[], where=None
    )
    with pytest.raises(RuntimeError, match="no parsed columns"):
        step._create_index(conn, idx)  # ty: ignore[invalid-argument-type]


def test_l2type_coverage_returns_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    # prod has Type_A + Type_B; new table only has a Type_A column -> Type_B missing.
    prod_conn = FakeConn().queue_result([("Type_A",), ("Type_B",)])
    new_conn = FakeConn().queue_result([("Type_A",)])
    monkeypatch.setattr(step, "connect_prod", fake_connect(prod_conn))
    monkeypatch.setattr(step, "connect_new", fake_connect(new_conn))
    assert step._l2type_coverage(_CFG, "20260609") == ["Type_B"]


def test_l2type_coverage_returns_none_when_prod_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    # The None return (vs []) marks the check as skipped; it's load-bearing, so cover it.
    def _boom(*args: object, **kwargs: object) -> object:
        raise RuntimeError("org_districts unreachable")

    monkeypatch.setattr(step, "connect_prod", _boom)
    assert step._l2type_coverage(_CFG, "20260609") is None


class _PKRaisingConn:
    """Connection whose ALTER TABLE ... ADD CONSTRAINT raises a given psycopg error.

    Session SETs (non-ALTER) pass through, so only the PK statement triggers it.
    """

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def __enter__(self) -> _PKRaisingConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> _PKRaisingConn:
        return self

    def fetchone(self) -> object:
        # The idempotency pre-check ("does a PK already exist?") sees none, so _add_primary_key
        # proceeds to the ADD — where ALTER TABLE raises the injected error.
        return None

    def execute(self, sql: str, params: object = None) -> None:
        if sql.strip().startswith("ALTER TABLE"):
            raise self._exc


def test_add_primary_key_skips_when_pk_exists() -> None:
    # Re-runnability: a table can hold only one PK, so a re-run on a cluster that already has it
    # must SKIP the ADD (which would raise 42P16), not re-issue it. The pre-check finds the PK.
    conn = FakeConn().queue_result(("Voter_pkey",))
    pk = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])
    step._add_primary_key(conn, pk)  # ty: ignore[invalid-argument-type]  # must not raise
    assert not any("ADD CONSTRAINT" in s for s in executed_sql(conn))


def test_add_primary_key_propagates_invalid_definition() -> None:
    # With no existing PK, a genuine bad-DDL InvalidTableDefinition (42P16) on the ADD is a
    # structural rejection, not idempotency — it must propagate, never recorded as added.
    import psycopg

    pk = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])
    raising = _PKRaisingConn(psycopg.errors.InvalidTableDefinition("bad ddl"))
    with pytest.raises(psycopg.errors.InvalidTableDefinition):
        step._add_primary_key(raising, pk)  # ty: ignore[invalid-argument-type]
