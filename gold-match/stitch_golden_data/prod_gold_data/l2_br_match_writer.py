"""Databricks write path for the L2-to-BallotReady match results.

Two operations over the one table the matcher owns -- append a batch of
results under a run key, and delete a run -- plus the validation they share.
The schema of record is `l2_br_match_schema.py` beside this module, whose
`ensure_results_table()` provisions the table by hand; dbt reads it as a
source and never creates it, which is already the convention here.

Lives beside the matcher rather than in shared/: shared/ is imported by every
other service in this package and held to mypy's strict disallow_untyped_defs,
so giving it its first write capability for one caller is a blast radius this
module avoids by depending on shared.databricks_client.DatabricksClient
instead of becoming part of it. shared/ is not modified.

The write does not have to be atomic, and this module deliberately does not
try to make it so. A half-written run is incomplete, not corrupt: there is no
status column and no cross-row invariant left for a row to violate, every row
stands on its own, and an office whose row never arrived is still on the
pending list, which is where it started. So what the writer owes instead of a
transaction is detection -- `append_results` counts what landed against what
it was asked to write -- and a documented recovery, which is `delete_run`.
That is why there is no staging table, no CREATE on one, and no truncate step.

`attempted_at` is the run key. One run stamps one value across every row it
writes, and the CALLER passes it in rather than each writer minting its own,
so a run that is sharded or resumed keeps a single key. A resumed run
anti-joins what it already wrote under that key: the matcher is not
deterministic, so two rows for one office at one timestamp are two different
answers with no rule for choosing between them.

`attempted_at` must be **timezone-aware**. It is the sole key for the
anti-join, the count and `delete_run`, and a naive value resolved under a
different session timezone on a later resume silently splits one run into
two keys, with the count check passing on both. Hand-built timestamps are
the ones that come out naive, and the baseline run stamps a hand-chosen
historical date.
"""

from collections.abc import Iterator
from datetime import datetime

from databricks.sql.client import Cursor

from shared.databricks_client import DatabricksClient
from shared.logger import get_logger
from stitch_golden_data.prod_gold_data.l2_br_match_schema import RESULTS_TABLE_PATH
from stitch_golden_data.prod_gold_data.l2_br_matcher import MatchResult

# 6 columns x 500 rows = 3,000 bound parameters per INSERT, inside the 4,000
# verified by hand against the real warehouse -- 500 groups of 8, over this
# same connector version and the same default (Thrift) backend, as a read-only
# `SELECT count(*) FROM (VALUES ...)` touching no table. No 256-parameter
# ceiling exists for positional markers on this path. ceil(20,166 / 500) = 41
# round trips for the measured backlog. Not tuned against a live warehouse.
RESULTS_INSERT_CHUNK_SIZE = 500


def validate_results(results: list[MatchResult]) -> None:
    """Per-row validation over the WHOLE batch, before the first insert.
    Raises ValueError naming every failing row and why, without writing
    anything.

    Two rules, and only two, because only two are both unenforced elsewhere
    and reachable from a batch this module can be handed:

    - **The district key is all set or all null.** A partial key is a writer
      bug. It is deliberately not a dbt test or a Delta CHECK (that decision
      is in the epic's ledger: per-row validation belongs in the container),
      so this is its only enforcement. The damage is quiet rather than loud:
      a partial key misses the universe join, so the office reopens on every
      pending-list build and re-pays the LLM cost forever.
    - **No duplicate br_database_id inside one batch.** The anti-join in
      `append_results` covers rows already durable under this run key; it
      cannot see two rows for one office inside the in-memory batch it is
      handed, which a sharded or concatenated run can produce. "Latest
      attempt wins" has no answer for two rows at one timestamp.

    Not checked here, deliberately, with the reason each:
    `attempted_at` and `br_database_id` nullity are `not null` in the DDL and
    Delta enforces that on write. `confidence` range is validated
    unconditionally upstream in `_selection_from_response` (integrality and
    0-100) for every row that carries one; a geography-filter abstain writes
    None with no model judgment behind it, which the nullable column accepts.
    `br_database_id` type is not checked either: `append_results` puts the id
    through a set membership test before this runs, so an unhashable one
    raises there first, and a hashable non-int can only come from a
    hand-built MatchResult. All three would guard a path that does not run.
    """
    errors: list[str] = []
    seen_ids: set[int] = set()

    for i, row in enumerate(results):
        row_errors: list[str] = []

        if row.br_database_id in seen_ids:
            row_errors.append(f"br_database_id {row.br_database_id} is duplicated in this batch")
        else:
            seen_ids.add(row.br_database_id)

        district_fields = (row.l2_state, row.l2_district_type, row.l2_district_name)
        if any(f is None for f in district_fields) and any(f is not None for f in district_fields):
            row_errors.append(
                "l2_state, l2_district_type and l2_district_name must be all set (a match) "
                f"or all null (an attempt that found nothing), got {district_fields!r}"
            )

        if row_errors:
            errors.append(f"row {i} (br_database_id={row.br_database_id!r}): " + "; ".join(row_errors))

    if errors:
        raise ValueError(f"{len(errors)} of {len(results)} row(s) failed validation:\n" + "\n".join(errors))


def _require_aware(attempted_at: datetime) -> None:
    """The run key must be timezone-aware. Enforced rather than documented,
    because the failure is silent in both directions.

    A naive key makes the anti-join see nothing already written, so a resumed
    run re-inserts every office under a second timestamp representation and
    the count check passes on both -- two rows per office, and "latest wins"
    has no answer. On `delete_run` the same value matches nothing, so the one
    recovery path there is returns 0 having removed nothing.

    Reachable from the documented workflow rather than hypothetically: the
    baseline run stamps a hand-chosen historical date, and a hand-built
    `datetime` is naive unless someone remembers otherwise.
    """
    if attempted_at.tzinfo is None:
        raise ValueError(f"attempted_at must be timezone-aware, got a naive datetime: {attempted_at!r}")


def _chunked(rows: list[MatchResult], size: int) -> Iterator[list[MatchResult]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


class MatchResultWriter:
    """Appends match results and deletes a run. Nothing here decides FOR the
    operator whether a run should be kept or rolled back.
    """

    def __init__(
        self,
        results_table: str = RESULTS_TABLE_PATH,
        databricks: DatabricksClient | None = None,
    ) -> None:
        """`databricks` is injectable so the supervised cutover can share the
        matcher's own session instead of opening a second one --
        DatabricksClient memoizes its connection and never health-checks it,
        and the cutover is hand-driven across a long session with a human
        review pause in the middle. `close()` only closes a connection this
        instance opened itself: closing a shared, injected one out from under
        its other owner would defeat the point of sharing it.
        """
        self.logger = get_logger(__name__)
        self._owns_databricks = databricks is None
        self.databricks = databricks or DatabricksClient()
        self.results_table = results_table

    def _cursor(self) -> Cursor:
        return self.databricks.connect().cursor()

    def _written(self, cursor: Cursor, attempted_at: datetime) -> tuple[int, set[int]]:
        """(row count, distinct ids) for this run key, from ONE scan.

        Two scans could disagree with each other if anything wrote between
        them, and the two numbers are not the same thing: the anti-join needs
        the ids, the count check needs rows.
        """
        cursor.execute(
            f"select br_database_id from {self.results_table} where attempted_at = ?",
            [attempted_at],
        )
        ids = [row[0] for row in cursor.fetchall()]
        return len(ids), set(ids)

    def _row_count(self, cursor: Cursor, attempted_at: datetime) -> int:
        cursor.execute(
            f"select count(*) from {self.results_table} where attempted_at = ?",
            [attempted_at],
        )
        (count,) = cursor.fetchone()
        return int(count)

    def append_results(self, results: list[MatchResult], attempted_at: datetime) -> int:
        """Append `results` under the run key `attempted_at`, and return how
        many rows this call wrote.

        Validates the whole batch, then skips any office already written
        under this key (the anti-join), then inserts what is left in chunks,
        then counts what the table holds for this key and raises if it is
        short of what it should be. That count is the whole of what replaces a transaction:
        the connector has none -- `Connection.commit()` is a documented no-op
        and `rollback()` raises `NotSupportedError` -- so each chunk commits
        independently and a failure part-way leaves an incomplete run. That
        is recoverable and not corrupt; `delete_run(attempted_at)` is the
        documented repair.

        Multi-row INSERT with bound parameters throughout, chunked at
        RESULTS_INSERT_CHUNK_SIZE -- never `Cursor.executemany`, which by its
        own docstring is a naive loop issuing one sequential request per row
        with no batching.
        """
        _require_aware(attempted_at)
        if not results:
            self.logger.warning("append_results called with zero rows; nothing to do")
            return 0

        cursor = self._cursor()
        try:
            # Validate BEFORE the anti-join, not after. The anti-join drops
            # every row whose id is already durable, so a batch carrying the
            # same id twice loses both copies when that id is already written
            # -- and the duplicate, which this module is the only enforcement
            # of, never becomes visible.
            validate_results(results)

            rows_before, already_written = self._written(cursor, attempted_at)
            to_write = [r for r in results if r.br_database_id not in already_written]
            if already_written:
                self.logger.info(
                    f"Resuming run {attempted_at.isoformat()}: {len(already_written)} office(s) already "
                    f"written under this key, {len(to_write)} of {len(results)} left to write"
                )
            if not to_write:
                self.logger.info("Every office in this batch is already written under this run key")
                return 0

            for chunk in _chunked(to_write, RESULTS_INSERT_CHUNK_SIZE):
                placeholders = ", ".join(["(?, ?, ?, ?, ?, ?)"] * len(chunk))
                params = [
                    value
                    for row in chunk
                    for value in (
                        row.br_database_id,
                        row.l2_state,
                        row.l2_district_type,
                        row.l2_district_name,
                        row.confidence,
                        attempted_at,
                    )
                ]
                cursor.execute(
                    f"""
                    insert into {self.results_table}
                        (br_database_id, l2_state, l2_district_type, l2_district_name,
                         confidence, attempted_at)
                    values {placeholders}
                    """,
                    params,
                )

            expected = rows_before + len(to_write)
            actual = self._row_count(cursor, attempted_at)
            if actual < expected:
                raise RuntimeError(
                    f"Short write for run {attempted_at.isoformat()}: the table holds {actual} row(s) "
                    f"for this key, expected {expected}. The run is incomplete, not corrupt -- delete "
                    f"it with MatchResultWriter.delete_run and run it again."
                )
            if actual > expected:
                # NOT a short write, and the repair for one would destroy the
                # other's rows. Two shards sharing a run key and running
                # concurrently both read rows_before before either inserts, so
                # both land here. Do not delete; reconcile first.
                raise RuntimeError(
                    f"Run {attempted_at.isoformat()} holds {actual} row(s), more than the {expected} "
                    f"this call accounts for. Another writer touched this key concurrently. Do NOT "
                    f"delete_run: reconcile first, since one run key must have a single writer."
                )
        finally:
            cursor.close()

        self.logger.info(f"Wrote {len(to_write)} result row(s) under run key {attempted_at.isoformat()}")
        return len(to_write)

    def delete_run(self, attempted_at: datetime) -> int:
        """Delete every row stamped with this run key, and return how many
        rows went.

        The rollback, and also the repair for a short write. The previous
        answer for each of those offices becomes current again, since the
        staging model takes the newest row per office by `attempted_at`. To
        undo a release, delete the target run and every run after it, then
        rebuild. The DELETE itself stays in Delta history, so what happened
        and when is still answerable afterwards.
        """
        _require_aware(attempted_at)
        cursor = self._cursor()
        try:
            before = self._row_count(cursor, attempted_at)
            cursor.execute(
                f"delete from {self.results_table} where attempted_at = ?",
                [attempted_at],
            )
            # Counted rather than asserted: this connector hardcodes
            # `Cursor.rowcount = -1`, so a DELETE that matched nothing --
            # a mistyped key, or a naive datetime against an aware one --
            # is indistinguishable from a successful one. This is the only
            # recovery path there is, driven by hand under cutover pressure,
            # so it returns a number the caller can check rather than
            # reporting success having done nothing.
            after = self._row_count(cursor, attempted_at)
        finally:
            cursor.close()
        deleted = before - after
        self.logger.info(f"Deleted {deleted} row(s) for run {attempted_at.isoformat()}")
        return deleted

    def close(self) -> None:
        """Close the Databricks connection this instance opened itself.

        A no-op if `databricks` was injected at construction (see `__init__`).
        `DatabricksClient.close()` is itself idempotent, so a second call is
        harmless and needs no flag here to make it so.
        """
        if self._owns_databricks:
            self.databricks.close()
