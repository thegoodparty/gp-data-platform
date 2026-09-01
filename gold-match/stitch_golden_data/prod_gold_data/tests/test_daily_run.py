"""The daily entry point's policy core (the outcome-conditional write
policy, the pre-cutover boundary filter) and its match loop (per-office
quarantine, the circuit breaker, quarantine-table eligibility and release).
No live AWS/Databricks calls -- every collaborator here is a self-contained
fake, never a real warehouse or a real matcher.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from bedrock_clients.structured import StructuredOutputError
from stitch_golden_data.prod_gold_data import daily_run
from stitch_golden_data.prod_gold_data.l2_br_matcher import MatchResult

PRE_BOUNDARY_TS = daily_run.CUTOVER_BOUNDARY - timedelta(days=1)
POST_BOUNDARY_TS = daily_run.CUTOVER_BOUNDARY + timedelta(days=1)


def _match(bid: int, district: str = "District 5") -> MatchResult:
    return MatchResult(bid, "DE", "House", district, 90)


def _abstain(bid: int) -> MatchResult:
    return MatchResult(bid, None, None, None, confidence=None)


class TestWithdrawalHoldPolicy:
    """The three lines of spec section 4's write policy: a match always
    writes, an abstain writes unless the prior serving answer was itself a
    match (a withdrawal, held per POLICY_VERSION).
    """

    def test_withdrawal_held_when_prior_was_match(self):
        """Failure this catches: a served office losing its link because a
        renamed or reshaped L2 district makes the matcher abstain -- the
        hold is what makes the loop additive-only toward served offices.
        """
        results = [_abstain(1)]
        prior = {1: "District 3"}  # office 1's latest serving answer is a match

        write, held = daily_run.split_by_write_policy(results, prior)

        assert write == [] and held == [1]

    def test_abstain_written_when_prior_was_abstain_or_absent(self):
        """Failure this catches: a first abstain or a re-abstain being held
        instead of written, which would stop the 30-day clock from ever
        resetting and silently freeze the pending list.
        """
        results = [_abstain(1), _abstain(2)]
        prior = {1: None}  # office 2 has no prior row at all

        write, held = daily_run.split_by_write_policy(results, prior)

        assert [r.br_database_id for r in write] == [1, 2] and held == []

    def test_matches_always_written(self):
        """Failure this catches: a real match being held because the prior
        answer happened to be a (now-superseded) match -- only an abstain
        outcome is ever a candidate for the hold.
        """
        results = [_match(1)]
        prior = {1: "Old District"}

        write, held = daily_run.split_by_write_policy(results, prior)

        assert len(write) == 1 and held == []


class TestPreCutoverBoundaryFilter:
    def test_boundary_drops_precutover_attempts_keeps_never_attempted(self):
        """Failure this catches: the daily loop re-touching the backlog
        cohorts the owner deliberately withheld from Run B (their eraser is
        the supervised tuning-era rerun), or the mirror mistake of dropping
        a genuinely new arrival that has no prior attempt at all.
        """
        prior_attempted_at = {1: PRE_BOUNDARY_TS, 2: POST_BOUNDARY_TS}

        kept, dropped = daily_run.boundary_filter([1, 2, 3], prior_attempted_at)

        assert kept == [2, 3] and dropped == [1]


class TestCliGuards:
    def test_naive_run_key_refused(self):
        """Failure this catches: a naive --run-key reaching the writer's
        anti-join under an unpinned session timezone, silently splitting one
        run across two keys (the same trap backlog_run's own --run-key guards).
        """
        with pytest.raises(SystemExit):
            daily_run.parse_args(["--run-key", "2026-09-02 14:30:00"])

    def test_blank_git_sha_refused(self, monkeypatch):
        """Failure this catches: a run silently writing a blank git_sha (or
        crashing on a nonexistent git binary) instead of refusing outright
        -- the container image carries no .git, so GIT_SHA is the only
        provenance source there is.
        """
        monkeypatch.delenv("GIT_SHA", raising=False)
        with pytest.raises(RuntimeError, match="GIT_SHA"):
            daily_run.require_git_sha()


# -- Match-loop fakes: plain classes, not Mocks, standing in for
# L2BrMatcher.match_office and a pending-office row. -------------------


class _FakeOffice:
    def __init__(self, br_database_id: int):
        self.br_database_id = br_database_id
        self.name = f"Office {br_database_id}"
        self.state = "DE"
        self.mtfcc = "G4110"
        self.is_judicial = False
        self.has_unknown_boundaries = False
        self.geo_id = None
        self.sub_area_name = None
        self.sub_area_value = None


class _FakeMatcher:
    """Stands in for L2BrMatcher: exposes only match_office, the one method
    _match_cohort calls, keyed by outcome per office id (a MatchResult, or
    an exception instance to raise).
    """

    def __init__(self, outcomes: dict):
        self._outcomes = outcomes

    async def match_office(self, *, br_database_id, **kwargs):
        outcome = self._outcomes[br_database_id]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class TestMatchLoopQuarantine:
    def test_structured_output_error_quarantines_and_run_finishes(self):
        """Failure this catches: one office's response-shape failure
        aborting the whole cohort instead of being quarantined -- exactly
        the behavior this loop adds on top of L2BrMatcher.run()'s own
        fail-loud (no per-office catch) contract.
        """
        offices = [_FakeOffice(1), _FakeOffice(2), _FakeOffice(3)]
        matcher = _FakeMatcher({1: _match(1), 2: StructuredOutputError("bad shape"), 3: _match(3)})

        results, quarantined = asyncio.run(daily_run._match_cohort(matcher, offices, batch_size=3))

        assert [r.br_database_id for r in results] == [1, 3]
        assert quarantined == [(2, daily_run.REASON_STRUCTURED_OUTPUT)]

    def test_circuit_breaker_aborts_before_write(self):
        """Failure this catches: the circuit breaker threshold not enforced
        (or enforced only at the very end), letting a systemic response-
        shape failure silently suppress a whole wave instead of aborting
        the run before any batch calls the writer.
        """
        n = daily_run.QUARANTINE_CIRCUIT_BREAKER + 1
        offices = [_FakeOffice(i) for i in range(1, n + 1)]
        matcher = _FakeMatcher({i: StructuredOutputError("bad shape") for i in range(1, n + 1)})

        with pytest.raises(RuntimeError, match="circuit breaker"):
            asyncio.run(daily_run._match_cohort(matcher, offices, batch_size=n))

    def test_exactly_breaker_count_is_tolerated(self):
        """Pins the boundary's other side: exactly QUARANTINE_CIRCUIT_BREAKER
        quarantines is the maximum TOLERATED count per the spec's "more than
        10 aborts" -- the run finishes and records them, it does not abort.
        """
        n = daily_run.QUARANTINE_CIRCUIT_BREAKER
        offices = [_FakeOffice(i) for i in range(1, n + 1)]
        matcher = _FakeMatcher({i: StructuredOutputError("bad shape") for i in range(1, n + 1)})

        results, quarantined = asyncio.run(daily_run._match_cohort(matcher, offices, batch_size=n))

        assert results == []
        assert len(quarantined) == n


# -- Quarantine-table fakes: a fake DatabricksClient recording the literal
# SQL and bound params, the same contract the writer's own tests hold it
# to -- never a Mock standing in for the warehouse. -------------------


class _FakeCursor:
    def __init__(self, recorder: list):
        self._recorder = recorder

    def execute(self, sql, params=None):
        self._recorder.append((sql, params))

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, recorder: list):
        self._recorder = recorder

    def cursor(self):
        return _FakeCursor(self._recorder)


class _FakeDatabricksClient:
    def __init__(self, query_result: pd.DataFrame | None = None):
        self.executed: list[tuple[str, list | None]] = []
        self.queries: list[str] = []
        self._query_result = pd.DataFrame() if query_result is None else query_result

    def execute_query(self, sql: str) -> pd.DataFrame:
        self.queries.append(sql)
        return self._query_result

    def connect(self):
        return _FakeConnection(self.executed)


def _calls(client: _FakeDatabricksClient, fragment: str) -> list:
    return [c for c in client.executed if fragment in c[0].lower()]


class TestPendingWrap:
    def test_wrap_filters_and_captures_both_counts(self):
        """Failure this catches: the wrap's filter sequencing or its captured
        counts drifting from what actually got dropped -- those two numbers
        are written verbatim to the run log, so a miscount is a false audit
        record, and a mis-sequenced filter double-drops or misses offices.
        """
        boundary = daily_run.CUTOVER_BOUNDARY

        class _PendingOnlyMatcher:
            def load_pending_offices(self, states=None, limit=None):
                return pd.DataFrame(
                    {
                        "br_database_id": [1, 2, 3, 4],
                        "state": ["CA", "CA", "TX", "TX"],
                    }
                )

        matcher = _PendingOnlyMatcher()
        prior_attempted_at = {
            2: boundary - timedelta(seconds=1),  # pre-cutover: boundary-dropped
            3: boundary,  # exactly at the key: Run B's own, stays
        }
        captured = daily_run._install_daily_pending_wrap(
            matcher, suppressed_ids={1}, prior_attempted_at=prior_attempted_at
        )

        df = matcher.load_pending_offices()

        assert list(df["br_database_id"]) == [3, 4]  # 4 never attempted, stays
        assert captured["quarantine_dropped"] == 1
        assert captured["boundary_dropped"] == 1


class TestPriorAnswersRead:
    def test_maps_null_district_to_none_and_pins_query_shape(self):
        """Failure this catches: pandas surfacing a SQL NULL district as NaN
        (which would make every prior abstain look like a match to the write
        policy, silencing the withdrawal hold), the query losing its pending
        semi-join (the read would scan unbounded history again), or the
        tie-break drifting from the staging model's abstain-wins ordering,
        which must stay character-identical or "latest answer" silently
        changes meaning.
        """
        ts_old = datetime(2026, 8, 31, 20, 0, 0, tzinfo=UTC)
        ts_new = datetime(2026, 9, 1, 20, 0, 0, tzinfo=UTC)
        rows = pd.DataFrame(
            {
                "br_database_id": [1, 2],
                "l2_district_name": [float("nan"), "District 3"],
                "attempted_at": [ts_old, ts_new],
            }
        )
        client = _FakeDatabricksClient(query_result=rows)

        out = daily_run._read_prior_answers(client, datetime(2026, 9, 2, tzinfo=UTC), "cat.dbt.pending")

        assert out[1] == (None, ts_old)
        assert out[2] == ("District 3", ts_new)
        (sql,) = client.queries
        assert "in (select br_database_id from cat.dbt.pending)" in sql
        assert "order by attempted_at desc, l2_district_name nulls first" in sql


class TestQuarantineEligibility:
    def test_due_auto_quarantine_bypasses_suppression(self):
        """Failure this catches: an auto row still suppressing its office
        after the 30-day backoff elapses, which would strand a
        deterministically-failing office (and the human who needs to see
        it) forever instead of giving it its scheduled retry.
        """
        now = datetime(2026, 9, 5, tzinfo=UTC)
        rows = pd.DataFrame(
            {
                "br_database_id": [101, 202],
                "retry_class": ["auto", "auto"],
                "last_failed_at": [now - timedelta(days=31), now - timedelta(days=5)],
            }
        )
        client = _FakeDatabricksClient(query_result=rows)

        suppressed, due = daily_run._read_quarantine_eligibility(client, now)

        assert due == {101}
        assert suppressed == {202}

    def test_unknown_retry_class_fails_closed(self):
        """Failure this catches: a misspelled manually seeded class (the DDL
        does not enforce the enum) silently defaulting into the auto-retry
        path -- a 'held' office typo'd as 'hold' would get retried monthly.
        """
        now = datetime(2026, 9, 5, tzinfo=UTC)
        rows = pd.DataFrame(
            {
                "br_database_id": [303],
                "retry_class": ["hold"],
                "last_failed_at": [now - timedelta(days=31)],
            }
        )
        client = _FakeDatabricksClient(query_result=rows)

        with pytest.raises(RuntimeError, match="unknown retry_class"):
            daily_run._read_quarantine_eligibility(client, now)


class TestQuarantineRelease:
    def test_successful_retry_releases_quarantine_row(self):
        """Failure this catches: a due office that actually returned a
        result this run being left suppressed forever instead of released
        -- the quarantine table would never reflect that a retry (or the
        upstream client fix) resolved it. Also pins the narrower edge case:
        a due office NOT attempted this run (e.g. it fell off the pending
        list on its own) must not be stamped released just because it also
        never showed up as a fresh failure.
        """
        client = _FakeDatabricksClient()
        run_key = datetime(2026, 9, 5, tzinfo=UTC)

        daily_run._write_quarantine_upserts(
            client, quarantined_this_run=[], due_ids={555, 556}, attempted_bids={555}, run_key=run_key
        )

        assert _calls(client, "released_at") == [
            (
                f"update {daily_run.QUARANTINE_TABLE_PATH} "
                "set released_at = ?, release_note = ? "
                "where br_database_id = ? and released_at is null",
                [run_key, "auto: succeeded on retry", 555],
            )
        ]

    def test_updates_scope_to_active_episode_only(self):
        """Failure this catches: a re-quarantined office's UPDATE touching its
        RELEASED historical rows too, clobbering their timestamps and manual
        release notes -- both quarantine UPDATEs must carry the active-episode
        scope (released_at is null).
        """
        client = _FakeDatabricksClient()
        run_key = datetime(2026, 9, 5, tzinfo=UTC)

        daily_run._write_quarantine_upserts(
            client,
            quarantined_this_run=[(777, "structured_output_shape")],
            due_ids={777},
            attempted_bids={777},
            run_key=run_key,
        )

        updates = _calls(client, "update ")
        assert updates, "expected the due re-fail to re-stamp last_failed_at"
        for sql, _params in updates:
            assert "released_at is null" in sql
