"""The unattended daily entry point: match the pending list, quarantine any
per-office response-shape failure, apply the outcome-conditional write
policy, and append under the run key the orchestrator (the DAG) passes in.

Carries none of the supervised ceremony `backlog_run.py` has -- no compare
artifacts, no cohort-predicate file, no exclusion file, no local run
records. Bedrock-only, pinned-prompt fail-loud, and the strict per-office
prompt wrap are inherited unchanged, by import, from `backlog_run`.

    python -m stitch_golden_data.prod_gold_data.daily_run \\
        --run-key 2026-09-02T14:30:00+00:00 [--batch-size N] \\
        [--embedding-batch-size N]
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta

import pandas as pd

import stitch_golden_data.prod_gold_data.l2_br_matcher as _matcher_mod
from bedrock_clients.structured import StructuredOutputError
from shared.braintrust import flush_logs
from shared.databricks_client import DatabricksClient
from stitch_golden_data.prod_gold_data.backlog_run import (
    _build_clients,
    _iso_timestamp,
    _require_pinned_prompt,
    _strict_build_cached_prompt,
)
from stitch_golden_data.prod_gold_data.l2_br_match_schema import (
    QUARANTINE_TABLE_PATH,
    RESULTS_TABLE_PATH,
    RUN_LOG_TABLE_PATH,
)
from stitch_golden_data.prod_gold_data.l2_br_match_writer import MatchResultWriter
from stitch_golden_data.prod_gold_data.l2_br_matcher import L2BrMatcher, MatchResult, _positive_int

# Run B's own run key. An office whose latest attempt predates this boundary
# is one of the cohorts the owner withheld from Run B (quarantined stale or
# moved matches, held withdrawals, the 77 shape-miss offices); their
# disposition is the supervised tuning-era rerun, never this loop. An
# attempt AT OR AFTER the boundary -- including Run B's own written rows --
# is this loop's diet: that is what lets the ~09-30 abstain wave mature
# here. Delete this constant (and the filter) when that rerun erases the
# cohorts.
CUTOVER_BOUNDARY = datetime(2026, 8, 31, 19, 46, 39, tzinfo=UTC)
# A pending list bigger than this is a de facto full re-match, which is a
# supervised owner decision; the ~8k monthly abstain wave sits comfortably
# under it. Deliberately not configurable -- a single-use knob here would
# only ever be turned by the same owner call this ceiling already routes to.
COHORT_CEILING = 20_000
# A systemic response-shape failure must fail the run loudly, not
# quarantine a wave.
QUARANTINE_CIRCUIT_BREAKER = 10
# Matches the pending list's own abstain-maturity clock
# (int__l2_br_match_pending_offices) -- a released and re-quarantined office
# gets exactly one more cycle at this length before it suppresses again.
QUARANTINE_RETRY_DAYS = 30
# Identity of the WHOLE cohort semantics: the pending-selector rule and the
# outcome write policy together. Bump this when either changes.
POLICY_VERSION = "v1-hold-withdrawals"
# The only reason code that exists today; bounded enum, never exception text.
REASON_STRUCTURED_OUTPUT = "structured_output_shape"


def require_git_sha() -> str:
    """Unlike backlog_run's `_git_state()`, there is no git probe to fall
    back to here: the image carries no .git and no git binary, so an unset
    GIT_SHA means this run would have no provenance at all. That must fail
    loud, not write a blank column.
    """
    git_sha = os.environ.get("GIT_SHA")
    if not git_sha:
        raise RuntimeError("GIT_SHA is not set; the daily entry point refuses to run without run provenance")
    return git_sha


# -- Outcome-conditional write policy (spec section 4) ----------------------


def split_by_write_policy(
    results: list[MatchResult], prior_district_by_bid: dict[int, str | None]
) -> list[MatchResult]:
    """The rows that write. Every match writes; an abstain writes only when
    the prior serving answer was NOT itself a match (a first abstain or a
    re-abstain, which resets the 30-day clock). What is left out is exactly
    the withdrawals, held until the rename-normalization lever lands, so the
    caller derives that count as len(results) - len(write). `prior` absent
    and `None` both mean "not a prior match".
    """
    return [r for r in results if r.l2_district_name is not None or prior_district_by_bid.get(r.br_database_id) is None]


def boundary_filter(office_ids: list[int], prior_attempted_at: dict[int, datetime]) -> list[int]:
    """Ids whose latest attempt predates CUTOVER_BOUNDARY: the pre-cutover
    backlog the owner withheld from Run B. Ids with no prior attempt at all
    are never dropped.
    """
    return [bid for bid in office_ids if (at := prior_attempted_at.get(bid)) is not None and at < CUTOVER_BOUNDARY]


# -- Warehouse reads: one query feeds both filters above ---------------------


def _read_prior_answers(
    databricks: DatabricksClient, before: datetime, pending_table: str
) -> dict[int, tuple[str | None, datetime]]:
    """Newest row per pending office as of just before this run -- the same
    qualify-newest-row shape as backlog_run's `_prior_answers_sha256`, but
    returning the district name and timestamp directly rather than a hash:
    `split_by_write_policy` and `boundary_filter` both derive from this one
    read, so there is only one place the "latest answer" definition can
    drift. Semi-joined to the pending table because both consumers only ever
    look up pending offices, and the results history grows without bound
    while the daily cohort stays small.
    """
    boundary = before.isoformat(sep=" ", timespec="seconds")
    df = databricks.execute_query(
        f"""
        select br_database_id, l2_district_name, attempted_at
        from {RESULTS_TABLE_PATH}
        where
            attempted_at < timestamp'{boundary}'
            and br_database_id in (select br_database_id from {pending_table})
        qualify row_number() over (
            partition by br_database_id order by attempted_at desc, l2_district_name nulls first
        ) = 1
        """
    )
    return {
        int(row.br_database_id): (None if pd.isna(row.l2_district_name) else row.l2_district_name, row.attempted_at)
        for row in df.itertuples(index=False)
    }


def _read_quarantine_eligibility(databricks: DatabricksClient, now: datetime) -> tuple[set[int], set[int]]:
    """One query over every unreleased quarantine row; Python decides due
    vs. suppressed so the 30-day arithmetic lives in one place, not
    duplicated between this read and the eventual release write. A `held`
    row never lands in `due`; an `auto` row does once `last_failed_at` is
    30+ days old.
    """
    df = databricks.execute_query(
        f"select br_database_id, retry_class, last_failed_at from {QUARANTINE_TABLE_PATH} where released_at is null"
    )
    retry_cutoff = now - timedelta(days=QUARANTINE_RETRY_DAYS)
    suppressed: set[int] = set()
    due: set[int] = set()
    for row in df.itertuples(index=False):
        bid = int(row.br_database_id)
        # Fail closed: the DDL does not enforce the enum, and a misspelled
        # manually seeded class must never default into the auto-retry path.
        if row.retry_class not in ("auto", "held"):
            raise RuntimeError(
                f"quarantine row for office {bid} carries unknown retry_class "
                f"{row.retry_class!r}; fix the row before running"
            )
        if row.retry_class == "held" or row.last_failed_at > retry_cutoff:
            suppressed.add(bid)
        else:
            due.add(bid)
    return suppressed, due


def _install_daily_pending_wrap(
    matcher: L2BrMatcher, suppressed_ids: set[int], prior_attempted_at: dict[int, datetime]
) -> dict:
    """Shadow THIS matcher's own `load_pending_offices`, mirroring
    backlog_run's exclusion wrap: both filters ride the one read the loop
    needs anyway, and their counts are exactly what the run log persists.
    """
    original = matcher.load_pending_offices
    captured = {"quarantine_dropped": 0, "boundary_dropped": 0}

    def wrapped():
        # The real loader returns its declared columns even when empty, so
        # every step below is safe unguarded on an empty frame.
        df = original()
        mask = df["br_database_id"].isin(suppressed_ids)
        captured["quarantine_dropped"] = int(mask.sum())
        df = df[~mask]
        dropped = boundary_filter(list(df["br_database_id"]), prior_attempted_at)
        captured["boundary_dropped"] = len(dropped)
        df = df[~df["br_database_id"].isin(dropped)]
        return df.reset_index(drop=True)

    matcher.load_pending_offices = wrapped
    return captured


# -- The match loop: run()'s own logic, plus a per-office quarantine catch --


async def _match_cohort(matcher: L2BrMatcher, offices: list, batch_size: int) -> tuple[list[MatchResult], list[int]]:
    """Mirrors `L2BrMatcher.run()`'s own batch loop, with the one addition
    `run()` deliberately does not have: a per-office catch for the client's
    typed response-shape failure, so one bad office quarantines instead of
    aborting the whole day's cohort. Every other exception still propagates
    -- an LLM failure must still fail the run. Checked after every batch,
    not only at the end, so a systemic failure aborts before finishing a
    long cohort's remaining (paid) batches.
    """
    results: list[MatchResult] = []
    quarantined: list[int] = []

    async def _one(office) -> MatchResult | None:
        try:
            return await matcher.match_office(
                br_database_id=office.br_database_id,
                br_name=office.name,
                state=office.state,
                mtfcc=office.mtfcc,
                is_judicial=office.is_judicial,
                has_unknown_boundaries=office.has_unknown_boundaries,
                geo_id=office.geo_id,
                sub_area_name=office.sub_area_name,
                sub_area_value=office.sub_area_value,
            )
        except StructuredOutputError:
            quarantined.append(office.br_database_id)
            return None

    for batch_start in range(0, len(offices), batch_size):
        batch = offices[batch_start : batch_start + batch_size]
        batch_results = await asyncio.gather(*(_one(office) for office in batch))
        results.extend(r for r in batch_results if r is not None)
        # Strictly greater, per the spec's "more than 10 quarantines aborts":
        # exactly QUARANTINE_CIRCUIT_BREAKER is the maximum tolerated count.
        if len(quarantined) > QUARANTINE_CIRCUIT_BREAKER:
            raise RuntimeError(
                f"{len(quarantined)} office(s) quarantined for a response-shape failure in one run, "
                f"more than the {QUARANTINE_CIRCUIT_BREAKER} the circuit breaker tolerates; a systemic "
                "failure must fail loud, not silently suppress a wave (write-at-end: nothing has landed yet)"
            )

    return results, quarantined


# -- Quarantine table upserts -------------------------------------------


def _write_quarantine_upserts(
    databricks: DatabricksClient,
    quarantined_this_run: list[int],
    due_ids: set[int],
    result_bids: set[int],
    run_key: datetime,
) -> None:
    """Three cases, in order: a due office that failed again re-stamps
    `last_failed_at` (it is already a row); an office quarantined for the
    first time gets a new row; a due office that RETURNED A RESULT this run
    is released. Release keys on results, deliberately: a due id can sit in
    the quarantine table while no longer being on the pending list at all
    (its universe tuple healed on its own, say), and that office was neither
    re-attempted nor re-failed, so it must not be stamped released.
    """
    cursor = databricks.connect().cursor()
    try:
        for bid in quarantined_this_run:
            if bid in due_ids:
                # released_at is null scopes the UPDATE to the ACTIVE episode:
                # a released historical row for the same office must keep its
                # own timestamps and release note.
                cursor.execute(
                    f"update {QUARANTINE_TABLE_PATH} set last_failed_at = ? "
                    "where br_database_id = ? and released_at is null",
                    [run_key, bid],
                )
            else:
                cursor.execute(
                    f"""
                    insert into {QUARANTINE_TABLE_PATH}
                        (br_database_id, reason_code, retry_class, first_failed_at, last_failed_at)
                    values (?, ?, 'auto', ?, ?)
                    """,
                    [bid, REASON_STRUCTURED_OUTPUT, run_key, run_key],
                )
        for bid in sorted(due_ids & result_bids):
            cursor.execute(
                f"update {QUARANTINE_TABLE_PATH} set released_at = ?, release_note = ? "
                "where br_database_id = ? and released_at is null",
                [run_key, "auto: succeeded on retry", bid],
            )
    finally:
        cursor.close()


def _write_run_log(databricks: DatabricksClient, **counts) -> None:
    cursor = databricks.connect().cursor()
    try:
        cursor.execute(
            f"""
            insert into {RUN_LOG_TABLE_PATH}
                (run_key, policy_version, cohort_size, backlog_boundary_dropped, quarantine_dropped,
                 matched_written, abstains_written, withdrawals_held, quarantined_this_run,
                 embedding_config, llm_config, prompt_provenance, git_sha, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                counts["run_key"],
                POLICY_VERSION,
                counts["cohort_size"],
                counts["backlog_boundary_dropped"],
                counts["quarantine_dropped"],
                counts["matched_written"],
                counts["abstains_written"],
                counts["withdrawals_held"],
                counts["quarantined_this_run"],
                json.dumps(counts["embedding_config"]),
                json.dumps(counts["llm_config"]),
                json.dumps(counts["prompt_provenance"]),
                counts["git_sha"],
                datetime.now(UTC),
            ],
        )
    finally:
        cursor.close()


# -- Orchestration --------------------------------------------------------


async def _run(args: argparse.Namespace) -> None:
    git_sha = require_git_sha()

    # Installed before anything can call the matcher: a per-office render
    # failure must crash this run, never silently swap in the fallback.
    _matcher_mod.build_cached_prompt = _strict_build_cached_prompt

    embedding_client, llm = _build_clients("bedrock")
    matcher = L2BrMatcher(embedding_client=embedding_client, llm=llm)
    try:
        _require_pinned_prompt(matcher)

        prior_answers = _read_prior_answers(matcher.databricks, args.run_key, matcher.pending_offices_path)
        prior_district_by_bid = {bid: district for bid, (district, _at) in prior_answers.items()}
        prior_attempted_at = {bid: at for bid, (_district, at) in prior_answers.items()}

        suppressed_ids, due_ids = _read_quarantine_eligibility(matcher.databricks, args.run_key)

        captured = _install_daily_pending_wrap(matcher, suppressed_ids, prior_attempted_at)
        pending_df = matcher.load_pending_offices()
        cohort_size = len(pending_df)
        if cohort_size > COHORT_CEILING:
            raise RuntimeError(
                f"pending cohort is {cohort_size} office(s), over the {COHORT_CEILING} ceiling -- a cohort "
                "this size is a de facto full re-match, an owner-decided run, never this loop's"
            )

        worklist_states = sorted(pending_df["state"].unique())
        await matcher.build_universe(worklist_states, args.embedding_batch_size)

        offices = list(pending_df.itertuples(index=False))
        results, quarantined = await _match_cohort(matcher, offices, args.batch_size)

        write = split_by_write_policy(results, prior_district_by_bid)
        withdrawals_held = len(results) - len(write)
        matched_written = sum(1 for r in write if r.l2_district_name is not None)
        abstains_written = len(write) - matched_written

        writer = MatchResultWriter(databricks=matcher.databricks)  # shares the matcher's own connection
        written = writer.append_results(write, attempted_at=args.run_key)

        result_bids = {r.br_database_id for r in results}
        _write_quarantine_upserts(matcher.databricks, quarantined, due_ids, result_bids, args.run_key)

        _write_run_log(
            matcher.databricks,
            run_key=args.run_key,
            cohort_size=cohort_size,
            backlog_boundary_dropped=captured["boundary_dropped"],
            quarantine_dropped=captured["quarantine_dropped"],
            matched_written=matched_written,
            abstains_written=abstains_written,
            withdrawals_held=withdrawals_held,
            quarantined_this_run=len(quarantined),
            embedding_config=matcher.embedding_client.resolved_config(),
            llm_config=matcher.llm.resolved_config(),
            prompt_provenance=matcher.prompt_provenance,
            git_sha=git_sha,
        )

        summary = {
            "run_key": args.run_key.isoformat(),
            "policy_version": POLICY_VERSION,
            "cohort_size": cohort_size,
            "backlog_boundary_dropped": captured["boundary_dropped"],
            "quarantine_dropped": captured["quarantine_dropped"],
            "written": written,
            "matched_written": matched_written,
            "abstains_written": abstains_written,
            "withdrawals_held": withdrawals_held,
            "quarantined_this_run": len(quarantined),
        }
        # stdout plus the warehouse run log are the durable records; a local
        # file would die with the pod and could fail after the writes landed.
        print(f"daily run summary: {summary}", flush=True)
    finally:
        flush_logs()
        try:
            matcher.close()
        except Exception:
            matcher.logger.warning("matcher.close() raised during teardown", exc_info=True)


# -- CLI --------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--run-key",
        required=True,
        # Truncated to second precision, matching the writer's own
        # attempted_at convention and backlog_run's minted run_key.
        type=lambda v: _iso_timestamp(v).replace(microsecond=0),
        help="This run's key: a timezone-aware ISO-8601 timestamp (naive is refused).",
    )
    parser.add_argument(
        "--batch-size", type=_positive_int, default=100, help="Offices matched concurrently per group (default: 100)"
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=_positive_int,
        default=100,
        help="District texts embedded per call (default: 100)",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise
