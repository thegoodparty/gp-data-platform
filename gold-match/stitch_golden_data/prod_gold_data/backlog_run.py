"""The checked-in Run-B entry point: `run` and `rollback`.

Before this module, `MatchResultWriter.append_results` had no non-test caller
and the runbook's write step was a hand-assembled snippet -- so a cohort-scoped
publication decision had no enforcement (the writer takes whatever it is
handed) and a rollback had no verification that the delete actually matched
what the run wrote. This wraps `L2BrMatcher.run()` and the writer, making
ZERO changes to the matcher. The writer's `delete_run` gained one optional
parameter (`expected_count`) -- an explicitly ruled exception, so the count
check runs before the DELETE executes instead of recomputing it after.

    python -m stitch_golden_data.prod_gold_data.backlog_run run \\
        --cohort-predicate-file approved.sql --cohort-expected-count 1234 \\
        --compare-against ../research/dry-run-2026-08-27 --out-dir out/

    python -m stitch_golden_data.prod_gold_data.backlog_run rollback \\
        --run-record out/run-record.json [--published]
"""

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

import stitch_golden_data.prod_gold_data.l2_br_matcher as _matcher_mod
from shared.braintrust import build_cached_prompt as _orig_build_cached_prompt
from shared.braintrust import flush_logs
from shared.databricks_client import DatabricksClient
from stitch_golden_data.prod_gold_data.l2_br_match_schema import RESULTS_TABLE_PATH
from stitch_golden_data.prod_gold_data.l2_br_match_writer import MatchResultWriter
from stitch_golden_data.prod_gold_data.l2_br_matcher import (
    L2BrMatcher,
    MatchResult,
    _build_clients,
    _canonical_state_arg,
    _positive_int,
)

INPUT_FIELDS = (
    "br_database_id",
    "name",
    "state",
    "mtfcc",
    "geo_id",
    "sub_area_name",
    "sub_area_value",
    "is_judicial",
    "has_unknown_boundaries",
)
OUTPUT_FIELDS = ("l2_state", "l2_district_type", "l2_district_name", "confidence")

DBT_CLOUD_REBUILD_JOB_ID = 70471823431462  # the scheduled build: universe + every consumer
ELECTION_API_SYNC_DAG_ID = "sync_election_api"  # swaps the reviewed Databricks tables into PostgreSQL
ELECTION_API_SWAP_TASK_ID = "swap"  # airflow/astro/dags/sync_election_api.py's own @task function name
ELECTION_API_SWAP_VARIABLE = "election_api_swap_enabled"  # same DAG's SWAP_GATE_VARIABLE
_DBT_CLOUD_ENV_VARS = ("DBT_CLOUD_BASE_URL", "DBT_CLOUD_ACCOUNT_ID", "DBT_CLOUD_API_TOKEN")
_AIRFLOW_ENV_VARS = ("AIRFLOW_API_BASE_URL", "AIRFLOW_API_TOKEN")
_RUN_ARTIFACT_FILENAMES = ("manifest.json", "answers.json", "run-record.json")


# -- git / hash idioms (mirrors the dry-run driver, so A and B are comparable) --


def _git_state() -> dict:
    repo_root = Path(__file__).resolve().parents[3]

    def git(*args: str) -> str:
        return subprocess.run(["git", "-C", str(repo_root), *args], capture_output=True, text=True).stdout.strip()

    return {"sha": git("rev-parse", "HEAD"), "dirty_worktree": bool(git("status", "--porcelain"))}


def _sha(obj: object) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def _nn(v: object) -> object:
    # pandas hands back NaN, not None, for a SQL NULL read off a raw query.
    return None if v is None or (isinstance(v, float) and v != v) else v


def _universe_sha256(matcher: L2BrMatcher) -> tuple[str, dict[str, str]]:
    """Two-stage hash, exactly mirroring the dry-run driver's algorithm: one
    hash per state over its sorted district lines, then one hash over the
    sorted state:per-state-hash lines. A single flatten-everything-then-hash
    pass produces a DIFFERENT value for identical data, which would make the
    compare's universe equality always false against Run A.
    """
    per_state = {}
    for state, u in matcher._universe_by_state.items():
        lines = sorted(
            f"{u.states[i]}|{u.district_types[i]}|{u.district_names[i]}" for i in range(len(u.district_names))
        )
        per_state[state] = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    key_sha256 = hashlib.sha256("\n".join(f"{s}:{h}" for s, h in sorted(per_state.items())).encode()).hexdigest()
    return key_sha256, per_state


def _prior_answers_sha256(databricks: DatabricksClient, before: datetime) -> str:
    """Hash of the latest answer per office as of just before this run's key
    -- the staging model's own ordering -- so two runs can be compared on the
    same notion of "the state this run found things in".
    """
    # isoformat, not strftime: strftime's format codes drop the UTC offset,
    # and Databricks interprets an unzoned timestamp literal in the SESSION
    # timezone (never pinned by DatabricksClient.connect()) -- a non-UTC
    # session would shift this boundary by the full offset. isoformat keeps
    # "+00:00", which Databricks accepts inside a timestamp literal.
    boundary = before.isoformat(sep=" ", timespec="seconds")
    df = databricks.execute_query(
        f"""
        select br_database_id, l2_state, l2_district_type, l2_district_name, confidence
        from {RESULTS_TABLE_PATH}
        where attempted_at < timestamp'{boundary}'
        qualify row_number() over (
            partition by br_database_id order by attempted_at desc, l2_district_name nulls first
        ) = 1
        """
    )
    prior = {
        int(r.br_database_id): [_nn(r.l2_state), _nn(r.l2_district_type), _nn(r.l2_district_name), _nn(r.confidence)]
        for r in df.itertuples(index=False)
    }
    return _sha(prior)


# -- the pinned prompt, fail-loud (both halves of the gate driver's pattern) --


def _strict_build_cached_prompt(prompt_name: str, variables: dict, fallback_prompt: str | None = None) -> str:
    """Per-office half: a mid-run render failure must crash the run, never
    silently swap in the in-code fallback prompt."""
    rendered = _orig_build_cached_prompt(prompt_name, variables, fallback_prompt=None)
    if not rendered:
        raise RuntimeError(f"pinned prompt {prompt_name!r} failed to render; failing loud, not falling back")
    return rendered


def _require_pinned_prompt(matcher: L2BrMatcher) -> None:
    """Construction half: with Braintrust simply unconfigured (no API key),
    the matcher itself only warns and proceeds on the fallback -- fine for ad
    hoc dev use, but exactly the drift a production run must never allow.
    """
    if matcher.prompt_provenance is None:
        raise RuntimeError("pinned prompt did not load (is BRAINTRUST_API_KEY set?); refusing to run on the fallback")


# -- office exclusion + pending-list capture (one instance-local wrap) --
#
# Production run() has no per-office catch -- match_office raises through,
# and run()'s own except re-raises after logging cost -- so a known
# deterministic shape-miss office in the batch aborts the WHOLE run
# pre-write. Excluding it has to happen before the pending list is even
# read, not inside match_office, which this PR must not touch.
#
# This wrap is installed on the MATCHER INSTANCE, never the class, and
# always -- not only when an exclusion file is given. The entry point no
# longer reads the pending list itself (that read is now this wrap's
# capture of run()'s own single call), so there is no second read to fall
# back to: "no exclusion file" means the filtering half of the wrap is a
# no-op, not that the wrap is absent.


def _parse_exclude_ids(path: Path) -> set[int]:
    ids = set()
    for line in path.read_text().splitlines():
        stripped = line.split("#", 1)[0].strip()
        if stripped:
            ids.add(int(stripped))
    return ids


def _install_pending_offices_wrap(matcher: L2BrMatcher, exclude_ids: set[int]) -> dict:
    """Shadow THIS matcher's own `load_pending_offices` (an instance
    attribute, never touching `L2BrMatcher` itself). `run()`'s one internal
    call goes through it too, so the captured frame IS what `run()` worked
    from -- one warehouse read, not two, and nothing for a second read to
    drift out of sync with.
    """
    original = matcher.load_pending_offices
    captured = {"pending_df": None, "dropped": 0}

    def wrapped(states=None, limit=None):
        df = original(states=states, limit=limit)
        if not df.empty and exclude_ids:
            mask = df["br_database_id"].isin(exclude_ids)
            captured["dropped"] = int(mask.sum())
            df = df[~mask].reset_index(drop=True)
        captured["pending_df"] = df
        return df

    matcher.load_pending_offices = wrapped
    return captured


def _maybe_install_exclusion(matcher: L2BrMatcher, exclude_file: Path | None) -> tuple[set[int], dict]:
    """No file = no filtering, but the capturing wrap is still installed --
    it is the only read of the pending list left. Returns the exclude id
    set (for the compare's own "excluded" bucket) and the captured dict:
    `pending_df` once `matcher.load_pending_offices` actually runs (None
    until then), `dropped`, `file_sha256`, `id_count`.
    """
    exclude_ids = _parse_exclude_ids(exclude_file) if exclude_file is not None else set()
    captured = _install_pending_offices_wrap(matcher, exclude_ids)
    captured["file_sha256"] = hashlib.sha256(exclude_file.read_bytes()).hexdigest() if exclude_file else None
    captured["id_count"] = len(exclude_ids)
    return exclude_ids, captured


# -- cohort pre-write filter --


def apply_cohort_filter(
    databricks: DatabricksClient, predicate_sql: str, expected_count: int, results: list[MatchResult]
) -> tuple[list[MatchResult], dict]:
    """Wholesale publication is expressed the same way as a cohort: a
    predicate that selects every row, with its own recorded count. There is
    no bypass, so a forgotten filter can never publish more than what was
    counted and ruled on.
    """
    approved = databricks.execute_query(predicate_sql)
    approved_ids = {int(v) for v in approved["br_database_id"]}
    if len(approved_ids) != expected_count:
        raise RuntimeError(
            f"cohort predicate returned {len(approved_ids)} id(s), recorded count is {expected_count}; "
            "refusing to write until the predicate and the recorded count agree"
        )
    filtered = [r for r in results if r.br_database_id in approved_ids]
    counts = {"approved_count": len(approved_ids), "results_count": len(results), "intersection_count": len(filtered)}
    return filtered, counts


# -- manifest + A-vs-B compare --


def _answer_rows(pending_df: "pd.DataFrame", results: list[MatchResult]) -> list[dict]:
    """One row per pending office: the input the matcher was asked about,
    plus what it answered. `run()` returns outputs only, so the compare
    needs this echo -- read from `pending_df`, the SAME DataFrame `run()`'s
    one internal read produced (captured by the exclusion wrap, not a
    second read this module took itself).

    A length check, not a full identity check: with a single shared read,
    `run()` produces exactly one result per row of `pending_df` by its own
    contract (answers every office it was handed, or raises), so the two
    can only disagree in length if that contract itself broke -- there is
    no second, independently-drifting read left for a set mismatch to mean
    anything else.
    """
    if len(pending_df) != len(results):
        raise RuntimeError(
            f"run() returned {len(results)} result(s) for a pending list of {len(pending_df)} office(s); "
            "expected exactly one result per office"
        )

    by_id = {r.br_database_id: r for r in results}
    rows = []
    for row in pending_df.itertuples(index=False):
        bid = int(row.br_database_id)
        answer = by_id[bid]  # every id is in both sets, checked above
        rows.append(
            {
                "br_database_id": bid,
                "name": row.name,
                "state": row.state,
                "mtfcc": row.mtfcc,
                "geo_id": row.geo_id,
                "sub_area_name": row.sub_area_name,
                "sub_area_value": row.sub_area_value,
                # bool(...), not the raw cell: an object-dtype boolean column
                # (upstream of the matcher's own astype(bool) boundary) can
                # carry numpy.bool_, which stdlib json cannot encode -- this
                # would otherwise crash writing answers.json AFTER the paid
                # matching, not before it. Native types also keep the
                # pending/answers sha256 hashing clean and portable.
                "is_judicial": bool(row.is_judicial),
                "has_unknown_boundaries": bool(row.has_unknown_boundaries),
                "l2_state": answer.l2_state,
                "l2_district_type": answer.l2_district_type,
                "l2_district_name": answer.l2_district_name,
                "confidence": answer.confidence,
            }
        )
    return sorted(rows, key=lambda r: r["br_database_id"])


def _check_prior_pin(prior_out_dir: Path, this_prompt_provenance: dict | None) -> None:
    """The ONE hard-fail: a prompt-pin mismatch means A and B are not the
    same evaluated artifact, and nothing else in the compare can rescue
    that. Called BEFORE the paid run when `--compare-against` is given
    (discovering this only after `run()` has spent the embedding/LLM budget
    would waste the whole run on a comparison that was never valid), and
    again from `compare_runs` itself as a cheap re-assert.
    """
    prior_manifest = json.loads((prior_out_dir / "dry-run-manifest.json").read_text())
    prior_pin = (prior_manifest.get("prompt_provenance") or {}).get("resolved_version")
    this_pin = (this_prompt_provenance or {}).get("resolved_version")
    if prior_pin != this_pin:
        raise RuntimeError(
            f"prompt pin mismatch: prior run resolved {prior_pin!r}, this run resolved {this_pin!r}; not comparable"
        )


def compare_runs(
    this_manifest: dict,
    this_answer_rows: list[dict],
    prior_out_dir: Path,
    excluded_ids: set[int] | frozenset[int] = frozenset(),  # frozenset default: immutable, safe to share
) -> dict:
    """A-vs-B delta report against a prior (Run A) out-dir. Informational
    except the ONE hard-fail -- see `_check_prior_pin`. Everything else is
    review input for the named approver: a small delta focuses their
    review, a large one means it does not transfer.

    `excluded_ids` partitions prior-present/B-absent ids into `excluded`
    (explained: `--exclude-office-ids-file` kept them out of the pending
    list) vs `a_only` (unexplained) -- an id absent from BOTH runs is
    neither, since there is nothing here for the exclusion to explain.
    """
    _check_prior_pin(prior_out_dir, this_manifest.get("prompt_provenance"))
    prior_manifest = json.loads((prior_out_dir / "dry-run-manifest.json").read_text())
    prior_answers = {
        row["br_database_id"]: row for row in json.loads((prior_out_dir / "dry-run-answers.json").read_text())
    }

    config_equal = {
        "embedding_config": prior_manifest.get("embedding_config") == this_manifest.get("embedding_config"),
        "llm_config": prior_manifest.get("llm_config") == this_manifest.get("llm_config"),
    }
    prior_inputs, this_inputs = prior_manifest.get("inputs", {}), this_manifest.get("inputs", {})
    source_equal = {
        "pending_sha256": prior_inputs.get("pending_sha256") == this_inputs.get("pending_sha256"),
        # dry-run's own field predates this convention's name.
        "prior_answers_sha256": (prior_inputs.get("jan_answers_sha256") or prior_inputs.get("prior_answers_sha256"))
        == this_inputs.get("prior_answers_sha256"),
        "universe_sha256": prior_manifest.get("universe", {}).get("key_sha256")
        == this_manifest.get("universe", {}).get("key_sha256"),
    }

    input_keys = [f for f in INPUT_FIELDS if f != "br_database_id"]
    b_only, input_changed, output_flipped = [], [], []
    this_ids = set()
    for row in this_answer_rows:
        this_ids.add(row["br_database_id"])
        prior_row = prior_answers.get(row["br_database_id"])
        if prior_row is None:
            b_only.append(row["br_database_id"])
        elif any(row[f] != prior_row.get(f) for f in input_keys):
            input_changed.append(row["br_database_id"])
        elif any(row[f] != prior_row.get(f) for f in OUTPUT_FIELDS):
            output_flipped.append(row["br_database_id"])

    # Prior-present, B-absent: split by whether the exclusion list explains
    # it. Derived from prior_answers, not excluded_ids, so an id neither run
    # ever attempted (a stale or mistyped exclusion entry) lands in neither
    # bucket -- there is nothing here for it to explain.
    prior_only = set(prior_answers) - this_ids
    excluded = sorted(prior_only & excluded_ids)
    a_only = sorted(prior_only - excluded_ids)

    return {
        "compared_against": str(prior_out_dir),
        "config_equal": config_equal,
        "source_equal": source_equal,
        "row_deltas": {
            "b_only": b_only,
            "input_changed": input_changed,
            "output_flipped": output_flipped,
            "excluded": excluded,
            "a_only": a_only,
        },
    }


# -- run --


def _assert_fresh_out_dir(out_dir: Path) -> None:
    """Evidence durability: a second `run` pointed at an out-dir that
    already has artifacts would silently overwrite the first run's
    manifest/answers/record, exactly the guard the dry-run driver already
    has for its own single answers file.
    """
    existing = [name for name in _RUN_ARTIFACT_FILENAMES if (out_dir / name).exists()]
    if existing:
        raise RuntimeError(f"{out_dir} already has {', '.join(existing)}; refusing to overwrite a completed run")


async def _run(args: argparse.Namespace) -> None:
    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    _assert_fresh_out_dir(out_dir)

    # Installed before anything can call the matcher: a per-office render
    # failure must crash this run, never silently swap in the fallback.
    _matcher_mod.build_cached_prompt = _strict_build_cached_prompt

    run_key = datetime.now(UTC).replace(microsecond=0)  # minted ONCE; nothing else mints one
    embedding_client, llm = _build_clients(args.model_config)
    matcher = L2BrMatcher(embedding_client=embedding_client, llm=llm)
    try:
        _require_pinned_prompt(matcher)
        if args.compare_against:
            # Before the paid run: a pin mismatch found only after run() has
            # spent the embedding/LLM budget would waste it on a comparison
            # that was never going to be valid.
            _check_prior_pin(args.compare_against, matcher.prompt_provenance)

        # Instance-local wrap, installed on THIS matcher only, after
        # construction (it shadows an instance attribute, not the class).
        # run()'s own (only) call to load_pending_offices goes through it,
        # so the frame it captures IS what run() worked from.
        exclude_ids, captured = _maybe_install_exclusion(matcher, args.exclude_office_ids_file)

        results = await matcher.run(
            states=args.states,
            limit=args.limit,
            batch_size=args.batch_size,
            embedding_batch_size=args.embedding_batch_size,
            school_whole_assertion_enabled=args.enable_school_whole_assertion,
        )
        pending_df = captured["pending_df"]
        exclusion_info = {k: captured[k] for k in ("file_sha256", "id_count", "dropped")}
        answer_rows = _answer_rows(pending_df, results)
        matched = sum(1 for row in answer_rows if row["l2_district_name"] is not None)

        universe_key_sha256, universe_per_state = _universe_sha256(matcher)
        manifest = {
            "run_key": run_key.isoformat(),
            "model_config": args.model_config,
            "school_whole_assertion_enabled": args.enable_school_whole_assertion,
            "states_filter": args.states,
            "limit": args.limit,
            "git": _git_state(),
            "exclusions": exclusion_info,
            "offices": len(answer_rows),
            "matched": matched,
            "abstained": len(answer_rows) - matched,
            "inputs": {
                "pending_sha256": _sha([{k: row[k] for k in INPUT_FIELDS} for row in answer_rows]),
                "prior_answers_sha256": _prior_answers_sha256(matcher.databricks, run_key),
            },
            "universe": {"key_sha256": universe_key_sha256, "per_state_sha256": universe_per_state},
            "embedding_config": matcher.embedding_client.resolved_config(),
            "llm_config": matcher.llm.resolved_config(),
            "prompt_provenance": matcher.prompt_provenance,
            "answers_sha256": _sha(answer_rows),
        }
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=1, default=str))
        (out_dir / "answers.json").write_text(json.dumps(answer_rows, indent=1))

        if args.compare_against:
            report = compare_runs(manifest, answer_rows, args.compare_against, excluded_ids=exclude_ids)
            (out_dir / "compare-report.json").write_text(json.dumps(report, indent=1))
            print(f"compare report vs {args.compare_against}: {report['row_deltas']}", flush=True)

        writer = MatchResultWriter(databricks=matcher.databricks)  # shares the matcher's own connection
        predicate_sql = args.cohort_predicate_file.read_text()
        filtered, cohort_counts = apply_cohort_filter(
            matcher.databricks, predicate_sql, args.cohort_expected_count, results
        )
        written = writer.append_results(filtered, attempted_at=run_key)

        record = {
            "run_key": run_key.isoformat(),
            "results_count": len(results),
            "written": written,
            "matched": matched,
            "abstained": len(results) - matched,
            "cohort": cohort_counts,
            "exclusions": exclusion_info,
            "manifest_path": str(out_dir / "manifest.json"),
            "published": False,  # this entry point never triggers the swap; rollback's --published overrides
        }
        (out_dir / "run-record.json").write_text(json.dumps(record, indent=1))
        print(f"wrote {written} row(s) under run key {run_key.isoformat()}", flush=True)
    finally:
        flush_logs()
        try:
            matcher.close()
        except Exception:
            matcher.logger.warning("matcher.close() raised during teardown", exc_info=True)


# -- rollback --


def _require_env(names: tuple[str, ...]) -> None:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise RuntimeError(f"missing required env var(s): {', '.join(missing)}")


def _poll_until_terminal(
    fetch_state, terminal: dict[str, bool], label: str, poll_seconds: float, timeout_seconds: float
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        state = fetch_state()
        if state in terminal:
            if not terminal[state]:
                raise RuntimeError(f"{label} finished in a failing state: {state!r}")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"{label} did not reach a terminal state within {timeout_seconds}s (last seen: {state!r})"
            )
        time.sleep(poll_seconds)


def _trigger_dbt_rebuild_and_wait(poll_seconds: float = 30, timeout_seconds: float = 10800) -> None:
    # The scheduled build runs ~95-105 minutes (measured 2026-08-31); a 30-minute
    # budget would make every real rollback falsely report failure mid-rebuild.
    base, account_id = os.environ["DBT_CLOUD_BASE_URL"], os.environ["DBT_CLOUD_ACCOUNT_ID"]
    headers = {"Authorization": f"Token {os.environ['DBT_CLOUD_API_TOKEN']}"}
    with httpx.Client(timeout=30) as client:
        started = client.post(
            f"{base}/api/v2/accounts/{account_id}/jobs/{DBT_CLOUD_REBUILD_JOB_ID}/run/",
            headers=headers,
            json={"cause": "gold-match rollback rebuild"},
        )
        started.raise_for_status()
        run_id = started.json()["data"]["id"]

        def state() -> str:
            r = client.get(f"{base}/api/v2/accounts/{account_id}/runs/{run_id}/", headers=headers)
            r.raise_for_status()
            return r.json()["data"]["status_humanized"].lower()

        _poll_until_terminal(
            state, {"success": True, "error": False, "cancelled": False}, "dbt rebuild", poll_seconds, timeout_seconds
        )


def _assert_swap_succeeded(task_state: str) -> None:
    """A DAG run can succeed while the swap itself was silently skipped:
    `cutover_enabled` short-circuits every downstream task (including
    `swap`) whenever the gate variable isn't exactly "true", and Airflow
    counts a skipped task as a normal outcome, not a DAG failure -- so the
    DAG-level state alone cannot catch this.
    """
    if task_state != "success":
        raise RuntimeError(
            f"election-api sync DAG succeeded but its swap task state is {task_state!r}, not 'success' -- "
            f"the {ELECTION_API_SWAP_VARIABLE!r} Airflow variable is likely not exactly 'true'"
        )


def _trigger_election_api_sync_and_wait(poll_seconds: float = 30, timeout_seconds: float = 1800) -> None:
    base = os.environ["AIRFLOW_API_BASE_URL"]
    headers = {"Authorization": f"Bearer {os.environ['AIRFLOW_API_TOKEN']}"}
    with httpx.Client(timeout=30) as client:
        # Airflow 3 requires the logical_date KEY (null = server-assigned manual
        # run); an empty body is a 422, live-confirmed against astro-prod.
        started = client.post(
            f"{base}/api/v2/dags/{ELECTION_API_SYNC_DAG_ID}/dagRuns", headers=headers, json={"logical_date": None}
        )
        started.raise_for_status()
        run_id = started.json()["dag_run_id"]

        def state() -> str:
            r = client.get(f"{base}/api/v2/dags/{ELECTION_API_SYNC_DAG_ID}/dagRuns/{run_id}", headers=headers)
            r.raise_for_status()
            return r.json()["state"]

        _poll_until_terminal(
            state, {"success": True, "failed": False}, "election-api sync", poll_seconds, timeout_seconds
        )

        swap = client.get(
            f"{base}/api/v2/dags/{ELECTION_API_SYNC_DAG_ID}/dagRuns/{run_id}/taskInstances/{ELECTION_API_SWAP_TASK_ID}",
            headers=headers,
        )
        swap.raise_for_status()
        _assert_swap_succeeded(swap.json()["state"])


def _assert_no_later_runs(databricks: DatabricksClient, run_key: datetime) -> None:
    """The writer's own documented repair for undoing a release is deleting
    the target run key AND every run after it, together -- rolling back the
    target alone while a later run's rows still stand would leave that
    later run's answers un-rolled-back and the table's newest-row-wins
    contract half-undone. Fail closed rather than doing the partial version.
    """
    # isoformat, not strftime: see _prior_answers_sha256 -- the same session-
    # timezone risk applies to any unzoned timestamp literal built here.
    boundary = run_key.isoformat(sep=" ", timespec="seconds")
    df = databricks.execute_query(
        f"select count(*) as n from {RESULTS_TABLE_PATH} where attempted_at > timestamp'{boundary}'"
    )
    later = int(df["n"][0])
    if later > 0:
        raise RuntimeError(
            f"{later} row(s) exist under a run key later than {run_key.isoformat()}; rolling back the "
            "target key alone would leave a later run half-undone -- delete the target and every "
            "later key together, per delete_run's own documented contract"
        )


def _rollback(args: argparse.Namespace) -> None:
    if args.run_record:
        record = json.loads(args.run_record.read_text())
        run_key = datetime.fromisoformat(record["run_key"])
        expected_count = record["written"]
        published = bool(record.get("published")) or args.published
    else:
        run_key = args.run_key
        expected_count = args.expected_count
        published = args.published

    # Fail loud before deleting anything -- a missing credential must never
    # surface only after the rows are already gone.
    _require_env(_DBT_CLOUD_ENV_VARS)
    if published:
        _require_env(_AIRFLOW_ENV_VARS)

    writer = MatchResultWriter()
    delete_error: Exception | None = None
    try:
        _assert_no_later_runs(writer.databricks, run_key)
        try:
            writer.delete_run(run_key, expected_count=expected_count)
        except Exception as exc:
            # Delta DELETE commits atomically, so a raise here -- the
            # pre-check, or a transient on delete_run's own post-delete
            # verification query -- does not mean nothing changed. The
            # rebuild below must still run (rebuilding when nothing
            # actually changed is an idempotent no-op); the sync must
            # NOT run regardless of --published, since it would push a
            # delete of uncertain outcome out to PostgreSQL.
            delete_error = exc
            writer.logger.warning(
                f"delete_run raised for {run_key.isoformat()}; proceeding to the rebuild and skipping "
                "the sync (see the final raise after it)",
                exc_info=True,
            )
    finally:
        # A raise here must not skip the rebuild trigger below either: the
        # delete attempt already happened, so swallowing a teardown failure
        # is better than leaving every consumer unrebuilt until the next
        # scheduled build.
        try:
            writer.close()
        except Exception:
            writer.logger.warning("writer.close() raised during teardown", exc_info=True)

    _trigger_dbt_rebuild_and_wait()
    if published and delete_error is None:
        _trigger_election_api_sync_and_wait()
    if delete_error is not None:
        raise RuntimeError(
            f"delete_run raised for {run_key.isoformat()} (rebuild ran; sync skipped): {delete_error}"
        ) from delete_error


# -- CLI --


def _iso_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not a valid ISO-8601 timestamp: {value!r}") from exc
    # Reject here, not later: a naive value would reach _assert_no_later_runs'
    # own SQL before delete_run's _require_aware ever sees it.
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"--run-key must be timezone-aware, got a naive timestamp: {value!r}")
    return parsed


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Match the backlog and write results under one freshly minted run key.")
    run_p.add_argument(
        "--states", nargs="+", type=_canonical_state_arg, help="Limit to these state codes (e.g. --states DE CA)"
    )
    run_p.add_argument("--limit", type=_positive_int, help="Limit the number of pending offices read (positive)")
    run_p.add_argument(
        "--batch-size", type=_positive_int, default=100, help="Offices matched concurrently per group (default: 100)"
    )
    run_p.add_argument(
        "--embedding-batch-size",
        type=_positive_int,
        default=100,
        help="District texts embedded per call (default: 100)",
    )
    run_p.add_argument(
        "--enable-school-whole-assertion",
        action="store_true",
        help="Deny school SUB-level types; off by default per the gate decision",
    )
    run_p.add_argument(
        "--model-config",
        choices=["bedrock"],
        default="bedrock",
        help="the gate-evaluated stack; the only choice production accepts -- any other config is a "
        "semantics change requiring a fresh gating decision (bedrock-nova's embeddings never passed "
        "the gate and gemini's clients lack resolved_config())",
    )
    run_p.add_argument(
        "--cohort-predicate-file",
        required=True,
        type=Path,
        help="SQL file selecting the approved br_database_id set from the dry-run enriched table "
        "(wholesale publication is a predicate that selects every row, never a bypass)",
    )
    run_p.add_argument(
        "--cohort-expected-count", required=True, type=int, help="recorded row count the predicate must return"
    )
    run_p.add_argument(
        "--compare-against", type=Path, default=None, help="a prior (Run A) out-dir; produces the A-vs-B delta report"
    )
    run_p.add_argument(
        "--exclude-office-ids-file",
        type=Path,
        default=None,
        help="plain text, one br_database_id per line (# comments allowed); excluded ids never reach "
        "match_office -- production run() has no per-office catch, so a known shape-miss office would "
        "otherwise abort the whole run pre-write",
    )
    run_p.add_argument("--out-dir", required=True, type=Path)

    rb_p = sub.add_parser("rollback", help="Count-verified delete of a run, then rebuild and (if published) re-sync.")
    rb_group = rb_p.add_mutually_exclusive_group(required=True)
    rb_group.add_argument("--run-record", type=Path, help="the run subcommand's own run-record.json")
    rb_group.add_argument("--run-key", type=_iso_timestamp, help="attempted_at, if the run record is unavailable")
    rb_p.add_argument("--expected-count", type=int, help="required with --run-key")
    rb_p.add_argument("--published", action="store_true", help="force (or confirm) the PostgreSQL re-swap")

    args = parser.parse_args(argv)
    if args.command == "rollback" and args.run_key is not None and args.expected_count is None:
        parser.error("--run-key requires --expected-count")
    return args


def main() -> None:
    args = _parse_args()
    if args.command == "run":
        asyncio.run(_run(args))
    else:
        _rollback(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise
