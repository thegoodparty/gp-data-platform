"""hsm: `run [--dry-run]`, `init-log` and `verify --run-id ID`. Configured by the environment."""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from collections import Counter
from collections.abc import Sequence

from .client import HubSpotClient
from .config import Config, RunSettings
from .databricks import connect
from .execute import execute, preview, verify
from .log import MergeLog, init_log_table
from .pairs import check_pairs, read_merged_pairs, read_pairs
from .types import Status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hsm", description="Merge duplicate HubSpot contacts.")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="merge the pairs in MERGE_PAIR_RELATION, up to MERGE_MAX_PAIRS")
    run.add_argument("--dry-run", action="store_true", help="read and classify only; merge and log nothing")
    commands.add_parser("init-log", help="create MERGE_LOG_TABLE if it does not exist")
    check = commands.add_parser("verify", help="check one run's merged pairs against HubSpot")
    check.add_argument("--run-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = Config.from_env(os.environ)
    if args.command == "init-log":
        return _init_log(config)
    if args.command == "verify":
        return _verify(config, args.run_id)
    return _run(config, RunSettings.from_env(os.environ), dry_run=args.dry_run)


def _init_log(config: Config) -> int:
    with connect(config) as connection:
        init_log_table(connection, config.log_table)
    print(f"log table ready: {config.log_table}")
    return 0


def _verify(config: Config, run_id: str) -> int:
    hubspot = HubSpotClient(config)
    hubspot.verify_portal()
    with connect(config) as connection:
        merged = read_merged_pairs(connection, config.log_table, run_id)
    problems = list(verify(merged, hubspot))
    for pair, problem in problems:
        print(f"{pair.primary_id} <- {pair.secondary_id}: {problem}", file=sys.stderr)
    print(f"verified {len(merged)} merged pairs, {len(problems)} problems")
    return 1 if problems else 0


def _run(config: Config, settings: RunSettings, *, dry_run: bool) -> int:
    hubspot = HubSpotClient(config)
    hubspot.verify_portal()
    with connect(config) as connection:
        pairs = read_pairs(connection, settings.pair_relation, settings.max_pairs)
        check_pairs(pairs)
        if dry_run:
            planned: Counter[str] = Counter()
            for pair, outcome in preview(pairs, hubspot):
                status = outcome.status.value if outcome else "would_merge"
                planned[status] += 1
                print(f"{pair.primary_id} <- {pair.secondary_id}: {status}")
            print(f"dry run: {_summary(planned)}")
            return 0

        run_id = str(uuid.uuid4())
        log = MergeLog(connection, config.log_table, run_id=run_id, object_type=config.object_type)
        log.check()
        statuses = execute(pairs, hubspot, log)
    print(f"run {run_id}: {_summary(Counter({s.value: n for s, n in statuses.items()}))}")
    # id_changed means the ID preservation beta is off. Failures with no merge at all
    # usually mean the key cannot merge.
    if statuses[Status.ID_CHANGED] or (statuses[Status.FAILED] and not statuses[Status.MERGED]):
        return 1
    return 0


def _summary(counts: Counter[str]) -> str:
    return ", ".join(f"{status}={count}" for status, count in sorted(counts.items())) or "no pairs"


if __name__ == "__main__":
    raise SystemExit(main())
