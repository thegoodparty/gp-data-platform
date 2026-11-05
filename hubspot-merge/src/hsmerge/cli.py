"""hsm: `run [--dry-run]`, `init-log` and `verify --run-id ID`. Configured by the environment."""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from collections import Counter
from collections.abc import Sequence

from .client import HubSpotClient
from .config import Config
from .databricks import connect
from .execute import execute, preview, verify
from .log import MergeLog, init_log_table
from .pairs import read_merged_pairs, read_pairs
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
    with connect(config) as connection:
        if args.command == "init-log":
            init_log_table(connection, config.log_table)
            print(f"log table ready: {config.log_table}")
            return 0

        hubspot = HubSpotClient(config)
        hubspot.verify_portal()

        if args.command == "verify":
            merged = read_merged_pairs(connection, config.log_table, args.run_id)
            problems = list(verify(merged, hubspot))
            for pair, problem in problems:
                print(f"{pair.primary_id} <- {pair.secondary_id}: {problem}", file=sys.stderr)
            print(f"verified {len(merged)} merged pairs, {len(problems)} problems")
            return 1 if problems else 0

        pairs = read_pairs(connection, config.pair_relation, config.max_pairs)
        if args.dry_run:
            planned: Counter[str] = Counter()
            for pair, outcome in preview(pairs, hubspot):
                status = outcome.status.value if outcome else "would_merge"
                planned[status] += 1
                print(f"{pair.primary_id} <- {pair.secondary_id}: {status}")
            print(f"dry run: {_summary(planned)}")
            return 0

        run_id = str(uuid.uuid4())
        log = MergeLog(connection, config.log_table, run_id=run_id, object_type=config.object_type)
        statuses = execute(pairs, hubspot, log)
        print(f"run {run_id}: {_summary(Counter({s.value: n for s, n in statuses.items()}))}")
        # A merge that returned another id means the ID preservation beta is off.
        return 1 if statuses[Status.ID_CHANGED] else 0


def _summary(counts: Counter[str]) -> str:
    return ", ".join(f"{status}={count}" for status, count in sorted(counts.items())) or "no pairs"


if __name__ == "__main__":
    raise SystemExit(main())
