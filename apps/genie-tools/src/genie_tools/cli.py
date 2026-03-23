from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from .export_space import export_space_bundle
from .validate_space import validate_space_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export and validate Databricks Genie space config"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser(
        "export", help="Export a Genie space into repo-friendly JSON artifacts"
    )
    export_parser.add_argument("--space-id", required=True, help="Genie space ID")
    export_parser.add_argument(
        "--out-dir",
        required=True,
        type=Path,
        help="Destination directory for raw/, normalized/, redacted/, and metadata/",
    )
    export_parser.add_argument(
        "--write-redacted",
        action="store_true",
        help="Write a lightweight redacted JSON artifact for LLM context",
    )
    export_parser.add_argument(
        "--write-metadata",
        action="store_true",
        help="Write export metadata alongside the raw and normalized JSON",
    )

    validate_parser = subparsers.add_parser(
        "validate", help="Run lightweight checks on a normalized Genie export"
    )
    validate_parser.add_argument(
        "--file",
        required=True,
        type=Path,
        help="Path to a normalized Genie JSON file",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "export":
            artifacts = export_space_bundle(
                space_id=args.space_id,
                out_dir=args.out_dir,
                write_redacted=args.write_redacted,
                write_metadata=args.write_metadata,
            )
            print(f"WROTE raw: {artifacts.raw_path}")
            print(f"WROTE normalized: {artifacts.normalized_path}")
            if artifacts.redacted_path is not None:
                print(f"WROTE redacted: {artifacts.redacted_path}")
            if artifacts.metadata_path is not None:
                print(f"WROTE metadata: {artifacts.metadata_path}")
            return 0

        if args.command == "validate":
            validate_space_file(args.file)
            print(f"VALIDATION PASSED: {args.file}")
            return 0
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
