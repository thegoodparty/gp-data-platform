from __future__ import annotations

import pytest

from hsmerge.cli import build_parser, main


def test_run_parses_with_and_without_dry_run() -> None:
    assert build_parser().parse_args(["run"]).dry_run is False
    assert build_parser().parse_args(["run", "--dry-run"]).dry_run is True


def test_init_log_and_verify_parse() -> None:
    assert build_parser().parse_args(["init-log"]).command == "init-log"
    assert build_parser().parse_args(["verify", "--run-id", "abc"]).run_id == "abc"


def test_verify_requires_a_run_id() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["verify"])


def test_an_unwired_command_exits_nonzero_rather_than_reporting_success() -> None:
    """Catches: a stub returning 0, which a scheduled pod would read as a clean run."""
    with pytest.raises(SystemExit) as excinfo:
        main(["run"])
    assert excinfo.value.code != 0
