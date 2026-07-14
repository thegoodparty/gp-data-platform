"""Smoke tests for the loader CLI.

These tests do not hit AWS or any database. They confirm the Typer app is
wired up and every documented subcommand is registered.
"""

from __future__ import annotations

import re

from typer.testing import CliRunner

from loader.people_api.cli import app

# Typer renders help via Rich panels. On narrow CI terminals the literal
# token "--date" can be split across cells by ANSI escapes or wrapped lines,
# so we strip ANSI and collapse whitespace before substring matching.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

runner = CliRunner()

EXPECTED_COMMANDS = {
    "inspect-prod",
    "unload",
    "provision",
    "create-schema",
    "copy",
    "build-indexes",
    "resize",
    "validate",
    "teardown",
    "status",
}


def _plain(output: str) -> str:
    return re.sub(r"\s+", " ", _ANSI_RE.sub("", output))


def test_help_lists_all_subcommands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0, result.output
    plain = _plain(result.output)
    for cmd in EXPECTED_COMMANDS:
        assert cmd in plain, f"missing subcommand {cmd!r} in --help output"


def test_status_help_renders() -> None:
    result = runner.invoke(app, ["status", "--help"])
    assert result.exit_code == 0, result.output
    assert "--date" in _plain(result.output)


def test_build_indexes_parallelism_defaults_to_module_constant() -> None:
    # The CLI default must track build_indexes._DEFAULT_BUILDERS (128, tuned for the index box)
    # rather than a stale hardcoded literal — see the 32-vs-128 under-parallelization finding.
    from loader.people_api.steps.build_indexes import _DEFAULT_BUILDERS

    result = runner.invoke(app, ["build-indexes", "--help"])
    assert result.exit_code == 0, result.output
    assert f"[default: {_DEFAULT_BUILDERS}]" in _plain(result.output)
