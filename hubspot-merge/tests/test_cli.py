from __future__ import annotations

from hsmerge.cli import main


def test_main_reports_failure_rather_than_exiting_zero() -> None:
    """A stub that returns 0 would read as a clean scheduled run."""
    assert main() != 0
