from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from hsmerge import cli
from hsmerge.client import WrongPortalError
from hsmerge.types import Pair
from tests._fakes import FakeHubSpot, FakeLog
from tests.test_config import FULL_ENV


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """The CLI with the warehouse, HubSpot and the log replaced by fakes."""
    for name, value in FULL_ENV.items():
        monkeypatch.setenv(name, value)
    state: dict[str, Any] = {
        "hubspot": FakeHubSpot(live={"p1", "s1", "p2", "p3"}, absorbed={"s2": "p2"}),
        "log": FakeLog(),
        "init": [],
    }

    @contextmanager
    def connect(config: object) -> Iterator[object]:
        yield object()

    monkeypatch.setattr(cli, "connect", connect)
    monkeypatch.setattr(cli, "HubSpotClient", lambda config: state["hubspot"])
    monkeypatch.setattr(cli, "MergeLog", lambda *args, **kwargs: state["log"])
    monkeypatch.setattr(cli, "init_log_table", lambda connection, table: state["init"].append(table))
    monkeypatch.setattr(
        cli, "read_pairs", lambda *args: [Pair("p1", "s1"), Pair("p2", "s2"), Pair("p3", "gone")]
    )
    monkeypatch.setattr(cli, "read_merged_pairs", lambda *args: [Pair("p1", "s1")])
    return state


def test_run_merges_and_logs(wired: dict[str, Any]) -> None:
    assert cli.main(["run"]) == 0
    assert wired["hubspot"].merges == [("p1", "s1")]
    assert len(wired["log"].rows) == 3


def test_a_run_where_every_merge_fails_exits_1(wired: dict[str, Any]) -> None:
    wired["hubspot"].refuse.add("s1")
    assert cli.main(["run"]) == 1


def test_a_run_refuses_a_missing_log_before_any_merge(
    wired: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def missing() -> None:
        raise RuntimeError("table not found")

    monkeypatch.setattr(wired["log"], "check", missing)
    with pytest.raises(RuntimeError):
        cli.main(["run"])
    assert wired["hubspot"].merges == []


def test_dry_run_neither_merges_nor_logs(wired: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["run", "--dry-run"]) == 0
    assert wired["hubspot"].merges == []
    assert wired["log"].writes == []
    assert "already_merged=1, secondary_missing=1, would_merge=1" in capsys.readouterr().out


def test_init_log_creates_the_table_and_calls_no_hubspot(wired: dict[str, Any]) -> None:
    assert cli.main(["init-log"]) == 0
    assert wired["init"] == [FULL_ENV["MERGE_LOG_TABLE"]]
    assert wired["hubspot"].reads == 0


def test_verify_fails_when_a_merged_pair_is_not_merged(wired: dict[str, Any]) -> None:
    assert cli.main(["verify", "--run-id", "r1"]) == 1
    wired["hubspot"].absorbed["s1"] = "p1"
    assert cli.main(["verify", "--run-id", "r1"]) == 0


def test_a_wrong_portal_stops_the_run_before_any_read(
    wired: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def wrong_portal() -> None:
        raise WrongPortalError("21589597", "51780263")

    monkeypatch.setattr(wired["hubspot"], "verify_portal", wrong_portal)
    with pytest.raises(WrongPortalError):
        cli.main(["run"])
    assert wired["hubspot"].reads == 0
