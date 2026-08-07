"""promote: serving cutover — overwrite the single serving parameter to a new latest version,
best-effort label it `build-{date}`, and refuse unless validate passed."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import promote as step

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        new_conn_param=lambda rd: f"people-db-connection-string-prod-{rd}",
        db_conn_param="people-db-connection-string-prod",
    ),
)


def _manifests(monkeypatch: pytest.MonkeyPatch, *, promote: object, validate: object) -> None:
    def read(cfg: object, rd: str, name: str, model: object) -> object:
        return {"promote": promote, "validate": validate}[name]

    monkeypatch.setattr(step, "read_manifest", read)


def _patch_io(monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], *, label_fn: Any) -> None:
    """Patch promote's SSM reads/writes; label_fn stands in for label_ssm_parameter_version."""

    def fake_get(cfg: object, name: str) -> str:
        calls["get"] = name
        return "postgresql://serving"

    def fake_overwrite(cfg: object, name: str, value: str) -> int:
        calls["put"] = (name, value)
        return 7

    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "get_ssm_parameter", fake_get)
    monkeypatch.setattr(step, "overwrite_ssm_parameter", fake_overwrite)
    monkeypatch.setattr(step, "label_ssm_parameter_version", label_fn)


def test_promote_overwrites_serving_param_and_labels_version(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, Any] = {}
    _manifests(monkeypatch, promote=None, validate=SimpleNamespace(status="complete", all_passed=True))

    def fake_label(cfg: object, name: str, version: int, labels: list[str]) -> list[str]:
        calls["label"] = (name, version, labels)
        return []  # empty InvalidLabels — the label stuck

    _patch_io(monkeypatch, calls, label_fn=fake_label)
    manifest = step.run(_CFG, "20260728")

    assert calls["get"] == "people-db-connection-string-prod-20260728"
    # The cutover: overwrite the un-dated serving param to a new latest version.
    assert calls["put"] == ("people-db-connection-string-prod", "postgresql://serving")
    # Best-effort `build-{date}` label on that exact version (prefixed — a digit-leading label is
    # rejected by SSM). No `live` pointer: people-api reads the latest version.
    assert calls["label"] == ("people-db-connection-string-prod", 7, ["build-20260728"])
    assert manifest.status == "complete"
    assert manifest.serving_param == "people-db-connection-string-prod"
    assert manifest.version == 7
    assert manifest.labels == ["build-20260728"]


def test_promote_completes_when_label_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    # Labeling is best-effort bookkeeping — the overwrite already cut over — so a rejected label
    # (SSM returns it in InvalidLabels, e.g. at the per-version 10-label cap) must not fail promote.
    calls: dict[str, Any] = {}
    _manifests(monkeypatch, promote=None, validate=SimpleNamespace(status="complete", all_passed=True))
    _patch_io(monkeypatch, calls, label_fn=lambda cfg, name, version, labels: list(labels))
    manifest = step.run(_CFG, "20260728")
    assert calls["put"] == ("people-db-connection-string-prod", "postgresql://serving")  # cutover happened
    assert manifest.status == "complete"
    assert manifest.version == 7
    assert manifest.labels == []  # nothing stuck, but promote still completed


def test_promote_completes_when_label_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # A raised ClientError while labeling is likewise swallowed — the cutover stands, promote completes.
    calls: dict[str, Any] = {}
    _manifests(monkeypatch, promote=None, validate=SimpleNamespace(status="complete", all_passed=True))

    def boom(cfg: object, name: str, version: int, labels: list[str]) -> list[str]:
        raise ClientError(
            {"Error": {"Code": "ParameterVersionLabelLimitExceeded", "Message": "x"}},
            "LabelParameterVersion",
        )

    _patch_io(monkeypatch, calls, label_fn=boom)
    manifest = step.run(_CFG, "20260728")
    assert calls["put"] == ("people-db-connection-string-prod", "postgresql://serving")
    assert manifest.status == "complete"
    assert manifest.labels == []


def test_promote_refuses_without_green_validate(monkeypatch: pytest.MonkeyPatch) -> None:
    for validate in (None, SimpleNamespace(status="complete", all_passed=False)):
        _manifests(monkeypatch, promote=None, validate=validate)
        # Any SSM write here would be a bug — promote must abort before touching serving.
        monkeypatch.setattr(step, "overwrite_ssm_parameter", lambda *a, **k: pytest.fail("must not write"))
        with pytest.raises(RuntimeError, match="promote refused"):
            step.run(_CFG, "20260728")


def test_promote_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    _manifests(monkeypatch, promote=done, validate=None)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    monkeypatch.setattr(step, "overwrite_ssm_parameter", lambda *a, **k: pytest.fail("must not write"))
    assert step.run(_CFG, "20260728") is done
