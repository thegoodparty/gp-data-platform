"""promote: serving cutover — copy the run's conn string into the single serving parameter, label
the new version `build-{date}` and move the `live` pointer to it, and refuse unless validate passed."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import promote as step

_CFG = cast(
    LoaderConfig,
    SimpleNamespace(
        new_conn_param=lambda rd: f"people-db-connection-string-prod-{rd}",
        db_conn_param="people-db-connection-string-prod",
    ),
)


def _manifests(monkeypatch: pytest.MonkeyPatch, *, promote, validate) -> None:
    def read(cfg, rd, name, model):
        return {"promote": promote, "validate": validate}[name]

    monkeypatch.setattr(step, "read_manifest", read)


def test_promote_puts_serving_param_and_labels_version(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}
    _manifests(
        monkeypatch,
        promote=None,
        validate=SimpleNamespace(status="complete", all_passed=True),
    )

    def fake_get(cfg, name):
        calls["get"] = name
        return "postgresql://serving"

    def fake_put(cfg, name, value):
        calls["put"] = (name, value)
        return 7

    def fake_label(cfg, name, version, labels):
        calls["label"] = (name, version, labels)

    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "get_ssm_parameter", fake_get)
    monkeypatch.setattr(step, "put_ssm_parameter", fake_put)
    monkeypatch.setattr(step, "label_ssm_parameter_version", fake_label)

    manifest = step.run(_CFG, "20260728")

    # Reads the dated param provision wrote, writes the un-dated serving param.
    assert calls["get"] == "people-db-connection-string-prod-20260728"
    assert calls["put"] == ("people-db-connection-string-prod", "postgresql://serving")
    # Labels the exact version the put produced: the per-refresh anchor `build-{date}` (prefixed,
    # since a bare digit-leading label is rejected by SSM) and the moving `live` pointer. Applying
    # `live` here moves it off the prior version — that is the cutover.
    assert calls["label"] == ("people-db-connection-string-prod", 7, ["build-20260728", "live"])
    assert manifest.status == "complete"
    assert manifest.serving_param == "people-db-connection-string-prod"
    assert manifest.version == 7
    assert manifest.labels == ["build-20260728", "live"]


def test_promote_refuses_without_green_validate(monkeypatch: pytest.MonkeyPatch) -> None:
    for validate in (None, SimpleNamespace(status="complete", all_passed=False)):
        _manifests(monkeypatch, promote=None, validate=validate)
        # Any SSM write here would be a bug — promote must abort before touching serving.
        monkeypatch.setattr(step, "put_ssm_parameter", lambda *a, **k: pytest.fail("must not write"))
        with pytest.raises(RuntimeError, match="promote refused"):
            step.run(_CFG, "20260728")


def test_promote_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    _manifests(monkeypatch, promote=done, validate=None)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    monkeypatch.setattr(step, "put_ssm_parameter", lambda *a, **k: pytest.fail("must not write"))
    assert step.run(_CFG, "20260728") is done
