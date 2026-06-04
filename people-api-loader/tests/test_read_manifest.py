"""Tests for loader.core.manifest.io.read_manifest error discrimination.

The status command's tests patch read_manifest out entirely, so the actual
"is this a missing manifest or a real AWS error" logic is never exercised
there. These tests drive it directly with a stubbed S3 client (no moto/AWS):
NoSuchKey -> None, any other ClientError -> re-raise, present -> parsed model.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from botocore.exceptions import ClientError

from loader.core.config import BaseLoaderConfig
from loader.core.manifest.io import read_manifest
from loader.core.manifest.models import ManifestBase


def _cfg() -> BaseLoaderConfig:
    return cast(
        BaseLoaderConfig,
        SimpleNamespace(
            s3_bucket="test-bucket",
            manifest_key=lambda run_date, step: f"voter_export_{run_date}/_manifest/{step}.json",
        ),
    )


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


def _patch_s3(monkeypatch: pytest.MonkeyPatch, get_object) -> None:
    monkeypatch.setattr(
        "loader.core.manifest.io.s3",
        lambda cfg: SimpleNamespace(get_object=get_object),
    )


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "x"}}, "GetObject")


def test_missing_manifest_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    def get_object(**kwargs: object) -> dict:
        raise _client_error("NoSuchKey")

    _patch_s3(monkeypatch, get_object)
    assert read_manifest(_cfg(), "20260101", "inspect", ManifestBase) is None


def test_unexpected_client_error_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    def get_object(**kwargs: object) -> dict:
        raise _client_error("AccessDenied")

    _patch_s3(monkeypatch, get_object)
    with pytest.raises(ClientError):
        read_manifest(_cfg(), "20260101", "inspect", ManifestBase)


def test_present_manifest_is_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    body = (
        b'{"schema_version": 1, "run_date": "20260101", "step": "inspect", '
        b'"status": "complete", "started_at": "2026-01-01T00:00:00Z"}'
    )

    def get_object(**kwargs: object) -> dict:
        return {"Body": _Body(body)}

    _patch_s3(monkeypatch, get_object)
    m = read_manifest(_cfg(), "20260101", "inspect", ManifestBase)
    assert m is not None
    assert m.status == "complete"
    assert m.run_date == "20260101"
