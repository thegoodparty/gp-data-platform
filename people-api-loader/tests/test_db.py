"""Unit tests for loader.core.db.password_from_secret.

Covers the three branches: raw-string secret, JSON secret keyed on each
accepted name, and JSON secret with no accepted key (RuntimeError). `get_secret`
is patched so no AWS call is made.
"""

from __future__ import annotations

import json
from typing import cast

import pytest

from loader.core.config import BaseLoaderConfig
from loader.core.db import password_from_secret

_CFG = cast(BaseLoaderConfig, object())  # unused once get_secret is patched


def _patch_secret(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setattr("loader.core.db.get_secret", lambda cfg, secret_id: value)


def test_raw_string_secret_returned_stripped(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_secret(monkeypatch, "  hunter2  ")
    assert password_from_secret(_CFG, "sid") == "hunter2"


@pytest.mark.parametrize("key", ["password", "Password", "MasterUserPassword"])
def test_json_secret_with_known_key(monkeypatch: pytest.MonkeyPatch, key: str) -> None:
    _patch_secret(monkeypatch, json.dumps({key: "s3cret"}))
    assert password_from_secret(_CFG, "sid") == "s3cret"


def test_json_secret_without_known_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_secret(monkeypatch, json.dumps({"username": "postgres"}))
    with pytest.raises(RuntimeError, match="no 'password' key"):
        password_from_secret(_CFG, "sid")
