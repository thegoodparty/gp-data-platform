"""Shared test fixtures.

`LoaderConfig.from_env()` now requires `LOADER_S3_BUCKET` (the loader bucket is real
infrastructure and is not hardcoded in this public repo) and `ENVIRONMENT` (no silent
default — an unset value would let prod run as dev). Tests that build a config via
`from_env()` aren't exercising those, so supply defaults for the whole suite. Any test
that needs a variable unset or different (e.g. the guard tests) can `monkeypatch.delenv`
or override it.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _default_loader_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_S3_BUCKET", "test-loader-bucket")
    monkeypatch.setenv("ENVIRONMENT", "dev")
