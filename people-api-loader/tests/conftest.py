"""Shared test fixtures.

`LoaderConfig.from_env()` now requires `LOADER_S3_BUCKET` (the loader bucket is real
infrastructure and is not hardcoded in this public repo). Tests that build a config via
`from_env()` aren't exercising the bucket, so supply a dummy value for the whole suite.
Any test that needs the variable unset (e.g. the guard test) can `monkeypatch.delenv` it.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _default_loader_s3_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOADER_S3_BUCKET", "test-loader-bucket")
