"""Suite-wide guards.

The semantic catalog reads omni's instrument-health state over the network to
decide whether a build approval needs re-verification. Tests must never make
that call: it would be slow, it would depend on ambient `gh` auth, and a test
machine that happened to have a token would exercise a different code path than
one that did not.

Switching both paths off leaves `evidence.load_latches` on its degrade branch,
which is a real branch with its own tests, so nothing here hides a failure.
"""

import pytest
from semantic_catalog import evidence


@pytest.fixture(autouse=True)
def _no_cross_repo_reads(monkeypatch):
    monkeypatch.delenv(evidence.TOKEN_ENV, raising=False)
    monkeypatch.setenv(evidence.GH_FALLBACK_ENV, "1")
