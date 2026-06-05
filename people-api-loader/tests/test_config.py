"""Guard test for the forbidden-env-var security contract.

CLAUDE.md "Never" rule: the loader must never read `VOTER_DB_MASTER_PASSWORD`
from the environment. `LoaderConfig.from_env()` hard-fails if it is set. This
test pins that behavior so a future refactor cannot silently drop the guard.
"""

from __future__ import annotations

import pytest

from loader.people_api.config import LoaderConfig


def test_from_env_rejects_forbidden_password_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOTER_DB_MASTER_PASSWORD", "secret")
    with pytest.raises(RuntimeError, match="VOTER_DB_MASTER_PASSWORD"):
        LoaderConfig.from_env()
