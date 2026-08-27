"""Subproject pytest config.

Replicates omni's root-conftest autouse guard: with a real BRAINTRUST_API_KEY
in a developer's .env, these tests would authenticate and emit live Braintrust
telemetry. Clearing the key keeps BraintrustClient disabled for each test
(traced_span, traced_call, and load_prompt become no-ops); resetting the
singleton stops state leaking between tests.
"""

import pytest

from shared.braintrust import BraintrustClient


@pytest.fixture(autouse=True)
def disable_braintrust(monkeypatch):
    monkeypatch.setenv("BRAINTRUST_API_KEY", "")
    BraintrustClient.reset_instance()
    yield
    BraintrustClient.reset_instance()
