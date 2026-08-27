"""Prompt-integrity guards: build_cached_prompt must flatten every message
and fall back loudly on an emptied or broken remote prompt, and the prompt
load must honor a pinned version (the registry prompt is org-wide editable
and its creator has left; an unpinned slug is live risk)."""

from unittest.mock import patch

import pytest

from shared.braintrust import (
    BraintrustClient,
    build_cached_prompt,
    cache_prompt,
    get_prompt_provenance,
)


class FakePromptObj:
    def __init__(self, rendered, version="xact-123", version_error=None):
        self._rendered = rendered
        self._version = version
        self._version_error = version_error

    @property
    def version(self):
        # Mirrors braintrust 0.17's lazy resolution: the fetch (and any
        # bad-pin failure) happens on first .version access, not at load.
        if self._version_error is not None:
            raise self._version_error
        return self._version

    def build(self, **variables):
        if isinstance(self._rendered, Exception):
            raise self._rendered
        return self._rendered


class FakeBraintrustModule:
    def __init__(self, prompt_obj):
        self.prompt_obj = prompt_obj
        self.load_prompt_calls = []

    def load_prompt(self, project, slug, version=None):
        self.load_prompt_calls.append({"project": project, "slug": slug, "version": version})
        return self.prompt_obj


def enable_with(prompt_obj):
    client = BraintrustClient.get_instance()
    client._enabled = True
    client._project = "stitch-golden-data"
    fake = FakeBraintrustModule(prompt_obj)
    client._braintrust_module = fake
    return client, fake


def test_build_flattens_all_messages():
    enable_with(FakePromptObj({"messages": [{"content": "system half"}, {"content": "user half"}]}))
    cache_prompt("p")
    assert build_cached_prompt("p", {}, fallback_prompt="FALLBACK") == "system half\nuser half"


def test_emptied_prompt_is_falsy_and_falls_back_at_warning():
    enable_with(FakePromptObj({"messages": []}))
    cache_prompt("p")
    with patch("shared.braintrust.logger.warning") as warn:
        out = build_cached_prompt("p", {}, fallback_prompt="FALLBACK")
    assert out == "FALLBACK"
    assert warn.called


def test_build_exception_falls_back_at_warning():
    enable_with(FakePromptObj(RuntimeError("registry schema changed")))
    cache_prompt("p")
    with patch("shared.braintrust.logger.warning") as warn:
        out = build_cached_prompt("p", {}, fallback_prompt="FALLBACK")
    assert out == "FALLBACK"
    assert warn.called


def test_version_pin_reaches_load_prompt():
    _, fake = enable_with(FakePromptObj("plain text prompt"))
    cache_prompt("p", version="xact-123")
    assert fake.load_prompt_calls == [{"project": "stitch-golden-data", "slug": "p", "version": "xact-123"}]


def test_provenance_recorded_with_resolved_version():
    enable_with(FakePromptObj("plain text prompt", version="xact-123"))
    cache_prompt("p", version="xact-123")
    assert get_prompt_provenance("p") == {
        "slug": "p",
        "pinned_version": "xact-123",
        "resolved_version": "xact-123",
        "loaded": True,
    }


def test_bad_pin_fails_at_cache_time_not_first_use():
    """braintrust resolves lazily; the cache must force materialization so a
    bad pin is a failed load at construction, never a mid-run surprise."""
    enable_with(FakePromptObj("text", version_error=ValueError("Prompt not found")))
    assert cache_prompt("p", version="bogus") is None
    assert get_prompt_provenance("p") == {
        "slug": "p",
        "pinned_version": "bogus",
        "resolved_version": None,
        "loaded": False,
    }


def test_conflicting_pin_is_refused():
    """The cache holds one object per slug; a second caller pinning a
    different version must fail loudly, never silently receive the first
    caller's version."""
    enable_with(FakePromptObj("plain text prompt"))
    cache_prompt("p", version="xact-123")
    with pytest.raises(ValueError, match="conflicting"):
        cache_prompt("p", version="deadbeef")


def test_provenance_records_failed_load():
    client, fake = enable_with(FakePromptObj("x"))
    fake.prompt_obj = None
    cache_prompt("missing", version="xact-123")
    assert get_prompt_provenance("missing") == {
        "slug": "missing",
        "pinned_version": "xact-123",
        "resolved_version": None,
        "loaded": False,
    }
