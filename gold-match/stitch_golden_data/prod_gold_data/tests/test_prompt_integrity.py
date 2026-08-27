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
    def __init__(self, rendered):
        self._rendered = rendered

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
    cache_prompt("p", version="3a27a867")
    assert fake.load_prompt_calls == [{"project": "stitch-golden-data", "slug": "p", "version": "3a27a867"}]


def test_provenance_recorded():
    enable_with(FakePromptObj("plain text prompt"))
    cache_prompt("p", version="3a27a867")
    assert get_prompt_provenance("p") == {"slug": "p", "pinned_version": "3a27a867", "loaded": True}


def test_conflicting_pin_is_refused():
    """The cache holds one object per slug; a second caller pinning a
    different version must fail loudly, never silently receive the first
    caller's version."""
    enable_with(FakePromptObj("plain text prompt"))
    cache_prompt("p", version="3a27a867")
    with pytest.raises(ValueError, match="conflicting"):
        cache_prompt("p", version="deadbeef")


def test_provenance_records_failed_load():
    client, fake = enable_with(FakePromptObj("x"))
    fake.prompt_obj = None
    cache_prompt("missing", version="3a27a867")
    assert get_prompt_provenance("missing") == {
        "slug": "missing",
        "pinned_version": "3a27a867",
        "loaded": False,
    }
