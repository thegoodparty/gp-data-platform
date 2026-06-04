"""Step 0 — capture prod cluster shape for later diffs.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1907.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import InspectManifest


def run(cfg: LoaderConfig, run_date: str) -> InspectManifest:
    raise NotImplementedError(
        "loader.people_api.steps.inspect_prod is a stub; implement per ClickUp DATA-1907."
    )
