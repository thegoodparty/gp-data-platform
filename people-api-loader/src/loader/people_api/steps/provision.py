"""Step 2 — provision Aurora cluster, IAM role, VPCE, parameter groups.

Stub: scaffolding only. Per-step implementation tracked under ClickUp DATA-1909.
"""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import ProvisionManifest


def run(cfg: LoaderConfig, run_date: str) -> ProvisionManifest:
    raise NotImplementedError("loader.people_api.steps.provision is a stub; implement per ClickUp DATA-1909.")
