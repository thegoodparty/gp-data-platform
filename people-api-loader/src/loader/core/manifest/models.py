"""Base manifest schema.

Every step in a loader pipeline writes a manifest matching `ManifestBase`
(or a subclass) to S3 at `{export_prefix}/_manifest/{step}.json`. Step
re-entry reads the manifest first and no-ops when `status == "complete"`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

Status = Literal["in_progress", "complete", "failed"]


class ManifestBase(BaseModel):
    schema_version: Literal[1] = 1
    run_date: str
    step: str
    status: Status
    started_at: datetime
    finished_at: datetime | None = None
