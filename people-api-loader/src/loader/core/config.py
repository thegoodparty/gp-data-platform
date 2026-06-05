"""Base loader configuration.

`BaseLoaderConfig` holds the consumer-agnostic fields every loader needs:
AWS session info, an S3 bucket for artifacts, and a `tags` dict applied to
loader-created AWS resources. Subclass per consumer to add domain-specific
fields and to implement `export_prefix(run_date)` — the path under which
that consumer's per-run artifacts and manifests live.

Each consumer defines its own `from_env()` classmethod on its subclass.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(slots=True, kw_only=True)
class BaseLoaderConfig:
    aws_region: str
    aws_profile: str | None
    account_id: str
    s3_bucket: str
    _tags: dict[str, str] = field(default_factory=dict)

    @property
    def tags(self) -> dict[str, str]:
        return dict(self._tags)

    def tags_as_aws(self) -> list[dict[str, str]]:
        return [{"Key": k, "Value": v} for k, v in self._tags.items()]

    def export_prefix(self, run_date: str) -> str:
        """S3 key prefix for this run's artifacts. Override per consumer."""
        raise NotImplementedError("Subclasses must define export_prefix(run_date).")

    def manifest_key(self, run_date: str, step: str) -> str:
        return f"{self.export_prefix(run_date)}/_manifest/{step}.json"

    def log_key(self, run_date: str, step: str) -> str:
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"{self.export_prefix(run_date)}/_logs/{step}-{ts}.jsonl"
