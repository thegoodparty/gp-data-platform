"""The small types shared by the reader's client, collector, records and state.

One module because these cross every boundary: the client returns `Run`/`Step`,
`records` builds a `Streak` from runs, `state` persists a `Streak`. Keeping them
here is what stops the client and `records` importing each other.
"""

from dataclasses import dataclass
from datetime import datetime

# dbt Cloud's terminal run statuses, from the provider's DbtCloudJobRunStatus.
RUN_SUCCESS = 10
RUN_ERROR = 20
RUN_CANCELLED = 30
RUN_TERMINAL = (RUN_SUCCESS, RUN_ERROR, RUN_CANCELLED)


@dataclass(frozen=True)
class Run:
    """One dbt Cloud run, reduced to what the reader uses."""

    run_id: int
    job_id: int
    status: int
    created_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    scheduled: bool
    href: str = ""

    @property
    def finished(self) -> bool:
        return self.finished_at is not None

    @property
    def outcome(self) -> str:
        """success, error, cancelled or running. Only the four the spec names."""
        if self.status == RUN_SUCCESS:
            return "success"
        if self.status == RUN_ERROR:
            return "error"
        if self.status == RUN_CANCELLED:
            return "cancelled"
        return "running"


@dataclass(frozen=True)
class Step:
    """One step of a run. `index` is what the artifact call needs."""

    index: int
    name: str
    status: int


@dataclass(frozen=True)
class Streak:
    """Per pipeline, the two facts that need to survive a reader run.

    `failed_since_last_success` is why this exists: a failure followed by a
    cancelled run has to stay failed, and nothing in the tool's own state says so.
    """

    failed_since_last_success: bool
    last_real_success_at: datetime | None
