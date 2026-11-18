"""The dbt Cloud provider hook, wrapped for the reader.

Why a wrapper at all: `DbtCloudHook` is built for an operator that triggers and
waits on a run. The reader only reads, and it needs three things the hook does not
give directly: a refusal to talk to any host but the cell host, a `run_results.json`
call that cannot be made without a step number, and a typed error for a missing
artifact so a collector can degrade instead of crashing.

What the provider's `get_job_runs` does (read from the provider's own source,
`airflow/providers/dbt/cloud/hooks/dbt.py` in apache-airflow-providers-dbt-cloud
4.9.3): it is a single request that does NOT paginate. It returns one page, whose
size comes from the `limit` in the payload (dbt Cloud's own default when omitted),
and the caller has to page itself by offsetting. `list_job_runs` on the same hook
does paginate, by reading `extra.pagination.total_count`, but it takes no
`finished_after` filter. So the wrapper pages `get_job_runs` itself, honouring the
`limit` it sends and stopping on a short page.

The step parameter is the fragile part: `get_job_run_artifact` without `step`
returns the LAST step's artifact, which for a multi-step job is often
`docs generate` and looks green. `fetch_run_results` therefore requires a step.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from include.reader.models import Run, Step

CELL_HOST = "yx558.us1.dbt.com"
# One page of runs. dbt Cloud caps the page at 100; asking for 100 keeps a
# normal reader window to one request per job.
PAGE_LIMIT = 100
# A hard stop on paging, so a server that keeps returning a full page cannot
# spin forever. 20 pages is 2,000 runs, far beyond a windowed read.
MAX_PAGES = 20


class ArtifactUnavailable(Exception):
    """The step's artifact could not be fetched. The collector degrades rather
    than failing: a status-only record still tells a ticket what happened."""


class CellHostMismatch(Exception):
    """The connection points at a host the Admin API does not serve."""


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _in_trigger_cause(run: dict[str, Any], stem: str) -> bool:
    """Whether the run's trigger cause mentions `stem`.

    Substring, not equality: dbt Cloud's cause strings vary by how a run was
    started ("Scheduled" is the timer, and the same helper in gold_match_utils
    matches on the stem for this reason). Equality would silently classify the
    nightly as manual and it could never count as recovery.
    """
    trigger = run.get("trigger") or {}
    return stem in str(trigger.get("cause") or "").lower()


class DbtCloudClient:
    """Read-only access to one dbt Cloud account's runs and step artifacts."""

    def __init__(self, hook: DbtCloudHook):
        self._hook = hook

    @classmethod
    def from_conn_id(cls, conn_id: str, timeout_seconds: int = 30) -> "DbtCloudClient":
        """Build from a connection id.

        The host is checked here, not in the caller, because a wrong host fails as
        a confusing 404 against the generic `cloud.getdbt.com` endpoint rather than
        as "wrong host": the Admin API only answers on the account's cell host.
        """
        hook = DbtCloudHook(conn_id, timeout_seconds=timeout_seconds)
        client = cls(hook)
        client.check_host()
        return client

    def check_host(self) -> None:
        host = self._hook.get_connection(self._hook.dbt_cloud_conn_id).host or ""
        if host.strip().rstrip("/") != CELL_HOST:
            raise CellHostMismatch(
                f"the dbt Cloud connection must point at the cell host {CELL_HOST!r}; "
                "the Admin API does not work on the generic host"
            )

    def _page(self, job_id: int, offset: int) -> list[dict[str, Any]]:
        payload = {
            "job_definition_id": job_id,
            "order_by": "-id",
            "limit": PAGE_LIMIT,
            "offset": offset,
            # Without this the run carries no trigger and a scheduled run cannot
            # be told from a manual one, which is what real_success turns on.
            "include_related": ["trigger"],
        }
        return list(self._hook.get_job_runs(payload=payload).json()["data"])

    def _iter_raw_runs(self, job_id: int) -> Iterator[dict[str, Any]]:
        """Newest first by id, one request per page."""
        for page in range(MAX_PAGES):
            runs = self._page(job_id, page * PAGE_LIMIT)
            yield from runs
            if len(runs) < PAGE_LIMIT:
                return

    def list_runs(self, job_id: int, finished_after: datetime | None = None) -> list[Run]:
        """Every run of the job that finished after `finished_after`.

        Runs are listed newest first and filtered on finish time, not creation
        time, so a long run that finishes after a later-created one is still
        seen. A run still running has no finish time and is dropped here; the
        collector asks for those separately because a started run still counts as
        started for the due time.
        """
        cutoff = finished_after
        collected: list[Run] = []
        for raw in self._iter_raw_runs(job_id):
            run = Run(
                run_id=int(raw["id"]),
                job_id=int(raw.get("job_definition_id") or job_id),
                status=int(raw["status"]),
                created_at=_parse_ts(raw.get("created_at")),
                started_at=_parse_ts(raw.get("started_at")),
                finished_at=_parse_ts(raw.get("finished_at")),
                scheduled=_in_trigger_cause(raw, "schedul"),
                href=str(raw.get("href") or ""),
            )
            if run.finished_at is None:
                continue
            if cutoff is not None and run.finished_at <= cutoff:
                continue
            collected.append(run)
        return collected

    def first_unfinished(self, job_id: int) -> Run | None:
        """The newest run with no finish time, if one exists."""
        for raw in self._iter_raw_runs(job_id):
            if raw.get("finished_at"):
                continue
            return Run(
                run_id=int(raw["id"]),
                job_id=int(raw.get("job_definition_id") or job_id),
                status=int(raw["status"]),
                created_at=_parse_ts(raw.get("created_at")),
                started_at=_parse_ts(raw.get("started_at")),
                finished_at=None,
                scheduled=_in_trigger_cause(raw, "schedul"),
                href=str(raw.get("href") or ""),
            )
        return None

    def list_steps(self, run_id: int) -> list[Step]:
        """The run's steps, which carry the index the artifact call needs."""
        data = self._hook.get_job_run(run_id, include_related=["run_steps"]).json()["data"]
        return [
            Step(
                index=int(step["index"]),
                name=str(step.get("name") or ""),
                status=int(step.get("status") or 0),
            )
            for step in (data.get("run_steps") or [])
        ]

    def fetch_run_results(self, run_id: int, step: int) -> dict[str, Any]:
        """`run_results.json` for one step. A step is required: see the module docstring."""
        if not isinstance(step, int) or step < 1:
            raise ValueError("fetch_run_results needs a step index (the first step is 1)")
        try:
            response = self._hook.get_job_run_artifact(run_id, path="run_results.json", step=step)
            return dict(response.json()["data"])
        except Exception as exc:
            raise ArtifactUnavailable(
                f"run_results.json unavailable for step {step}: {exc.__class__.__name__}"
            ) from exc
