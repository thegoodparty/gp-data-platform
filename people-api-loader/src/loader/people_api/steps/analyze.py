"""Step 7 — ANALYZE the whole database as the final step.

`build_indexes` runs `VACUUM (ANALYZE)` on each partitioned parent. The VACUUM recurses to the
leaf partitions and sets their visibility maps, but the parent-level ANALYZE only refreshes the
parent's inheritance statistics — it does NOT populate each leaf partition's own per-column
stats (they stay `last_analyze IS NULL`), so the planner has no selectivity stats for the
partitions it actually scans. A bare, database-wide `ANALYZE` processes every table AND every
leaf partition the connecting role owns in one statement, giving the planner fresh per-partition
stats. Runs last so it reflects the fully loaded + resized serving cluster.

Idempotent: ANALYZE is safe to re-run, and a completed manifest short-circuits.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new
from loader.people_api.manifests import (
    AnalyzeManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

# Count the public base tables + leaf partitions that now carry fresh manual-ANALYZE stats, as an
# observability signal in the manifest (a partitioned parent's ANALYZE previously left these NULL).
_ANALYZED_COUNT_SQL = """
SELECT count(*)
FROM pg_stat_all_tables s
JOIN pg_class c ON c.oid = s.relid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r' AND s.last_analyze IS NOT NULL
"""


def run(cfg: LoaderConfig, run_date: str) -> AnalyzeManifest:
    bind(run_date=run_date, step="analyze")
    existing = read_manifest(cfg, run_date, "analyze", AnalyzeManifest)
    if existing and existing.status == "complete":
        log.info(
            "analyze.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "analyze")
        )
        return existing

    started = datetime.now(UTC)
    log.info("analyze.start", cluster=cfg.new_cluster_id(run_date))

    # A bare ANALYZE processes every table + leaf partition, which the per-parent ANALYZE in
    # build_indexes does not reach. connect_new is autocommit by default.
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        cur.execute("ANALYZE")
        cur.execute(_ANALYZED_COUNT_SQL)
        row = cur.fetchone()
        tables_analyzed = int(row[0]) if row else 0
    log.info("analyze.done", tables_analyzed=tables_analyzed)

    manifest = AnalyzeManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        tables_analyzed=tables_analyzed,
    )
    uri = write_manifest(cfg, manifest)
    log.info("analyze.complete", uri=uri, tables_analyzed=tables_analyzed)
    return manifest
