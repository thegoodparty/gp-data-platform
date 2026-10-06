# retl

A small CLI that diffs a Databricks desired-state model against a destination (HubSpot contacts, or
a CSV file per run), keyed on a stable person id, and logs every payload it delivers so a
later run only ever sends what changed. `.env.example` documents the full environment surface; this
file is the run and deploy story.

## Local

```bash
uv sync
cp .env.example .env  # fill in the Databricks + flow values, then export them
uv run retl --source=hubspot --destination=csv --dry-run
```

`--dry-run` reads and diffs exactly as a real run does, guards included, prints the summary line,
and sends and logs nothing, so it is safe to run repeatedly while iterating. It needs no
destination config (no HubSpot token, no output dir).

Without it, every destination is a real delivery and writes the log. `--destination=csv` writes
each run's diff to a new `<flow>_<utc timestamp>.csv` in `RETL_CSV_OUTPUT_DIR`, so a rerun writes
nothing (no file at all) until a row is added or changed. `--destination=hubspot_contacts` requires
`RETL_HUBSPOT_TOKEN`. Point `RETL_FLOW_<FLOW>_LOG_TABLE` at a scratch table unless you mean to send
for real.

A flow's log table and its `<log_table>_orphans` table must exist before its first non-init run:

```bash
uv run retl --source=hubspot --init-log
```

This is a one-time (or post-reset) setup ceremony, run by a human. It is never part of a scheduled
run.

## Deleted rows

retl never deletes from a destination. A key that was sent before and has left the source model gets a
`missing` event in `<log_table>_orphans`, with its last-sent payload for lookup, and a `returned`
event if it comes back. The ones to clean up by hand:

```sql
select tracking_key, last_payload, detected_at from <log_table>_orphans
qualify row_number() over (partition by tracking_key order by detected_at desc) = 1
  and event = 'missing'
```

A returned key is resent once even if unchanged, since its contact may have been deleted by hand in the
meantime.

## Container image

`Dockerfile` builds a two-stage image: `uv sync` installs the locked dependencies and this package
itself (`--no-editable`, so the venv is self-contained), and the runtime stage copies only the venv.
The entrypoint is the `retl` console script; the default `CMD` is `--help`.

```bash
docker build -t retl-local .
docker run --rm --env-file .env retl-local --source=hubspot --destination=csv --dry-run
```

## CI and deploy

`.github/workflows/reverse-etl-container.yml` builds and publishes a multi-arch image to GHCR
(`ghcr.io/thegoodparty/gp-data-platform/reverse-etl`) on every merge that touches this directory,
tagged with the commit sha alongside `latest`. The package is **private** — that is a package
setting, not something the workflow can set, so a deployment needs an image pull secret.

The daily Airflow DAG (`reverse_etl_hubspot`) pins its pod to a specific sha via the
`reverse_etl_image_tag` Variable, with no mutable-tag default — deploying a merged change is an explicit bump of that
Variable to the new build's sha, which is the provenance gate: the DAG always runs exactly the build
that was evaluated, never whatever `latest` happens to point at.

This line was added by a gp-pi parity probe.
