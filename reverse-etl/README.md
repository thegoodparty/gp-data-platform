# retl

A small CLI that diffs a Databricks desired-state model against a destination (HubSpot contacts, or
a CSV for a local preview), keyed on a stable person id, and logs every payload it delivers so a
later run only ever sends what changed. `.env.example` documents the full environment surface; this
file is the run and deploy story.

## Local

```bash
uv sync
cp .env.example .env  # fill in the Databricks + flow values, then export them
uv run retl --source=hubspot --destination=csv
```

`--destination=csv` never writes to the send log, so it is safe to run repeatedly while iterating.
`--destination=hubspot_contacts` requires `RETL_HUBSPOT_TOKEN` and does write the log — point
`RETL_FLOW_<FLOW>_LOG_TABLE` at a scratch table unless you mean to send for real.

A flow's log table must exist before its first non-init run:

```bash
uv run retl --source=hubspot --init-log
```

This is a one-time (or post-reset) setup ceremony, run by a human. It is never part of a scheduled
run.

## Container image

`Dockerfile` builds a two-stage image: `uv sync` installs the locked dependencies and this package
itself (`--no-editable`, so the venv is self-contained), and the runtime stage copies only the venv.
The entrypoint is the `retl` console script; the default `CMD` is `--help`.

```bash
docker build -t retl-local .
docker run --rm --env-file .env retl-local --source=hubspot --destination=csv
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
