# hsm

Merges duplicate HubSpot contacts from a Databricks relation and appends one row per
attempt to an append-only Delta log. `.env.example` documents the full environment
surface; this file is the run and deploy story.

The job merges every row of its input relation up to a cap and applies no filter of its
own: eligibility, survivor ranking and every guardrail live in the `hubspot_contact_merge_queue`
view. Reports of *what* happened come from the log, which dbt reads.

## Local

```bash
uv sync
cp .env.example .env  # fill in the Databricks and merge values, then export them
uv run hsm run --dry-run
```

`--dry-run` confirms the portal, reads the pairs and prints their classification, then
merges nothing and writes no log row, so it is safe to repeat while iterating.

A merge cannot be undone. Point `HUBSPOT_TOKEN` and `HUBSPOT_EXPECTED_PORTAL_ID` at the
sandbox portal (51780263) and `MERGE_PAIR_RELATION` at a pair table you built until you
mean to merge in production.

The log table must exist before the first real run:

```bash
uv run hsm init-log
```

This is a one-time (or post-reset) setup ceremony, run by a human, and never part of a
scheduled run.

## CI

`.github/workflows/hubspot-merge.yml` runs pytest and mypy through the shared
`uv-subproject.yml` body on every change here. Repo-wide ruff and ruff-format run from
`.github/workflows/pre-commit.yml`, and the `pytest-hubspot-merge` hook runs the same
suite on push.
