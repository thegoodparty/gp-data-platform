# hsm

Merges duplicate HubSpot contacts from a Databricks relation and appends one row per
attempt to an append-only Delta log. `.env.example` documents the full environment surface.

The job merges every row of its input relation up to a cap and applies no filter of its own:
eligibility, survivor ranking and every guardrail live in the `hubspot_contact_merge_queue`
view. What happened comes from the log, which dbt reads.

The `hsm` console script is a stub that exits non-zero; the commands are a later step.

## Local

```bash
uv sync
cp .env.example .env  # fill in the Databricks and merge values, then export them
```

A merge cannot be undone. Point `HUBSPOT_TOKEN` and `HUBSPOT_EXPECTED_PORTAL_ID` at the
sandbox portal (51780263) and `MERGE_PAIR_RELATION` at a pair table you built until you mean
to merge in production.

## CI

`.github/workflows/hubspot-merge.yml` runs pytest and mypy through the shared
`uv-subproject.yml` body on every change here. Repo-wide ruff and ruff-format run from
`.github/workflows/pre-commit.yml`, and the `pytest-hubspot-merge` hook runs the same suite on
push.
