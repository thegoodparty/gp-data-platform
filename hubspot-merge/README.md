# hsm

Merges duplicate HubSpot contacts from a Databricks relation and appends one row per
attempt to an append-only Delta log. `.env.example` documents the full environment surface.

The job merges every row of its input relation up to a cap and applies no filter of its own:
eligibility, survivor ranking and every guardrail live in the `hubspot_contact_merge_queue`
view. What happened comes from the log, which dbt reads.

## Commands

```bash
hsm init-log              # create MERGE_LOG_TABLE if it does not exist
hsm run --dry-run         # read and classify the pairs; merge and log nothing
hsm run                   # merge, and log one row per pair
hsm verify --run-id ID    # check a run's merged pairs against HubSpot
```

Every command except `init-log` first confirms the key opens `HUBSPOT_EXPECTED_PORTAL_ID`.
`run` exits 1 if any merge returned an id other than the primary's.

## Modules

| module | job |
|---|---|
| `config.py` | reads and checks the environment |
| `client.py` | the three HubSpot calls, paced and retried |
| `classify.py` | turns a read or a merge into a status |
| `pairs.py` | reads pairs from the queue, or from the log for `verify` |
| `execute.py` | per chunk: read, classify, merge, log |
| `log.py` | the log table's DDL and its INSERT |
| `databricks.py` | the warehouse connection |
| `cli.py` | wires the parts together |

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
