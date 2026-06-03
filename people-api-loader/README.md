# gp-data-loader

Generic Databricks -> S3 -> RDS loading harness for GoodParty.org pipelines.

The first concrete consumer is `loader.people_api`, which refreshes the
voter-DB Aurora cluster from the Databricks gold layer using a per-run,
manifest-driven pipeline. Additional consumers (e.g. donor data) can be
added as sibling packages under `src/loader/`.

This repository starts as the scaffolding only (DATA-1852); the per-step
command bodies live as stubs and will be implemented under their own
per-step tickets in the [DATA-1640](https://app.clickup.com/t/86ag66jjr)
epic. The `copy` body lands first under DATA-1851.

## Why this exists

The legacy `write__people_api_db` dbt job took >24h and timed out, blocking
voter-file refreshes. This loader replaces it with a per-step CLI that
unloads to S3, provisions a fresh Aurora cluster, parallel-COPYs into it,
builds indexes after load, then resizes to serverless and swaps in.

See `docs/PLAN_LOADER.md` for the full TDD (DATA-1735) — sizing, parameter
groups, validation, rollback.

## Train-deployment model

Every refresh is a new Aurora cluster, not a mutation of the existing one.
Each step:

1. Reads its manifest from `s3://{bucket}/voter_export_{date}/_manifest/{step}.json`.
2. If `status == "complete"`, logs and returns. (Re-runs are no-ops.)
3. Otherwise does its work and writes a fresh manifest at the end.

That means partial runs are safe to resume, and the orchestrator (Airflow)
gets idempotency for free.

Pipeline order: `inspect-prod` → `unload` → `provision` → `create-schema` →
`copy` → `build-indexes` → `resize` → `validate`. `status` reports
manifest state, `teardown` removes loader-created resources.

## Repo layout

```
src/loader/
  core/          # Consumer-agnostic harness (CLI helpers, config base,
                 # AWS/DB/log wrappers, manifest IO + base model).
  people_api/    # The people-API pipeline (steps, schema, SQL, concrete
                 # config and manifest models). The first concrete consumer.
  cli.py         # Top-level entry; delegates to people_api.cli.main.
```

Adding a second consumer means a new sibling package under `src/loader/`
that imports from `loader.core`. The harness is built to be reusable.

## Getting started

This project standardizes on the [Astral](https://astral.sh) toolchain:
[uv](https://docs.astral.sh/uv/) for env + dependency management,
[ruff](https://docs.astral.sh/ruff/) for lint + format, and
[ty](https://docs.astral.sh/ty/) for type-checking.

Install uv first: see
[docs.astral.sh/uv/getting-started/installation](https://docs.astral.sh/uv/getting-started/installation/).
uv reads `.python-version` and downloads Python 3.12 on first sync — no
separate `pyenv` install is needed.

```bash
# Pull the ai-rules submodule (one-time after fresh clone)
git submodule update --init --recursive

# Sync deps (creates .venv and downloads Python 3.12 if missing)
uv sync --extra dev

# CLI sanity check
uv run loader --help

# Per-run status (no AWS calls beyond caller-identity)
uv run loader status --date 20260101
```

Copy `.env.example` to `.env` and fill in your AWS profile before running
any step that touches AWS.

## Development

- Lint: `uv run ruff check src tests`
- Format: `uv run ruff format src tests`
- Type-check: `uv run ty check`
- Test: `uv run pytest`
- Pre-commit: `uv run pre-commit install` once, then commits run hooks automatically.

CI runs all of the above on push / PR. See `.github/workflows/ci.yml`.

## Reference

- `docs/PLAN_LOADER.md` — TDD (sizing, params, validation, rollback)
- `docs/validate.md` — validation report format
- ClickUp epic [DATA-1640](https://app.clickup.com/t/86ag66jjr) — People API Data Loading Revamp
- POC: [thegoodparty/poc-voterfile-loader](https://github.com/thegoodparty/poc-voterfile-loader)

## License

Released under the [Good Party Open Source License v1.0](LICENSE.md) — see `LICENSE.md` for the full terms.
