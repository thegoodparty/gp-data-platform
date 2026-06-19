# CLAUDE.md

Short, non-obvious context for `people-api-loader`. The repo overview is in `README.md`; the full TDD is in `docs/PLAN_LOADER.md`.

## ai-rules submodule

`ai-rules/` is a git submodule at the repository root (`thegoodparty/ai-rules`). After a fresh clone, run `git submodule update --init --recursive` from the repo root. Don't edit files under `ai-rules/` directly — changes belong in the submodule's upstream repo. When reviewing changed code in this directory, consult the rule files at `../../ai-rules/` in addition to this file.

## Astral-only toolchain

This repo standardizes on Astral tools and is intentionally not aligned with `gp-data-platform`'s older stack:

| Tool | Use | Notes |
|---|---|---|
| `uv` | env + deps | Installs Python 3.12 itself via `.python-version`. No pyenv. |
| `ruff` | lint + format | Line length 110. Don't introduce black/isort/flake8. |
| `ty` | type checker | Pre-1.0 — version is pinned exactly in `pyproject.toml`. Don't loosen the pin without testing. Don't introduce pyright/mypy. |

CI runs `uv run ruff check`, `uv run ruff format --check`, `uv run ty check`, and `uv run pytest` on every PR.

## Step bodies are stubs by design

Every file under `src/loader/people_api/steps/` raises `NotImplementedError` with a pointer to its ClickUp ticket (e.g. `DATA-1851` for `copy_s3`). Scaffolding shipped under DATA-1852; bodies land under their own per-step tickets in the [DATA-1640 epic](https://app.clickup.com/t/86ag66jjr). Don't fill in a stub unless that ticket is the task at hand.

Stub return-type rule: even when a stub only raises, declare the eventual real return type. The CLI is already wired up against those types, and `ty` will flag the mismatch. See `steps/validate.py` for the pattern.

## Never

- Don't read or set `VOTER_DB_MASTER_PASSWORD`. `LoaderConfig.from_env()` hard-fails if it's set. Connections come from SSM SecureString connection strings (`people-db-connection-string-{env}` for the Present cluster, `-{date}`-suffixed for each provisioned cluster), never an env-var password.
- Don't put people-API-specific knowledge in `src/loader/core/`. The harness must stay consumer-agnostic — a second consumer (e.g. donor data) should sit alongside `people_api/`, not require core changes.
- Don't omit `Environment=dev` tags on any AWS resource the loader creates. The loader's IAM permissions-boundary requires it; missing tags = denied API call.
- Don't mutate the existing serving cluster. The model is train-deployment: every refresh provisions a fresh Aurora cluster and swaps in.
- Don't write a manifest mid-step. Manifest existence at `s3://{bucket}/voter_export_{date}/_manifest/{step}.json` means that step is complete.
