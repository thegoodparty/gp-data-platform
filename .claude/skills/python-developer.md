---
name: python-developer
description: Repo-local guidance for writing Python in gp-data-platform. Captures the multi-venv, multi-tool reality so agents don't reach for the wrong package manager or run the wrong test command. Use whenever a task involves writing or running Python code in this repo.
---

You are writing Python in `gp-data-platform`. Read this before you `cd` anywhere or run any command.

## Ground truth

This repo is **multi-project**, not a single Python package. There is no root `pyproject.toml`, no root venv, no `make install`. Each Python subproject manages its own deps:

| Subproject | Tool | Lockfile | Python | Notes |
|------------|------|----------|--------|-------|
| `airflow/` | `poetry` | `airflow/poetry.lock` | 3.12 | Local DAG dev outside Astro container |
| `airflow/astro/` | `pip` (Astronomer) | `requirements.txt` | (Astronomer-managed) | The actual deployed Airflow runtime |
| `dbt/` | `poetry` | `dbt/poetry.lock` | 3.12 | Hosts the pre-commit pytest hook |
| `apps/genie-tools/` | `pip install -e .` | none committed | 3.12+ | PEP 621 `[project]` metadata, setuptools build backend, no `[tool.poetry]` block. CLI: `genie-tools` |
| `apps/genie-slack-bot/` | `pip` | `requirements.txt` | (no version pinned) | No `pyproject.toml` |
| **root pytest (CI)** | `pip` | `requirements_test.txt` | 3.11 | What `.github/workflows/python_tests.yml` runs |

Pick the row that matches what you're touching. If you're not sure, **ask the user before installing anything**.

## Decision tree

- **Are you adding or fixing a dbt model?** → `cd dbt && poetry install && eval "$(poetry env activate)"`. Then `cd project` and follow `dbt/project/CLAUDE.md`. **Do not** invoke dbt via `poetry`; dbt Cloud CLI is system-installed.
- **Are you adding or fixing an Airflow DAG?** → For local DAG-code dev: `cd airflow && poetry install && eval "$(poetry env activate)"`. To run Airflow itself locally: `cd airflow/astro && astro dev start` (Docker).
- **Are you working on `apps/genie-tools`?** → `cd apps/genie-tools && python -m venv .venv && source .venv/bin/activate && pip install -e .`. (Not poetry — `pyproject.toml` is PEP 621 + setuptools, no `[tool.poetry]` block.)
- **Are you working on `apps/genie-slack-bot`?** → `cd apps/genie-slack-bot && python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`. **Not poetry** — different from the other apps.
- **Are you adding a cross-subproject test?** → Test goes under `tests/<subproject>/`. CI installs `requirements_test.txt` and runs `pytest tests` from the repo root.

## Style enforcement

`.pre-commit-config.yaml` is the source of truth for code style. It runs:

- **black** with `--line-length=88`
- **isort** with `--profile black --line-length=88`
- **flake8** with `--max-line-length=88` and an explicit ignore list (`D100,D103,D200,D205,D400,D401,E203,E501,F821,W503`)
- **mypy** with type stubs declared in the hook (`types-requests`, `types-psycopg2`, `types-paramiko`, `types-tabulate`)
- **sqlfmt** for `.sql` files (with `[jinjafmt]` for dbt Jinja)
- a local **`pytest`** hook that runs the test suite before each commit

Run them yourself with `pre-commit run --all-files`. CI runs the same; local-green = CI-green.

## Testing

- **Cross-subproject tests** (CI parity): from the repo root,
  ```bash
  pip install -r requirements_test.txt
  pytest tests
  ```
- **Per-subproject tests:** `cd <subproject> && poetry run pytest` (or `pytest` if you already activated the env).
- **dbt model tests:** `cd dbt/project && dbt test --select <model>`. **Not pytest** — these are dbt's own data tests.
- **Pre-commit pytest hook:** runs from wherever you `git commit` from. From `dbt/`, do `poetry run git commit ...` so the hook can find pyspark and airflow.

## Common pitfalls

- **Running `poetry install` at the repo root.** There's no root `pyproject.toml`. You have to `cd` first.
- **Adding deps to `requirements_test.txt` without a reason.** That file is for **CI test deps** — anything else belongs in a subproject's `pyproject.toml` (or `requirements.txt` in the case of `apps/genie-slack-bot/` or `airflow/astro/`).
- **Writing `match` statements (Python 3.10+ structural pattern matching) in code that CI runs on 3.11.** Fine. **But** subprojects pin 3.12; don't use 3.13-only features anywhere.
- **Disabling a pre-commit hook in a one-off commit.** If a hook is failing, fix the root cause. Bypassing locally just pushes the failure into the PR.
- **Editing the dbt project without reading `dbt/project/CLAUDE.md` first.** The dbt-specific runbook there is non-trivial — `+` selectors, lateral column refs, NULL in `NOT IN`, naming conventions. Skipping it produces broken or wasteful code.
- **Editing `ai-rules/` directly.** Submodule. Changes belong in the upstream `thegoodparty/ai-rules` repo.

## What "good" looks like

- You picked the right subproject + venv before you started.
- You ran `pre-commit run --all-files` and it's green before you commit.
- You added or extended tests under `tests/<subproject>/` (or dbt tests in `dbt/project/`).
- You didn't touch `ai-rules/` or `.claude/skills/l2-uniform-drift-remediator/` unless the task explicitly involves them.
- If you added a new package manager, lockfile, or top-level convention, you also updated `CLAUDE.md` and `docs/architecture.md` to reflect it.
