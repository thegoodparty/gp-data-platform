# Getting Started

First-time setup for `gp-data-platform`. The high-level repo description lives in `README.md` and the dbt-specific runbook in `dbt/project/CLAUDE.md`. This doc covers what you need to be productive across the whole repo.

## Prerequisites

- **`pyenv`** for managing Python versions ([install guide](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation)).
- **Python 3.12** for subproject local dev (`pyenv install 3.12`). CI uses Python 3.11; if you want to reproduce CI exactly, install 3.11 too.
- **`pipx`** for installing tool-level CLIs ([install guide](https://pipx.pypa.io/stable/installation/)).
- **`poetry` ≥ 2.0** via `pipx install poetry` (per-subproject dep management). The `eval "$(poetry env activate)"` invocation used below is poetry-2.0-and-up syntax — older poetry installs will fail on it.
- **Docker** if you plan to run Airflow locally via the Astro CLI.
- **`astro` CLI** (`brew install astro` on macOS) if you'll be working on Airflow DAGs.
- **dbt Cloud CLI** installed at the **system level** (not via poetry) if you'll be working on dbt models. See [dbt Cloud docs](https://docs.getdbt.com/docs/cloud/cloud-cli-installation).
- **Databricks** access (creds + warehouse URL) if you'll run anything that hits the warehouse.

## Clone

This repo uses `ai-rules` as a git submodule. Clone with `--recursive`:

```bash
git clone --recursive git@github.com:thegoodparty/gp-data-platform.git
cd gp-data-platform
```

If you already cloned without `--recursive`:

```bash
make submodule-init    # or: git submodule update --init --recursive
```

There is **no `package.json` postinstall** to do this for you — run it explicitly. The `Makefile` target is added in this onboarding for convenience.

## Pick your subproject

There's no single "install everything" step — local dev is per-subproject. Pick the one you're working on and follow that section.

### Working on dbt models (most common)

```bash
cd dbt
poetry install
eval "$(poetry env activate)"          # shell hook to use the dbt-goodparty venv
pre-commit install                     # one-time, in the cloned repo

# Then for actual dbt work:
cd project
dbt build --select my_model           # see dbt/project/CLAUDE.md for the workflow
```

dbt Cloud CLI is system-installed; `dbt` is **not** invoked via `poetry`. The poetry env is needed so that the pre-commit pytest hook can find `pyspark` and `airflow`. If you skip the venv, your commit hooks fail.

### Working on Airflow DAGs

For DAG code outside the Astro container:

```bash
cd airflow
poetry install
eval "$(poetry env activate)"
```

For running Airflow locally (with the same DAGs Astronomer runs in prod):

```bash
cd airflow/astro
astro dev start                        # Docker-based local Airflow on http://localhost:8080
```

See `airflow/astro/README.md` for the full Astro CLI flow (deploy, env vars, connections).

### Working on `apps/genie-tools` (CLI)

```bash
cd apps/genie-tools
python -m venv .venv && source .venv/bin/activate
pip install -e .
genie-tools --help
```

Note `apps/genie-tools` uses PEP 621 `[project]` metadata with `setuptools` as the build backend — there is no `[tool.poetry]` block and no `poetry.lock` committed. Don't reach for poetry here.

### Working on `apps/genie-slack-bot`

```bash
cd apps/genie-slack-bot
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Note this is `pip` + `requirements.txt`, not poetry — different from the other apps.

### Reproducing CI locally (root pytest)

```bash
# From the repo root, in a fresh venv:
python -m venv .venv && source .venv/bin/activate
pip install -r requirements_test.txt
pytest tests
```

This is what `.github/workflows/python_tests.yml` runs. Same Python (3.11), same deps. Matches CI.

## Pre-commit hooks

```bash
pip install pre-commit                 # if not already
pre-commit install                     # one-time
pre-commit run --all-files             # run all hooks against all files
```

The hooks are: `black`, `isort`, `flake8`, `mypy`, `sqlfmt` (for `.sql` files), plus a local `pytest` hook. The full set is in `.pre-commit-config.yaml`. CI runs `pre-commit run --all-files` on every PR — if it's green locally, it'll be green in CI.

## Environment variables

`.env.example` is the source of truth. Today it has:

| Var | Notes |
|-----|-------|
| `DBT_CLOUD_PROJECT_ID` | dbt Cloud project ID for jobs/triggers |

Subprojects may need their own envs (Databricks creds, Slack tokens, etc.) — those live in subproject-specific files; check the relevant README.

## Running tests

| What | Command |
|------|---------|
| Cross-subproject pytest (CI parity) | `pytest tests` (from repo root, after `pip install -r requirements_test.txt`) |
| dbt model tests | `cd dbt/project && dbt test --select <model>` |
| Per-subproject pytest | `cd <subproject> && poetry run pytest` |
| All pre-commit hooks | `pre-commit run --all-files` |

## Common gotchas

- **`ai-rules/` is empty after clone** → run `make submodule-init` or `git submodule update --init --recursive`. There's no postinstall hook (no `package.json`).
- **Pre-commit pytest fails with "no module named pyspark / airflow"** → you ran the commit from outside the `dbt/` poetry env. Per `dbt/project/CLAUDE.md`, do `cd dbt && poetry run git commit ...`.
- **`poetry install` complains about Python version** → make sure `pyenv` has `3.12.x` installed and active in this directory (`pyenv local 3.12.x`).
- **CI fails on a 3.12-only feature** → CI uses Python 3.11. `match` (PEP 634) is fine — it's 3.10+. Things like `type` statements / PEP 695 generics, `TypeVar`-with-defaults syntax, and 3.12-only typing additions will break CI; don't reach for them without bumping the workflow.
- **`dbt run` complains about deferred state** → you don't need `--defer` or a state file path with dbt Cloud CLI. Per `dbt/project/CLAUDE.md`.
- **`dbt build --select +my_model` is slow** → don't use the `+` upstream selector during development — dbt Cloud already defers to prod artifacts. List only the modified models. Per `dbt/project/CLAUDE.md`.
- **Slack bot can't connect locally** → no `pyproject.toml` here; remember it's `pip install -r requirements.txt` inside `apps/genie-slack-bot/`.
- **`SECRETS` in `.env`** → never commit. `.env.example` is the only env file in git.

## Where to go next

- `README.md` — repo overview and component map.
- `CLAUDE.md` — agent + style guide for the whole repo.
- `dbt/project/CLAUDE.md` — dbt-specific workflow, conventions, terminology, ID mappings.
- `airflow/README.md` and `airflow/astro/README.md` — Airflow / Astronomer setup.
- `docs/architecture.md` — subproject map, data flow, CI shape.
- `ai-rules/` — org-wide review rules and skills (submodule).
