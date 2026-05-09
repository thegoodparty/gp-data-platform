# CLAUDE.md

Guidance for Claude Code and other AI agents working in `gp-data-platform`. Keep this file short — push detail into `docs/` and the per-subproject `CLAUDE.md` files.

## Project

Heterogeneous **multi-project** Python repo for GoodParty's data platform: Airflow DAGs (Astronomer-hosted) for orchestration, dbt models (dbt Cloud against Databricks) for transformation, plus standalone apps and ad-hoc scripts. **Each subproject manages its own Python deps** — `airflow/` and `dbt/` use `poetry`; `apps/genie-tools/` uses `pip install -e .` (PEP 621 + setuptools); `apps/genie-slack-bot/` uses `pip install -r requirements.txt`. Top-level CI uses `pip` + `requirements_test.txt` to run pytest across the repo. There is no single venv; there are several.

## Commands (most-used first)

```bash
# Top-level CI parity (what GitHub Actions runs)
pip install -r requirements_test.txt        # install root test deps
pytest tests                                 # run the cross-subproject tests
pre-commit run --all-files                   # black + isort + flake8 + mypy + sqlfmt + pytest

# Per-subproject local dev (run from inside the subdirectory)
cd dbt && poetry install && eval "$(poetry env activate)"
cd airflow && poetry install && eval "$(poetry env activate)"
cd apps/genie-tools && python -m venv .venv && source .venv/bin/activate && pip install -e .   # PEP 621 + setuptools, not poetry

# After hooks are installed:
pre-commit install                           # one-time, in the cloned repo
```

The repo's `Makefile` is intentionally minimal — its one task is `make submodule-init` for the `ai-rules` submodule init step. Per-subproject installs deliberately stay out of `Makefile` (each subproject has its own venv; see § Environment).

## Pointer table — when in doubt

| Doing | Read |
|-------|------|
| Adding or fixing an Airflow DAG | `airflow/astro/README.md` and `airflow/README.md` |
| Adding or fixing a dbt model | `dbt/project/CLAUDE.md` (already detailed) |
| Working on `apps/genie-tools` (Genie config CLI) | `apps/genie-tools/README.md` |
| Working on `apps/genie-slack-bot` (Slack bot) | `apps/genie-slack-bot/README.md` |
| Architecture overview (how the subprojects fit) | `docs/architecture.md` |
| First-time setup | `docs/getting-started.md` |
| AI rule-by-rule code review | `ai-rules/` (git submodule) |
| Why a thing is the way it is | `docs/adr/` (not yet seeded) |

## Code style

- **Python 3.12+** in subprojects (`airflow/` and `dbt/` pin `requires-python = ">=3.12,<3.13"`; `apps/genie-tools/` is `>=3.12` with no upper bound). **CI runs Python 3.11** for the root pytest pass — see § Environment.
- **Black**, line length **88** (`.pre-commit-config.yaml`). Don't fight it; `pre-commit run --all-files` is the source of truth.
- **isort**, `--profile black`, line length 88.
- **flake8** with `--max-line-length=88` and the ignore list in `.pre-commit-config.yaml`. Don't widen the ignore list without a reason.
- **mypy** runs in pre-commit. Type stubs come from the `additional_dependencies` block in `.pre-commit-config.yaml`.
- **sqlfmt** for `.sql` files (with `[jinjafmt]` for dbt's Jinja templating).
- Use type hints on public APIs. Don't blanket-`# type: ignore` — narrow the ignore with a reason if absolutely needed.

## Subproject map

```
gp-data-platform/
├── airflow/                   # Airflow / Astronomer
│   ├── astro/                 # Astronomer project (Dockerfile, dags/, requirements.txt)
│   ├── pyproject.toml         # poetry — name: astro-goodparty (Python 3.12, Airflow 3.2.0)
│   └── poetry.lock
├── apps/
│   ├── genie-tools/           # Databricks Genie config export CLI (setuptools-built, src/ layout)
│   │   └── pyproject.toml
│   └── genie-slack-bot/       # Slack bot wrapping Genie (requirements.txt — pip-only)
├── dbt/                       # dbt Cloud + Databricks
│   ├── project/               # dbt project root: models / macros / seeds / snapshots / tests
│   │   └── CLAUDE.md          # dbt-specific guidance (pre-existing, detailed)
│   ├── pyproject.toml         # poetry — name: dbt-goodparty
│   ├── poetry.toml            # virtualenvs.in-project = false
│   └── poetry.lock
├── genie/civics/              # Genie space configs
├── scripts/                   # shell glue (tgp_to_gp, local_to_election_db, dbt codegen)
├── tests/                     # cross-subproject pytest suites (airflow / apps / dbt)
├── requirements_test.txt      # what root CI installs to run the pytest suite
├── .pre-commit-config.yaml    # the actual style enforcement
└── .github/workflows/         # pre-commit.yml, python_tests.yml
```

`dbt/project/` is where most day-to-day data work happens — its `CLAUDE.md` has the dbt-specific runbook (model build/test workflow, naming conventions, the lateral-column-reference / NULL-in-NOT-IN gotchas).

## Testing

- Framework: **pytest** (root pytest + per-subproject pytest where each has a `pyproject.toml`).
- Test layout at the root: `tests/airflow/`, `tests/apps/`, `tests/dbt/` mirror the source tree. CI runs `pytest tests` from the repo root after `pip install -r requirements_test.txt`.
- The `pytest` hook in `.pre-commit-config.yaml` also runs locally on commit. Per `dbt/project/CLAUDE.md`, the dbt subproject requires `cd dbt && poetry run git commit ...` so the hook can find `pyspark` and `airflow`.
- **dbt model tests** are **dbt's own tests** (`dbt test`), not pytest — see `dbt/project/CLAUDE.md`.

## Never

- Never add commands to the root that assume a single venv. There isn't one. If your command needs deps, document **which subproject** to `cd` into first.
- Never invoke `dbt` via `poetry`. Per `dbt/project/CLAUDE.md`, dbt Cloud CLI is installed at the system level. `poetry run dbt ...` is wrong here.
- Never edit a file under `ai-rules/` directly — it's a submodule. Changes belong in the `thegoodparty/ai-rules` repo. Bump the pin afterward and stage `ai-rules` in the parent.
- Never overwrite `.claude/skills/l2-uniform-drift-remediator/`. That skill is a complete, working tool with scripts, tests, and reference runbooks.
- Never disable a pre-commit hook in a one-off commit to "make CI green." Fix the underlying issue. CI runs `pre-commit run --all-files`; bypassing it locally pushes the failure to the PR.
- Never check secrets into `.env`. Only `.env.example` is committed (`DBT_CLOUD_PROJECT_ID=`).

## Environment

- **Python 3.12** in subprojects (their `pyproject.toml`s pin it; `apps/genie-tools/` is `>=3.12` with no upper bound, the others cap at `<3.13`). **CI uses Python 3.11** (`.github/workflows/python_tests.yml` and `.github/workflows/pre-commit.yml`). This split is pre-existing — flag any new code that depends on a 3.12-only feature.
- **Package manager varies by subproject:** `airflow/` and `dbt/` use **poetry** (≥ 2.0, installed via `pipx`, with Python pinned via `pyenv`); `apps/genie-tools/` uses **pip + setuptools** (PEP 621); `apps/genie-slack-bot/` uses **pip + requirements.txt**. The poetry virtualenvs live in poetry's cache (`dbt/poetry.toml` sets `virtualenvs.in-project = false`).
- **No `uv` migration today.** The org-wide direction may be `uv`, but this repo isn't there yet — don't pre-write `uv` commands into new docs. Update the docs in lockstep with the actual migration.
- The `ai-rules/` submodule isn't auto-initialized (no `package.json` postinstall available). After cloning, run `make submodule-init` (see `Makefile`) or `git submodule update --init --recursive`.
- Required env: see `.env.example` (currently just `DBT_CLOUD_PROJECT_ID=`).
