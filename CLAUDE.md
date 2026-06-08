# CLAUDE.md

Short, non-obvious context for `gp-data-platform`. The repo overview is in `README.md`; dbt-specific guidance is in `dbt/project/CLAUDE.md`.

## Multi-venv reality

There is no single root venv. Each subproject manages its own deps. `cd` into the right one before you install or run anything.

| Subproject | Tool | Python | Notes |
|---|---|---|---|
| `people-api-loader/` | uv | 3.12 | Astral toolchain (ruff + ty). `uv sync`, `uv run ...`. |
| `dbt/` | poetry | 3.12 | `dbt` itself is the system-installed dbt Cloud CLI; do not invoke it via poetry. |
| `airflow/` | poetry | 3.12 | Local DAG dev outside Astronomer. To run Airflow itself: `cd airflow/astro && astro dev start`. |
| `analytics/` | pip + root `requirements_test.txt` | 3.12 | No own deps file; its tests rely on the root test deps. |
| `matcha/` | uv | 3.14 | Splink entity-resolution pipeline. `cd matcha && uv sync`. Builds a container via `.github/workflows/matcha-container.yml`. |
| `apps/genie-tools/` | pip + setuptools (PEP 621) | 3.12+ | `pip install -e .`, not poetry. |
| `apps/genie-slack-bot/` | pip + `requirements.txt` | unpinned | Not poetry. |

Each subproject has its own CI workflow at `.github/workflows/<name>.yml`, path-filtered to its directory and running on its own Python (3.12 for most; `matcha` on 3.14). There is no single root `pytest` job; tests are colocated under each directory (e.g. `airflow/astro/tests`, `dbt/tests`, `analytics/tests`).

## ai-rules submodule

`ai-rules/` is a git submodule (`thegoodparty/ai-rules`). After a fresh clone:

```bash
git submodule update --init --recursive
```

Don't edit files under `ai-rules/` directly. Changes belong in the submodule's upstream repo.

When reviewing changed code in this repo (e.g. during `/simplify`, `/review`, or any code-review pass), consult the rule files under `ai-rules/` in addition to this file and the per-subproject `CLAUDE.md` files.

## pre-commit

Two layers, both driven by `pre-commit`:

- **Repo-wide lint/format** (ruff, ruff-format, sqlfmt, and the generic hooks) run on the default `pre-commit` stage and in CI (`pre-commit run --all-files` on every PR). A failing hook blocks the merge.
- **Per-directory tests** run on the `pre-push` stage only. Each directory has a `pytest-<dir>` hook gated by `files:`, so a push runs only the suites for the directories it touched, in that directory's own environment. These are local only: the `pre-push` stage keeps them out of the CI `pre-commit run --all-files` job, and CI test coverage is each directory's own workflow.

Install both hook types once (the config sets `default_install_hook_types`):

```bash
# from the repo root
pre-commit install
```

If `pre-commit` is not on your PATH, install it once with `pipx install pre-commit` (or `brew install pre-commit`).

For the per-directory test hooks to pass on push, set up the environment of each directory you touch: `poetry install` in `dbt/` and `airflow/`, `uv sync` in `people-api-loader/`, `pip install -e .` in `apps/genie-tools/`, `pip install -r requirements.txt` in `apps/genie-slack-bot/`, and the root `requirements_test.txt` for `analytics/`. Each hook `cd`s into its directory and runs the suite via that env (`poetry run` / `uv run` / `pytest`), so you do not need to wrap `git` in any venv.

`poetry install` in `dbt/` builds `psycopg2` from source and needs `pg_config` on PATH. On macOS:

```bash
brew install libpq
echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc
```

## Never

- Don't add a root-level command that assumes one venv. State which subproject to `cd` into.
- Don't invoke `dbt` via `poetry`. dbt Cloud CLI is system-installed.
- Don't disable pre-commit hooks to make a commit go through. CI runs `pre-commit run --all-files` and will catch a skipped lint/format hook.
- Don't commit secrets. `.env.example` is the only env file in git.
