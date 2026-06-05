# CLAUDE.md

Short, non-obvious context for `gp-data-platform`. The repo overview is in `README.md`; dbt-specific guidance is in `dbt/project/CLAUDE.md`.

## Multi-venv reality

There is no single root venv. Each subproject manages its own deps. `cd` into the right one before you install or run anything.

| Subproject | Tool | Python | Notes |
|---|---|---|---|
| `dbt/` | poetry | 3.12 | Hosts the pre-commit pytest hook. `dbt` itself is the system-installed dbt Cloud CLI — do not invoke it via poetry. |
| `airflow/` | poetry | 3.12 | Local DAG dev outside Astronomer. To run Airflow itself: `cd airflow/astro && astro dev start`. |
| `matcha/` | uv | 3.11 | Splink entity-resolution pipeline. `cd matcha && uv sync`. Builds a container via `.github/workflows/matcha-container.yml`. |
| `apps/genie-tools/` | pip + setuptools (PEP 621) | 3.12+ | `pip install -e .`, not poetry. |
| `apps/genie-slack-bot/` | pip + `requirements.txt` | unpinned | Not poetry. |
| Root CI pytest | pip + `requirements_test.txt` | **3.11** | `.github/workflows/python_tests.yml` runs this. |

CI runs on Python **3.11** while subprojects pin **3.12**. Don't write 3.12-only features into code paths exercised by root pytest.

## ai-rules submodule

`ai-rules/` is a git submodule (`thegoodparty/ai-rules`). After a fresh clone:

```bash
git submodule update --init --recursive
```

Don't edit files under `ai-rules/` directly — changes belong in the submodule's upstream repo.

When reviewing changed code in this repo (e.g. during `/simplify`, `/review`, or any code-review pass), consult the rule files under `ai-rules/` in addition to this file and the per-subproject `CLAUDE.md` files.

## pre-commit

CI runs `pre-commit run --all-files` on every PR, so a failing hook will block the merge. Install the git hook locally so the same checks run before you push:

```bash
# from the repo root
pre-commit install
```

If `pre-commit` is not on your PATH, install it once with `pipx install pre-commit` (or `brew install pre-commit`).

The local pytest hook needs the `dbt/` poetry venv on PATH. Run git from `dbt/` with `poetry run` so the hook can find `pytest`, `pyspark`, and `airflow`:

```bash
cd dbt && poetry run git commit ...
```

If an analytics venv is active (common when working under `analytics/`), the hook can still fail with `Executable pytest not found` because `VIRTUAL_ENV` shadows the poetry env. Unset it for the commit:

```bash
env -u VIRTUAL_ENV poetry run git commit ...   # from dbt/
```

`poetry install` in `dbt/` builds `psycopg2` from source and needs `pg_config` on PATH. On macOS:

```bash
brew install libpq
echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc
```

## Never

- Don't add a root-level command that assumes one venv. State which subproject to `cd` into.
- Don't invoke `dbt` via `poetry`. dbt Cloud CLI is system-installed.
- Don't disable pre-commit hooks to make a commit go through. CI runs `pre-commit run --all-files` and will catch it.
- Don't commit secrets. `.env.example` is the only env file in git.
