# gp-data-platform

A comprehensive data platform that handles data ingestion, transformation, orchestration, and serving.

## Components

### Data Orchestration (`airflow/astro/`)
- Airflow DAGs (Python) for workflow orchestration, hosted on [Astronomer](https://www.astronomer.io/)
- Deployed automatically via Git Deploys: feature branches deploy to `astro-dev`, `main` deploys to `astro-prod`
- See [`airflow/astro/README.md`](airflow/astro/README.md) for deployment workflow and local development setup

### Data Ingestion
- Airbyte configurations (YAML) for data source connections and synchronization

### Data Transformation
- dbt models for data transformation and modeling
- SQL, Jinja templating, and Python utilities
- Data quality tests and documentation

## Project Structure

The project structure will be added here as it evolves

## Development Practices

### System tools (Mac)

Apple's Xcode CLI Tools ship an outdated `git`. Upgrade to current upstream git via [Homebrew](https://brew.sh):

```bash
brew install git
git --version    # confirm 2.4x or newer
```

Pre-commit hooks and `uv run git` both invoke whichever `git` is first on your `PATH` — there's only one git installation; `uv run` just makes the project env available to hook subprocesses (so the pytest hook can find `pyspark`/`airflow`).

### Using Python locally

To manage Python versions locally, we use [`pyenv`](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation). This ensures consistent Python versions across development environments.

Environments are managed by [`uv`](https://docs.astral.sh/uv/getting-started/installation/). Each subproject with a `pyproject.toml` has its own `uv.lock` and its own virtualenv — this repo does not have a single shared environment.

Enter the subdirectory of development and run `uv sync` to install dependencies exactly as locked. Run commands inside that environment with `uv run <command>` (for example `uv run pytest`); no separate activation step is needed. Dependencies can be added with `uv add <package>`, which updates both `pyproject.toml` and `uv.lock`.

For integration with VS Code, select the interpreter at `.venv/bin/python` inside the subproject directory. For example:
```shell
dbt/.venv/bin/python
```

### Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality and consistency. The hooks include:
- General file linting (trailing whitespace, file endings, YAML checks)
- Python code formatting, linting and type checking (black, isort, flake8, mypy)

To set up pre-commit:

1. Install dependencies (includes pre-commit as a dev dependency):
```bash
cd dbt
uv sync
```

2. Install the git hooks:
```bash
pre-commit install
```

3. (Optional) Run against all files:
```bash
pre-commit run --all-files
```

The pre-commit hooks will run automatically on `git commit`. If any hooks fail, fix the issues and try committing again.

The pre-commit checks are also run automatically via GitHub Actions:
- On all pull requests
- On all pushes to main/master branches
