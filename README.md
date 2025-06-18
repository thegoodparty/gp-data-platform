# gp-data-platform

A comprehensive data platform that handles data ingestion, transformation, orchestration, and serving.

## Components

### Data Orchestration
- Airflow DAGs (Python) for workflow orchestration
- Local development environment setup using Docker Compose
    - Docker setup for Airflow
    - Local database configuration
    - Development workflows
    - Testing procedures

### Data Ingestion
- Airbyte configurations (YAML) for data source connections and synchronization

### Data Transformation
- dbt models for data transformation and modeling
- SQL, Jinja templating, and Python utilities
- Data quality tests and documentation

## Project Structure

The project structure will be added here as it evolves

## Development Practices

### Using python locally

To manage Python versions locally, we use [`pyenv`](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation). This ensures consistent Python versions across development environments.

The environment is managed by [`poetry`](https://python-poetry.org/docs/#installing-with-pipx), which is installed via [`pipx`](https://pipx.pypa.io/stable/installation/).

Enter the subdirectory of development and run `poetry install` where there is a `pyproject.toml` to install dependencies. To [activate the environment](https://python-poetry.org/docs/managing-environments/#bash-csh-zsh), run `eval $(poetry env activate)`, and `deactivate` to deactivate. Dependencies can be added with `poetry add <package>`.

For integration with VS Code, use the output path from `poetry env info --executable` when selecting the Python interpreter. For example on Mac:
```shell
/Users/my_user_name/Library/Caches/pypoetry/virtualenvs/dbt-goodparty-gN6X-qpi-py3.13/bin/python
```

**Important setup requirements:**
- You must use Python **3.13 or higher** for your local environment.
- You must install **Poetry version 2.1.1 or higher** using **regular `pip`**, not `pipx` (e.g. `pip install poetry==2.1.1`).
- You should **not modify the `pyproject.toml` manually** to change Python constraints.
- You must explicitly tell Poetry which Python version to use:
```bash
poetry env use python3.13
```

### Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality and consistency. The hooks include:
- General file linting (trailing whitespace, file endings, YAML checks)
- Python code formatting, linting and type checking (black, isort, flake8, mypy)

To set up pre-commit:

1. Install pre-commit:
```bash
pip install pre-commit
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
