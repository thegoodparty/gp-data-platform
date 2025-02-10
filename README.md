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

### Data Serving
- NestJS API for serving transformed data
- GraphQL or RESTful endpoints for data access

## Project Structure

The project structure will be added here as it

## Development Practices

### Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality and consistency. The hooks include:
- General file linting (trailing whitespace, file endings, YAML checks)
- Python code formatting (black)
- Python import sorting (isort)
- Python code linting (flake8)
- Python type checking (mypy)

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

This ensures that all code changes adhere to the project's quality standards, even if contributors haven't set up pre-commit locally.
