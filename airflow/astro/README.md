# Airflow (Astro)

Apache Airflow DAGs managed via [Astronomer](https://www.astronomer.io/). This project uses [Astro Runtime 3.1](https://docs.astronomer.io/astro/runtime-release-notes) (based on Airflow 3.x).

## Project Structure

```
airflow/astro/
├── dags/                       # Airflow DAG definitions
├── include/
│   └── custom_functions/       # Reusable Python modules used by DAGs
├── tests/
│   └── dags/                   # DAG validation tests
├── plugins/                    # Custom Airflow plugins
├── Dockerfile                  # Astro Runtime base image + customizations
├── requirements.txt            # Python dependencies installed in the Docker image
└── packages.txt                # OS-level packages (apt-get)
```

## Environments

| Environment | Deployment | Branch Mapping |
|-------------|-----------|----------------|
| Dev | `astro-dev` | Your feature branch (e.g., `data-1534/my-feature`) |
| Prod | `astro-prod` | `main` |

Infrastructure for both deployments is managed in [gp-terraform-dataplatform](https://github.com/thegoodparty/gp-terraform-dataplatform).

## Deployment

Deployments are handled automatically via **Astronomer Git Deploys** (configured in Astro UI > Workspace Settings > Git Deploys). The `airflow/astro` subdirectory is the Astro Project Path.

When files under `airflow/astro/` change on a mapped branch, Astronomer automatically:
- Rebuilds the Docker image (full image deploy) when `Dockerfile`, `requirements.txt`, `include/`, or `packages.txt` change
- Syncs only DAG files (DAG-only deploy) when only files in `dags/` change

Branch mappings:
- **astro-prod**: Always mapped to `main`. Merging a PR to main auto-deploys to prod.
- **astro-dev**: Mapped to whichever feature branch a developer is currently testing. Updated manually in the Astro UI as needed.

## Development Workflow

### 1. Create a feature branch

```bash
git checkout main && git pull
git checkout -b data-XXXX/my-feature
```

### 2. Develop and test locally

Start a local Airflow environment:

```bash
cd airflow/astro
astro dev start
```

This builds the Docker image locally and starts Airflow at `http://localhost:8080`.

To run DAG validation tests:

```bash
astro dev pytest
```

### 3. Map your branch to astro-dev

In the Astro UI, go to **Workspace Settings > Git Deploys** and update the `astro-dev` branch mapping to point to your feature branch (e.g., `data-XXXX/my-feature`).

Once mapped, every push to your branch auto-deploys to **astro-dev**.

> **Note**: Only one branch can be mapped to `astro-dev` at a time. Coordinate with the team if multiple people need the dev environment.

### 4. Configure Airflow Variables

DAGs read configuration from [Airflow Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) at runtime (not hardcoded). Set these in the Astro UI under **Admin > Variables** for each environment.

Both dev and prod environments use the same DAG code but different Variable values, allowing you to point dev at test resources and prod at production resources.

### 5. Test in dev

- Verify the DAG appears in the Astro UI DAG list
- Trigger a manual run and check task logs
- Validate data changes in downstream systems

### 6. Promote to production

Once validated in dev, open a PR from your feature branch to `main`:

```bash
gh pr create --base main --head data-XXXX/my-feature
```

After PR review and merge to `main`, Astronomer automatically deploys to **astro-prod**.

## Keeping Versions in Sync

The `Dockerfile` pins the Astro Runtime version (e.g., `runtime:3.1-12`). This must match what the Astronomer deployments are running. To check:

```bash
astro deployment list
```

If Astronomer upgrades the runtime, update the tag in the `Dockerfile` to match.

## Adding Dependencies

1. Add the Python package to `requirements.txt`
2. Add the matching pinned version to `airflow/pyproject.toml` (used by the local poetry dev environment)
3. For OS-level dependencies, add to `packages.txt`
4. Test locally with `astro dev start` to verify the image builds
