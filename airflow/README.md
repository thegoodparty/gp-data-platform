# Astro CLI Guide

This guide provides comprehensive instructions for getting started with Airflow using the Astro CLI, based on the [official Astronomer documentation](https://www.astronomer.io/docs/astro/cli/get-started-cli#next-steps).

## Overview

The Astro CLI allows you to run Airflow on your local machine, providing a complete development environment for building, testing, and deploying Airflow DAGs. This tool creates a local Airflow environment with all necessary components running in Docker containers.

## Prerequisites

- Docker installed and running on your machine
- Access to a terminal/command line

## Installation

### macOS
```bash
brew install astro
```

### Windows (with winget)
```bash
winget install Astronomer.Astro
```

### Linux
```bash
curl -sSL install.astronomer.io | sudo bash -s
```

## Getting Started

### Step 0: Activate local python environment with poetry
The current version of astro used in project is 3.0-7, as defined in `astro/Dockerfile`. The release notes can be found [here](https://www.astronomer.io/docs/astro/runtime-release-notes#astro-runtime-30-7), which list Airflow 3.0.4 as the included package over python 3.12. Packages for the development environment are defined in `pyproject.toml`.

### Step 1: Use the Existing Astro Project

This repository already contains a complete Astro project in the `airflow/astro` directory. This project includes all the files necessary to run Airflow, including dedicated folders for DAGs, plugins, and dependencies. The project is ready to use and contains example DAGs and configurations.

Navigate to the existing project directory:

```bash
cd airflow/astro
```

The project is already set up with:
- Example DAGs in the `dags/` directory
- Custom functions and data in the `include/` directory
- Required dependencies in `packages.txt` and `requirements.txt`
- Docker configuration in `Dockerfile`

### Step 2: Run Airflow Locally

Running your project locally allows you to test your DAGs before deploying them to production. While not required for deployment, Astronomer recommends always using the Astro CLI to test locally first. These steps are adapted from the [Astro Getting Started Guide](https://www.astronomer.io/docs/astro/cli/get-started-cli#step-3-run-airflow-locally).

1. You should already be in the project directory (`airflow/astro`). If not, navigate there:
   ```bash
   cd airflow/astro
   ```

2. Start your project in a local Airflow environment:
   ```bash
   astro dev start
   ```

This command builds your project and spins up 4 Docker containers:
- **Postgres**: Airflow's metadata database
- **Webserver**: Renders the Airflow UI
- **Scheduler**: Monitors and triggers tasks
- **Triggerer**: Runs Triggers and signals tasks to resume (for deferrable operators)

3. After successful build, open the Airflow UI at `https://localhost:8080/`

4. Find your DAGs in the `dags` directory in the Airflow UI

**Note**: The Astro CLI uses port `8080` for the Airflow webserver and port `5432` for the Airflow metadata database by default. If these ports are already in use, you'll need to resolve conflicts before proceeding. The ports can be changed according to the [Astro FAQs](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver), which local configs stored in `airflow/astro/.astro/config.yaml`.

### Step 3: Develop Locally

Once your project is running locally, you can start developing by:
- Adding new DAGs
- Installing dependencies
- Setting environment variables
- Adding plugins

Most changes (including DAG code updates) are applied automatically without rebuilding. However, you must rebuild and restart for changes to:
- `packages.txt`
- `Dockerfile`
- `requirements.txt`
- `airflow_settings.yaml`

To restart your local environment:
```bash
astro dev restart
```

To stop without restarting:
```bash
astro dev stop
```

## Project Structure

This Astro project is located in `airflow/astro/` and includes:
```
airflow/astro/
├── dags/                 # Your DAG files
├── include/              # Additional files (data, custom functions)
├── Dockerfile            # Custom container configuration
├── packages.txt          # System-level dependencies
├── requirements.txt      # Python dependencies
└── README.md             # Project documentation
```

## Example DAG

The Learning Airflow template includes an example DAG called `example-astronauts` that demonstrates:
- A basic ETL pipeline
- Querying the Open Notify API for astronauts in space
- Using the TaskFlow API
- Dynamic task mapping

## Next Steps

After completing the basic setup, consider:

1. **Configure the CLI**: Set up authentication and preferences
2. **Authenticate to cloud services**: Connect to cloud data sources for testing
3. **Build and run projects locally**: Develop more complex workflows
4. **Deploy to Astro**: Move from local development to production

## Troubleshooting

### Port Conflicts
If you encounter port conflicts:
- Check what's running on ports 8080 and 5432
- Stop conflicting services or configure different ports
- Use `astro dev start --port 8081` to specify alternative ports

### Container Issues
- Use `astro dev logs` to view container logs
- Run `astro dev restart` to rebuild and restart containers
- Check Docker is running and has sufficient resources

### Dependencies
- Ensure all Python packages are listed in `requirements.txt`
- System packages should be in `packages.txt`
- Rebuild after dependency changes

## Additional Resources

- [Astro CLI Documentation](https://docs.astronomer.io/astro/cli/overview)
- [Astro Project Development](https://docs.astronomer.io/astro/develop-project)
- [Airflow Documentation](https://airflow.apache.org/docs/)

---

*This guide is based on the [official Astronomer documentation](https://www.astronomer.io/docs/astro/cli/get-started-cli#next-steps) and provides practical steps for getting started with Astro CLI.*
