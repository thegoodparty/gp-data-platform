# Local Apache Airflow Setup with Docker Compose

This guide explains how to set up Apache Airflow locally using Docker Compose. Alternatively, you can use the [Astro CLI](https://www.astronomer.io/docs/astro/cli/get-started-cli) to manage the local Airflow environment.

## Setup Instructions

1. Create .env file for Airflow UID:

For Linux:
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

For other operating systems:
```bash
echo -e "AIRFLOW_UID=50000" > .env
```

2. Initialize the Airflow database:
```bash
docker compose up airflow-init
```

3. Start all Airflow services:
```bash
docker compose up -d
```

The webserver can be accessed at:

- host: http://localhost:8080
- username: `airflow`
- password: `airflow`


## Project Structure

- `./dags`: DAGs placed here are automatically picked up by Airflow
- `./logs`: Contains logs from task execution and scheduler
- `./plugins`: Place custom plugins here
- `./config`: For custom log parsers or `airflow_local_settings.py`

## Cleaning Up

To stop and remove containers, delete volumes, and remove images:
```bash
docker compose down --volumes --rmi all
```

## Adding Custom Dependencies

1. Create a `requirements.txt` file in your project directory
2. Create a `Dockerfile`:
```dockerfile
FROM apache/airflow:2.10.5
COPY requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
```

3. Update `docker-compose.yaml`:
```yaml
# Comment out the image line and uncomment the build line:
# image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.5}
build: .
```

4. Rebuild and start:
```bash
docker compose build
docker compose up -d
```

## Notes

For more detailed information, refer to the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/).
