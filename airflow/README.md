# Local Apache Airflow Setup with Docker Compose

This guide explains how to set up Apache Airflow locally using Docker Compose.

## Setup Instructions

1. Create project directory structure:
```bash
mkdir -p ./dags ./logs ./plugins ./config
```

2. Download the official docker-compose.yaml:
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'
```

3. Create .env file for Airflow UID:

For Linux:
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

For other operating systems:
```bash
echo -e "AIRFLOW_UID=50000" > .env
```

4. Initialize the Airflow database:
```bash
docker compose up airflow-init
```

5. Start all Airflow services:
```bash
docker compose up -d
```

## Accessing Airflow

- **Web Interface**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`

- **CLI Access**:
```bash
docker compose run airflow-worker airflow info
```

Optional: Download the convenience script for easier CLI access:
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/airflow.sh'
chmod +x airflow.sh
```

Then use:
```bash
./airflow.sh info
```

## Project Structure

- `./dags`: Place your DAG files here
- `./logs`: Contains logs from task execution and scheduler
- `./plugins`: Place your custom plugins here
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

- This setup includes:
  - Webserver
  - Scheduler
  - Worker
  - Triggerer
  - Postgres database
  - Redis message broker

For more detailed information, refer to the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/).
