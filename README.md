# 🏠 REAL ESTATE PIPELINE: Real Estate Data ETL System

This project implements an automated, scalable Extract, Transform, Load (ETL) Data Pipeline designed to collect, standardize, and store real estate listing data from multiple API and web scraping sources. It enforces the Medallion Architecture paradigm to ensure data quality, lineage, and readiness for advanced analytics and reporting.

### Prerequisites
- [Docker](https://www.docker.com/get-started/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Installing

A step by step series of examples that tell you how to get a development
environment running

1. **Configure Environment Variables:**
   Ensure the project's root directory contains a properly configured .env file detailing MinIO (S3), Postgres, and Airflow user credentials like below.

    ```shell
    MINIO_ENDPOINT=<MINIO_ENDPOINT>
    MINIO_ACCESS_KEY=<MINIO_ACCESS_KEY>
    MINIO_SECRET_KEY=<MINIO_SECRET_KEY>
    MINIO_BUCKET=<MINIO_BUCKET>
    ```

2. **Build PySpark Processor Image:**
   This step creates the custom Docker image containing Spark, the Delta Lake dependencies, and the S3 connector required to execute the transformation jobs.

   ```shell
   docker compose build pyspark-processor
   ```

3. **Initialize Airflow Database:**
   Start the necessary services, then run the database migration.

    ```shell
    # Start Postgres and Airflow Webserver
    docker compose up -d postgres airflow-webserver

    # Run database migration (wait 10-20 seconds for Postgres to start)
    docker compose run --rm airflow-webserver airflow db migrate
    ```

4. **Create Airflow Admin User:**
   Create the administrative account used to access the Airflow Web UI.

   ```shell
    docker compose run --rm airflow-webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
   ```

Once all installation steps are complete, start the full system:

```shell
docker compose up -d
```

### Built With

- Apache Airflow - Workflow orchestration and scheduling
- PySpark - Large-scale data transformation and processing
- Delta Lake - Open-source storage layer providing ACID transactions
- MinIO - S3-compatible object storage for local development
- Docker - Containerization
