"""
Airflow DAG for Gold Layer ML Features ETL
Phase 1: Foundation - Basic features for ML training
"""

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from docker.types import Mount

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'gold_ml_features_pipeline',
    default_args=default_args,
    description='Build ML features from Silver layer for real estate price prediction',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM (after Silver ETL)
    catchup=False,
    tags=['gold', 'ml', 'features', 'phase1'],
)

# Task: Run Gold ML ETL
gold_ml_etl_task = DockerOperator(
    task_id='run_gold_ml_etl',
    image='pyspark-etl-processor:latest',  # Match docker-compose.yml
    api_version='auto',
    auto_remove=True,
    force_pull=False,  # Use local image, don't pull from Docker Hub
    command='python /app/gold_ml_etl.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='data_network',  # Match docker-compose.yml
    environment={
        'SPARK_MASTER': 'local[*]',
        'SPARK_DRIVER_MEMORY': '4g',
        'SPARK_EXECUTOR_MEMORY': '4g',
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin',
        'SILVER_PROJECT_PATH': 's3a://real-estate-silver/silver/projects',
        'GOLD_ML_FEATURES_PATH': 's3a://real-estate-gold/marts/ml_features/current',
        'AIRFLOW_EXECUTION_DATE': '{{ ds }}',
    },
    mounts=[
        Mount(
            source='/var/run/docker.sock',
            target='/var/run/docker.sock',
            type='bind'
        )
    ],
    mount_tmp_dir=False,
    dag=dag,
)

# Task dependencies
gold_ml_etl_task
