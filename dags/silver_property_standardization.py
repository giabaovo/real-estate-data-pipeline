import pendulum
import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator


DATA_PROCESSING_HOST_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_processing'))

PYSPARK_IMAGE = 'pyspark-etl-processor:latest'
ETL_SCRIPT_PATH = '/app/silver_etl_script.py'

BRONZE_DAGS = {
    # 'chotot_api': 'chotot_project_scrapy_pipeline', # Can't sensor this task, need to fix later
    'meeyproject_api': 'meeyproject_api_scrapy_pipeline',
    'onehousing_api': 'onehousing_real_estate_scrapy_pipeline'
}

BRONZE_RUN_HOUR = 1
BRONZE_RUN_MINUTE = 30
BRONZE_RUN_TZ = "UTC"

def calculate_bronze_execution_date(dt: datetime, **context) -> pendulum.DateTime:
    """
    Tính toán logical date (Run ID) của Bronze DAG.
    Chờ DAG crawl của ngày trước (ds - 1) để khớp với schedule 8:30 AM VN (1:30 UTC).
    """
    dt_pendulum = pendulum.instance(dt).in_tz(BRONZE_RUN_TZ)
    prev_day = dt_pendulum.subtract(days=1)
    target_dt = prev_day.set(
        hour=BRONZE_RUN_HOUR,
        minute=BRONZE_RUN_MINUTE,
        second=0,
        microsecond=0
    )
    return target_dt

@dag(
    dag_id='silver_project_etl_incremental',
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['etl', 'pyspark', 'delta', 'silver'],
    default_args={
        'owner': 'airflow',
        'execution_timeout': pendulum.duration(minutes=30),
        'retries': 1,
    }
)
def silver_project_etl_incremental():
    sensors = []
    for source, dag_id in BRONZE_DAGS.items():
        sensor = ExternalTaskSensor(
            task_id=f'wait_for_{source}_crawl',
            external_dag_id=dag_id,
            external_task_id=f'run_{source}_spider',
            execution_date_fn=calculate_bronze_execution_date,
            timeout=60 * 60 * 2,
            poke_interval=60 * 1,
            mode='reschedule',
            soft_fail=False
        )
        sensors.append(sensor)

    join_sensors = DummyOperator(
        task_id='join_crawl_sensors',
        doc_md="Join all sensors before run ETL Silver."
    )

    run_silver_etl = DockerOperator(
        task_id='run_silver_project_etl_delta_merge',
        image=PYSPARK_IMAGE,
        command=f"/spark/bin/spark-submit {ETL_SCRIPT_PATH}",
        network_mode='data_network',
        environment={
            'MINIO_ENDPOINT': os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
            'MINIO_ACCESS_KEY': os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
            'AIRFLOW_EXECUTION_DATE': '{{ ds }}'
        },
        auto_remove=True,
        mount_tmp_dir=False,
    )

    sensors >> join_sensors >> run_silver_etl

silver_project_etl_dag = silver_project_etl_incremental()