import pendulum
import os
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

SCARPY_IMAGE_NAME = 'real-estate-scrapy-env:latest'
SCARPY_PROJECT_PATH = '/opt/airflow/real_estate_scrappers'
SPIDER_NAME = 'meeyproject_api'

@dag(
    dag_id='meeyproject_api_scrapy_pipeline',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule_interval='30 8 * * *', 
    catchup=False,
    tags=['scrapy', 'minio', 'api', 'meeyland'],
    default_args={
        'owner': 'airflow',
        'execution_timeout': pendulum.duration(minutes=20), 
        'retries': 1,
    }
)
def run_meeyproject_crawler_dag():
    
    run_scrapy_crawler = DockerOperator(
        task_id='run_meeyproject_api_spider',
        image=SCARPY_IMAGE_NAME,
        command=f"scrapy crawl {SPIDER_NAME}",
        
        working_dir=SCARPY_PROJECT_PATH,
        
        network_mode='data_network',

        environment={
            'SCRAPY_PROCESS_RUN_ID': '{{ run_id }}', 
            'MINIO_ENDPOINT': os.environ.get('MINIO_ENDPOINT', 'minio:9000') ,
            'MINIO_ACCESS_KEY': os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
            'MINIO_BUCKET': os.environ.get('MINIO_BUCKET', 'real-estate-bronze'),
        },
        auto_remove=True,
        mount_tmp_dir=False,
    )

    run_scrapy_crawler

meeyproject_pipeline = run_meeyproject_crawler_dag()