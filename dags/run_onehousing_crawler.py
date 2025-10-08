import pendulum
import os
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

# =================================================================
# 1. Cấu hình Docker Image Build
# =================================================================
# Tên image sẽ được xây dựng từ Dockerfile ở thư mục gốc
SCARPY_IMAGE_NAME = 'real-estate-scrapy-env:latest'
# Thư mục chứa mã nguồn Scrapy (đã được mount vào /opt/airflow/real_estate_scrappers)
SCARPY_PROJECT_PATH = '/opt/airflow/real_estate_scrappers'
# Tên Spider cần chạy (được định nghĩa trong onehousing_api.py)
SPIDER_NAME = 'onehousing_api'

@dag(
    dag_id='onehousing_real_estate_scrapy_pipeline',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule_interval='30 8 * * *', # Chạy mỗi 2 phút
    catchup=False,
    tags=['scrapy', 'minio', 'api', 'onehousing'],
    default_args={
        'owner': 'airflow',
        'execution_timeout': pendulum.duration(minutes=1),
        'retries': 1,
    }
)
def run_onehousing_crawler_dag():
    run_scrapy_crawler = DockerOperator(
        task_id='run_onehousing_api_spider',
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

onehousing_pipeline = run_onehousing_crawler_dag()
