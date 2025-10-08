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
# Tên Spider cần chạy (được định nghĩa trong meeyproject_api_spider.py)
SPIDER_NAME = 'meeyproject_api' # Đã được đổi thành 'meeyproject_api'

@dag(
    dag_id='meeyproject_api_scrapy_pipeline',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    # LỊCH CHẠY ĐỒNG THỜI: 8 giờ 30 phút sáng hàng ngày
    schedule_interval='30 8 * * *', 
    catchup=False,
    tags=['scrapy', 'minio', 'api', 'meeyland'],
    default_args={
        'owner': 'airflow',
        # Tăng timeout lên 20 phút vì Meeyproject có nhiều items (5k+) và cần nhiều request
        'execution_timeout': pendulum.duration(minutes=20), 
        'retries': 1,
    }
)
def run_meeyproject_crawler_dag():
    
    run_scrapy_crawler = DockerOperator(
        task_id='run_meeyproject_api_spider',
        image=SCARPY_IMAGE_NAME,
        # Lệnh chạy Scrapy: scrapy crawl <spider_name>
        command=f"scrapy crawl {SPIDER_NAME}",
        
        # Thư mục làm việc trong Container Docker
        working_dir=SCARPY_PROJECT_PATH,
        
        # Tên mạng phải khớp với mạng trong docker-compose.yml (data_network)
        network_mode='data_network',
        
        # TRUYỀN BIẾN MÔI TRƯỜNG VÀO CONTAINER (để Scrapy đọc)
        environment={
            # Truyền Run ID của Airflow vào Scrapy Settings
            'SCRAPY_PROCESS_RUN_ID': '{{ run_id }}', 
            'MINIO_ENDPOINT': os.environ.get('MINIO_ENDPOINT', 'minio:9000') ,
            'MINIO_ACCESS_KEY': os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
            'MINIO_BUCKET': os.environ.get('MINIO_BUCKET', 'real-estate-bronze'),
        },
        
        # Tự động xóa Container sau khi Task hoàn thành
        auto_remove=True,
        mount_tmp_dir=False,
    )

    run_scrapy_crawler

# Gán function vào biến để Airflow nhận diện
meeyproject_pipeline = run_meeyproject_crawler_dag()