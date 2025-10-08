import pendulum
import os
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

SCARPY_IMAGE_NAME = 'real-estate-scrapy-env:latest'
# Thư mục chứa mã nguồn Scrapy (đã được mount vào /opt/airflow/real_estate_scrappers)
SCARPY_PROJECT_PATH = '/opt/airflow/real_estate_scrappers'
# Tên Spider cần chạy (ĐÃ CẬP NHẬT thành 'chotot_api')
SPIDER_NAME = 'chotot_api'

@dag(
    dag_id='chotot_project_scrapy_pipeline',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule_interval='30 8 * * *', 
    catchup=False,
    tags=['scrapy', 'minio', 'api', 'chotot'],
    default_args={
        'owner': 'airflow',
        # Tăng timeout lên 30 phút vì Chợ Tốt có lượng dữ liệu lớn và dùng limit=100
        'execution_timeout': pendulum.duration(minutes=30), 
        'retries': 1,
    }
)
def run_chotot_crawler_dag():
    run_scrapy_crawler = DockerOperator(
        task_id='run_chotot_project_api_spider',
        image=SCARPY_IMAGE_NAME, # Image đã được xây dựng
        
        # LỆNH ĐÃ SỬA: Thiết lập PYTHONPATH để Scrapy có thể tìm thấy các module và Spider.
        command=f"scrapy crawl {SPIDER_NAME}",
        
        # Đặt thư mục làm việc trong Container Docker là thư mục mã nguồn Scrapy
        working_dir=SCARPY_PROJECT_PATH,
        
        # Tên mạng Docker
        network_mode='data_network',
        
        # Chuyển các biến môi trường cần thiết
        environment={
            # Biến môi trường để Pipeline đọc ID lần chạy của Airflow
            'SCRAPY_PROCESS_RUN_ID': '{{ run_id }}', 
            
            # Cấu hình MinIO (đọc từ .env của Airflow)
            'MINIO_ENDPOINT': os.environ.get('MINIO_ENDPOINT', 'minio:9000') ,
            'MINIO_ACCESS_KEY': os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
            'MINIO_BUCKET': os.environ.get('MINIO_BUCKET', 'real-estate-bronze')
        },
        
        auto_remove=True, # Tự động xóa container sau khi hoàn thành
        mount_tmp_dir=False,
    )

    run_scrapy_crawler

# Khởi tạo DAG
chotot_pipeline = run_chotot_crawler_dag()
