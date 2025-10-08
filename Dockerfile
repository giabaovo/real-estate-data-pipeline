FROM python:3.11-slim

# Thiết lập biến môi trường
ENV PYTHONUNBUFFERED 1
# Thiết lập UID Airflow để tránh lỗi quyền truy cập file trong môi trường Docker
ENV AIRFLOW_UID=50000

# Tạo thư mục làm việc trong container
WORKDIR /opt/airflow

# Cài đặt các dependencies hệ thống (cần cho Scrapy/MinIO Client)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev libxslt1-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Sao chép và cài đặt các thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn Scrapy vào thư mục /app
# Thư mục real_estate_scrappers nằm cùng cấp với Dockerfile
COPY real_estate_scrappers real_estate_scrappers

# Chuyển thư mục làm việc vào thư mục dự án Scrapy (nơi chứa scrapy.cfg)
WORKDIR /opt/airflow/real_estate_scrappers
