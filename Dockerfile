FROM python:3.11-slim

ENV PYTHONUNBUFFERED 1
ENV AIRFLOW_UID=50000

WORKDIR /opt/airflow

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev libxslt1-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY real_estate_scrappers real_estate_scrappers

WORKDIR /opt/airflow/real_estate_scrappers
