from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, coalesce, concat_ws, sha2, current_timestamp, 
    from_json, regexp_replace, trim, when, date_format, to_date, row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import os
import sys

BRONZE_BASE_PATH = "s3a://real-estate-bronze/bronze/{spider_name}/"
SILVER_PROJECT_PATH = "s3a://real-estate-silver/silver/projects" 
PROJECT_SPIDERS = [
    "chotot_api",
    "meeyproject_api",
    "onehousing_api"
]

def initialize_spark_session():

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    spark = SparkSession.builder \
        .appName("SilverProjectETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_bronze_paths_for_incremental_load(start_date):
    """
    Generates a list of S3A paths for Incremental reading, only filtering files
    whose date prefix matches the start_date (YYYYMMDD).

    :param start_date: The date the DAG is running (YYYY-MM-DD).
    :return: List of S3A paths with a wildcard.
    """

    year = start_date[:4]
    month = start_date[5:7]
    day_prefix = start_date.replace('-', '') 
    
    all_paths = []
    for spider_name in PROJECT_SPIDERS:
        # Path only reads files in the year/month partition whose name starts with YYYYMMDD
        # Example: s3a://.../bronze/chotot_api/year=2025/month=10/20251008*.jsonl
        path_template = BRONZE_BASE_PATH.format(spider_name=spider_name) + f"year={year}/month={month}/{day_prefix}*.jsonl"
        all_paths.append(path_template)
        
    return all_paths

def run_silver_project_etl(spark: SparkSession, start_date: str):
    """
    Executes the Incremental Load ETL process and MERGE INTO the Delta Table.
    :param start_date: The start date for the load (from Airflow {{ ds }}).
    """
    
    BRONZE_PATHS = get_bronze_paths_for_incremental_load(start_date)
    print(f"Start reading Bronze Layer Incremental data from date: {start_date}")
    print(f"Reading paths: {BRONZE_PATHS}")
    
    try:
        df_bronze = spark.read.json(BRONZE_PATHS)    
    except Exception as e:
        print(f"Error when reading Bronze data: {e}")
        return

    if df_bronze.rdd.isEmpty():
        print("No new data found in today's directories. Stopping ETL.")
        return
        
    df_bronze.printSchema()
    print(f"Total number of raw records read: {df_bronze.count()}")


if __name__ == "__main__":
    airflow_exec_date = os.environ.get("AIRFLOW_EXECUTION_DATE", datetime.now().strftime("%Y-%m-%d"))
    
    try:
        spark_session = initialize_spark_session()
        run_silver_project_etl(spark_session, airflow_exec_date)
        spark_session.stop()
    except Exception as e:
        print(f"Error when running Spark: {e}")
        sys.exit(1)
