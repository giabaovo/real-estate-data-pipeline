"""
PySpark Session Initialization
Shared configuration for all ETL scripts
"""

import os
from pyspark.sql import SparkSession

def initialize_spark_session(app_name="RealEstateETL"):
    """
    Initialize Spark session with S3A and Delta Lake support
    """
    
    # MinIO/S3 configuration
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    # Spark configuration
    spark_master = os.environ.get("SPARK_MASTER", "local[*]")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY", "4g")
    
    print(f"🔧 Initializing Spark session: {app_name}")
    print(f"   Master: {spark_master}")
    print(f"   Driver memory: {driver_memory}")
    print(f"   Executor memory: {executor_memory}")
    print(f"   MinIO endpoint: {minio_endpoint}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(spark_master) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark session initialized successfully")
    print(f"   Spark version: {spark.version}")
    
    return spark
