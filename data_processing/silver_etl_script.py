"""
Silver Layer ETL - Production Ready Version
Integrates all modules: schema mapping, data quality, transformations
"""

import os
import sys

# Add current directory to Python path for module imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, coalesce, concat_ws, sha2, current_timestamp, 
    regexp_replace, trim, when, date_format, to_date, row_number,
    lower, length, to_timestamp, mean, stddev, abs as spark_abs,
    split, expr, array, struct, get_json_object, from_json, size, flatten, from_unixtime
)
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from schema_config import (
    SOURCE_MAPPINGS, TYPE_CONVERSIONS, VALIDATION_RULES,
    DEFAULT_VALUES, get_required_fields, SPECIAL_TRANSFORMATIONS,
    get_special_transformations, SILVER_SCHEMA
)
from data_quality_checks import DataQualityChecker, REAL_ESTATE_QUALITY_CHECKS
from transformation_utils import DataTransformer

# ===========================
# CONFIGURATION
# ===========================
BRONZE_BASE_PATH = "s3a://real-estate-bronze/bronze/{spider_name}/"
SILVER_PROJECT_PATH = "s3a://real-estate-silver/silver/projects"
QUARANTINE_PATH = "s3a://real-estate-silver/quarantine/projects"
METADATA_PATH = "s3a://real-estate-silver/metadata/pipeline_logs"

PROJECT_SPIDERS = ["chotot_api", "meeyproject_api", "onehousing_api"]

# Quality thresholds
QUALITY_THRESHOLDS = {
    "min_completeness_score": 0.5,  # 50% minimum data completeness
    "max_invalid_percentage": 0.1,   # Max 10% invalid records
    "max_duplicate_percentage": 0.05  # Max 5% duplicates
}

# ===========================
# MINIO BUCKET INITIALIZATION
# ===========================
def ensure_minio_buckets():
    """Ensure required MinIO buckets exist, create if not"""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    try:
        client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        required_buckets = ["real-estate-bronze", "real-estate-silver", "real-estate-gold"]
        
        for bucket_name in required_buckets:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"✨ Created MinIO bucket: {bucket_name}")
            else:
                print(f"✅ MinIO bucket exists: {bucket_name}")
                
    except S3Error as e:
        print(f"⚠️  MinIO bucket check warning: {e}")
    except Exception as e:
        print(f"⚠️  MinIO connection warning: {e}")

# ===========================
# SPARK SESSION
# ===========================
def initialize_spark_session():
    """Initialize Spark Session with optimized configurations"""
    from delta import configure_spark_with_delta_pip
    
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    # Create builder with Delta Lake configuration
    builder = SparkSession.builder \
        .appName("SilverProjectETL_Enhanced") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    
    # Configure Delta Lake with pip-installed packages
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ===========================
# STEP 1: INGESTION
# ===========================
def get_bronze_paths_for_incremental_load(start_date):
    """Generate S3A paths for incremental reading"""
    year = start_date[:4]
    month = start_date[5:7]
    day_prefix = start_date.replace('-', '')
    
    all_paths = []
    for spider_name in PROJECT_SPIDERS:
        path_template = BRONZE_BASE_PATH.format(spider_name=spider_name) + \
                       f"year={year}/month={month}/{day_prefix}*.jsonl"
        all_paths.append(path_template)
    
    return all_paths

def read_bronze_data(spark: SparkSession, start_date: str):
    """Read incremental data from Bronze Layer"""
    bronze_paths = get_bronze_paths_for_incremental_load(start_date)
    print(f"\n{'='*70}")
    print(f"📥 STEP 1: INGESTION - Reading Bronze Data")
    print(f"{'='*70}")
    print(f"Date: {start_date}")
    print(f"Paths: {bronze_paths}")
    
    try:
        df_bronze = spark.read.json(bronze_paths)
        record_count = df_bronze.count()
        print(f"✅ Successfully read {record_count:,} raw records")
        return df_bronze
    except Exception as e:
        print(f"❌ Error reading Bronze data: {e}")
        return None

# ===========================
# STEP 2: SCHEMA MAPPING
# ===========================
def apply_schema_mapping(df):
    """Map source-specific schemas to unified Silver schema with special transformations"""
    print(f"\n{'='*70}")
    print(f"🗺️  STEP 2: SCHEMA MAPPING & TRANSFORMATION")
    print(f"{'='*70}")
    
    # Process each source separately
    df_mapped_list = []
    
    for spider_name in PROJECT_SPIDERS:
        df_source = df.filter(col("spider_name") == spider_name)
        
        if df_source.count() == 0:
            print(f"  ⏭️  No records for {spider_name}")
            continue
        
        print(f"  🔄 Processing {spider_name}...")
        
        # Apply special transformations first
        special_transforms = get_special_transformations(spider_name)
        
        # Handle geo coordinate splitting for chotot_api
        if spider_name == "chotot_api":
            if "geo" in df_source.columns:
                print(f"    • Extracting coordinates from 'geo' field")
                df_source = DataTransformer.split_geo_coordinates(df_source, "geo")
        
        # Handle OneHousing API special transformations
        if spider_name == "onehousing_api":
            # Extract bedroom/bathroom ranges from insight_by_bedroom
            if "insight_by_bedroom" in df_source.columns:
                print(f"    • Extracting bedroom/bathroom ranges")
                df_source = DataTransformer.extract_bedroom_bathroom_ranges(df_source)
            
            # Extract quality index names
            if "quality_indexes" in df_source.columns:
                print(f"    • Extracting quality indexes")
                df_source = DataTransformer.extract_quality_index_names(df_source)
            
            # Extract images from albums
            if "albums" in df_source.columns:
                print(f"    • Extracting images from albums")
                df_source = DataTransformer.extract_album_images(df_source)
            
            # Extract first element from array fields
            if "number_basement" in df_source.columns or "number_ele" in df_source.columns:
                print(f"    • Extracting array values (basement, elevators)")
                df_source = DataTransformer.extract_first_from_array(
                    df_source, 
                    {"number_of_basement": "number_basement", "number_of_elevators": "number_ele"}
                )
        
        # Handle OneHousing API special transformations
        if spider_name == "onehousing_api":
            # 1. Convert total_area from hectares to m²
            if "total_area" in df_source.columns:
                print(f"    • Converting total_area from ha to m²")
                df_source = df_source.withColumn(
                    "total_area",
                    when(
                        col("total_area").isNotNull(),
                        col("total_area") * 10000  # 1 ha = 10,000 m²
                    ).otherwise(lit(None))
                )
            
            # 2. Extract images from albums array
            if "albums" in df_source.columns:
                print(f"    • Extracting images from albums")
                try:
                    df_source = df_source.withColumn(
                        "images_extracted",
                        when(
                            col("albums").isNotNull() & (size(col("albums")) > 0),
                            expr("transform(albums, x -> x.images)")
                        ).otherwise(lit(None))
                    ).withColumn(
                        "images_flattened",
                        when(
                            col("images_extracted").isNotNull(),
                            expr("flatten(images_extracted)")
                        ).otherwise(lit(None))
                    ).drop("images_extracted")
                    # Rename for mapping
                    df_source = df_source.withColumn("albums", col("images_flattened")).drop("images_flattened")
                except Exception as e:
                    print(f"      ⚠️ Failed to extract images: {e}")
            
            # 3. Process insight_by_bedroom for apartment_prices
            if "insight_by_bedroom" in df_source.columns:
                print(f"    • Processing apartment prices from insight_by_bedroom")
                try:
                    df_source = df_source.withColumn(
                        "apartment_prices_processed",
                        when(
                            col("insight_by_bedroom").isNotNull(),
                            expr("""
                                transform(insight_by_bedroom, x -> 
                                    struct(
                                        cast(x.number_of_bedroom as int) as number_of_bedroom,
                                        cast(x.min_price as double) as min_price,
                                        cast(x.max_price as double) as max_price,
                                        cast(x.min_carpet_area as double) as min_area,
                                        cast(x.max_carpet_area as double) as max_area
                                    )
                                )
                            """)
                        ).otherwise(lit(None))
                    )
                    # Rename for mapping
                    df_source = df_source.withColumn("insight_by_bedroom", col("apartment_prices_processed")).drop("apartment_prices_processed")
                except Exception as e:
                    print(f"      ⚠️ Failed to process apartment prices: {e}")
        
        # Handle Meeyproject API special transformations
        if spider_name == "meeyproject_api":
            # Extract location coordinates
            if "location" in df_source.columns:
                print(f"    • Extracting coordinates from location")
                df_source = DataTransformer.extract_meeyproject_location(df_source)
            
            # Extract nested fields
            if "investorRelated" in df_source.columns:
                print(f"    • Extracting nested investor information")
                try:
                    df_source = df_source.withColumn(
                        "investor_name",
                        col("investorRelated.investor.name")
                    )
                except:
                    pass
            
            if "juridical" in df_source.columns:
                print(f"    • Extracting juridical description")
                try:
                    df_source = df_source.withColumn(
                        "description",
                        col("juridical.description")
                    )
                except:
                    pass
            
            if "utilities" in df_source.columns:
                print(f"    • Extracting utilities")
                try:
                    df_source = df_source.withColumn(
                        "utilities_internal",
                        col("utilities.basicUtilities")
                    )
                except:
                    pass
            
            # Extract translation fields (ward, district, city)
            translation_fields = {}
            for field in ["ward", "district", "city"]:
                if field in df_source.columns:
                    translation_fields[field] = f"{field}.translation[0].name"
            
            if translation_fields:
                print(f"    • Extracting translation fields")
                df_source = DataTransformer.extract_nested_translation(df_source, translation_fields)
        
        # Apply standard field mappings
        mapping = SOURCE_MAPPINGS.get(spider_name, {})
        for target_field, source_field in mapping.items():
            if source_field in df_source.columns:
                # If target already exists, drop it first to avoid ambiguity
                if target_field in df_source.columns and target_field != source_field:
                    df_source = df_source.drop(target_field)
                df_source = df_source.withColumnRenamed(source_field, target_field)
        
        # Apply type conversions
        for field, field_type in TYPE_CONVERSIONS.items():
            if field in df_source.columns:
                if field_type == "double":
                    df_source = df_source.withColumn(field, col(field).cast("double"))
                elif field_type == "integer":
                    df_source = df_source.withColumn(field, col(field).cast("integer"))
        
        # Special conversion for OneHousing handover_date_from (after mapping)
        if spider_name == "onehousing_api" and "handover_date_from" in df_source.columns:
            print(f"    • Converting handover_date_from to Timestamp")
            try:
                df_source = df_source.withColumn(
                    "handover_date_from",
                    when(
                        col("handover_date_from").isNotNull(),
                        # Handle both formats:
                        # 1. String: "2022-04-01"
                        # 2. Unix timestamp (ms): 1648771200000
                        when(
                            # Check if value is numeric (Unix timestamp in ms)
                            col("handover_date_from").cast("long").isNotNull() & (col("handover_date_from").cast("long") > 1000000000000),
                            # Convert Unix timestamp (ms) to timestamp
                            to_date(
                                from_unixtime(col("handover_date_from").cast("long") / 1000)
                            ).cast("timestamp")
                        )
                        # Otherwise parse as ISO 8601 string
                        .otherwise(
                            to_timestamp(col("handover_date_from"), "yyyy-MM-dd")
                        )
                    ).otherwise(lit(None).cast("timestamp"))
                )
            except Exception as e:
                print(f"      ⚠️ Failed to convert handover_date_from: {e}")
        
        # Add missing columns with defaults
        for field, default in DEFAULT_VALUES.items():
            if field not in df_source.columns:
                df_source = df_source.withColumn(field, lit(default))
        
        # Preserve Bronze timestamp column for later use (will be converted to ingested_at_utc)
        bronze_timestamp_col = None
        if "timestamp" in df_source.columns:
            bronze_timestamp_col = col("timestamp")
        
        # Cast all columns to match SILVER_SCHEMA types to avoid union conflicts
        for schema_field in SILVER_SCHEMA.fields:
            field_name = schema_field.name
            if field_name in df_source.columns:
                target_type = schema_field.dataType
                
                # Special handling for ArrayType fields
                if isinstance(target_type, ArrayType):
                    # Check current column type
                    current_type = df_source.schema[field_name].dataType
                    
                    # If it's already an array, try to cast
                    if isinstance(current_type, ArrayType):
                        # Check if target is ARRAY<STRING> but source is ARRAY<STRUCT>
                        if (isinstance(target_type.elementType, StringType) and 
                            hasattr(current_type.elementType, 'fields')):
                            # Extract appropriate field from array of structs
                            # Try 'name' field first, then 'value', then first string field
                            struct_fields = [f.name for f in current_type.elementType.fields]
                            extract_field = None
                            
                            if 'name' in struct_fields:
                                extract_field = 'name'
                            elif 'value' in struct_fields:
                                extract_field = 'value'
                            elif 'key' in struct_fields:
                                extract_field = 'key'
                            else:
                                # Use first string field
                                for f in current_type.elementType.fields:
                                    if isinstance(f.dataType, StringType):
                                        extract_field = f.name
                                        break
                            
                            if extract_field:
                                try:
                                    df_source = df_source.withColumn(
                                        field_name,
                                        when(
                                            col(field_name).isNotNull(),
                                            expr(f"transform({field_name}, x -> x.{extract_field})")
                                        ).otherwise(lit(None).cast(target_type))
                                    )
                                except:
                                    # If transform fails, set to null
                                    df_source = df_source.withColumn(field_name, lit(None).cast(target_type))
                            else:
                                # No suitable field found, set to null
                                df_source = df_source.withColumn(field_name, lit(None).cast(target_type))
                        else:
                            # Simple array cast
                            try:
                                df_source = df_source.withColumn(
                                    field_name, 
                                    col(field_name).cast(target_type)
                                )
                            except:
                                df_source = df_source.withColumn(field_name, lit(None).cast(target_type))
                    # If it's a string, try to parse as JSON array or wrap in array
                    elif isinstance(current_type, StringType):
                        # Try to parse as JSON array first, if fails wrap as single element
                        df_source = df_source.withColumn(
                            field_name,
                            when(
                                col(field_name).isNotNull() & (col(field_name) != ""),
                                when(
                                    col(field_name).startswith("["),
                                    # Try to parse as JSON array
                                    from_json(col(field_name), target_type)
                                ).otherwise(
                                    # Wrap single string in array
                                    array(col(field_name))
                                )
                            ).otherwise(lit(None).cast(target_type))
                        )
                    else:
                        # For other types, set to null
                        df_source = df_source.withColumn(field_name, lit(None).cast(target_type))
                else:
                    # For non-array types, simple cast
                    try:
                        df_source = df_source.withColumn(
                            field_name, 
                            col(field_name).cast(target_type)
                        )
                    except:
                        # If cast fails, set to null
                        df_source = df_source.withColumn(field_name, lit(None).cast(target_type))
        
        # Select only columns defined in SILVER_SCHEMA to avoid extra columns
        # But keep Bronze timestamp for later conversion
        schema_columns = [field.name for field in SILVER_SCHEMA.fields]
        available_columns = [col_name for col_name in schema_columns if col_name in df_source.columns]
        
        # Add timestamp back if it exists (needed for downstream processing)
        if bronze_timestamp_col is not None and "timestamp" not in available_columns:
            df_source = df_source.select(*available_columns, bronze_timestamp_col.alias("timestamp"))
        else:
            df_source = df_source.select(*available_columns)
        
        df_mapped_list.append(df_source)
        print(f"  ✅ Mapped {spider_name}: {df_source.count():,} records")
    
    # Union all sources
    if df_mapped_list:
        df_unified = df_mapped_list[0]
        for df_other in df_mapped_list[1:]:
            df_unified = df_unified.unionByName(df_other, allowMissingColumns=True)
        
        # Convert Bronze timestamp to ingested_at_utc immediately after union
        if "timestamp" in df_unified.columns and "ingested_at_utc" not in df_unified.columns:
            print("  🕐 Converting Bronze timestamp to ingested_at_utc...")
            df_unified = df_unified.withColumn(
                "ingested_at_utc",
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
            ).drop("timestamp")  # Drop Bronze timestamp after conversion
        
        print(f"✅ Total mapped records: {df_unified.count():,}")
        return df_unified
    
    return df

# ===========================
# STEP 3: DATA VALIDATION
# ===========================
def validate_data_with_rules(df):
    """Enhanced validation with configurable rules for real estate data"""
    print(f"\n{'='*70}")
    print(f"🔍 STEP 3: DATA VALIDATION")
    print(f"{'='*70}")
    
    # Critical field validation
    validation_expr = (
        col("spider_name").isNotNull() &
        col("ingested_at_utc").isNotNull() &
        col("source_id").isNotNull() &
        col("project_name").isNotNull() &
        (length(col("project_name")) > 0)
    )
    
    # Coordinate validation (warn only, don't reject)
    df = df.withColumn(
        "_has_valid_coords",
        when(
            (col("latitude").isNotNull()) & 
            (col("longitude").isNotNull()) &
            (col("latitude") != 0) &
            (col("longitude") != 0) &
            (col("latitude").between(-90, 90)) &
            (col("longitude").between(-180, 180)),
            lit(True)
        ).otherwise(lit(False))
    )
    
    # Price validation for valuation model
    df = df.withColumn(
        "_has_valid_price",
        when(
            (col("min_selling_price").isNotNull()) |
            (col("max_selling_price").isNotNull()) |
            (col("min_unit_price").isNotNull()) |
            (col("max_unit_price").isNotNull()),
            lit(True)
        ).otherwise(lit(False))
    )
    
    df_valid = df.filter(validation_expr)
    df_invalid = df.filter(~validation_expr)
    
    valid_count = df_valid.count()
    invalid_count = df_invalid.count()
    total_count = valid_count + invalid_count
    
    invalid_pct = (invalid_count / total_count * 100) if total_count > 0 else 0
    
    # Count records with coordinates and prices
    coords_count = df_valid.filter(col("_has_valid_coords") == True).count()
    price_count = df_valid.filter(col("_has_valid_price") == True).count()
    
    print(f"  ✅ Valid records: {valid_count:,} ({100-invalid_pct:.2f}%)")
    print(f"  ❌ Invalid records: {invalid_count:,} ({invalid_pct:.2f}%)")
    print(f"  📍 Records with coordinates: {coords_count:,} ({coords_count/valid_count*100:.1f}%)")
    print(f"  💰 Records with pricing: {price_count:,} ({price_count/valid_count*100:.1f}%)")
    
    # Check quality threshold
    if invalid_pct > QUALITY_THRESHOLDS["max_invalid_percentage"] * 100:
        print(f"  ⚠️  WARNING: Invalid percentage ({invalid_pct:.2f}%) exceeds threshold!")
    
    return df_valid, df_invalid

# ===========================
# STEP 4: DATA QUALITY CHECKS
# ===========================
def run_data_quality_checks(df):
    """Run comprehensive data quality checks"""
    print(f"\n{'='*70}")
    print(f"📊 STEP 4: DATA QUALITY CHECKS")
    print(f"{'='*70}")
    
    checker = DataQualityChecker(df)
    
    # Run checks
    required_fields = ["spider_name", "source_id", "ingested_at_utc"]
    checker.check_null_percentage(required_fields, threshold=0.3)
    checker.check_completeness(required_fields)
    
    if "universal_id" in df.columns:
        checker.check_uniqueness(["universal_id"])
    
    # Check format validations
    if "email" in df.columns:
        checker.check_format("email", REAL_ESTATE_QUALITY_CHECKS["email_pattern"])
    
    if "phone" in df.columns:
        checker.check_format("phone", REAL_ESTATE_QUALITY_CHECKS["phone_pattern"])
    
    # Get failed checks
    failed_checks = checker.get_failed_checks()
    
    if failed_checks:
        print(f"  ⚠️  Failed checks: {', '.join(failed_checks)}")
        print("\n" + checker.generate_report())
        return False
    else:
        print("  ✅ All quality checks passed!")
        return True

# ===========================
# STEP 5: DATA CLEANSING
# ===========================
def cleanse_data(df):
    """Comprehensive data cleansing"""
    print(f"\n{'='*70}")
    print(f"🧹 STEP 5: DATA CLEANSING")
    print(f"{'='*70}")
    
    original_count = df.count()
    
    # 5.1: Remove Duplicates
    print("  📌 Removing duplicates...")
    df_with_key = df.withColumn(
        "record_key",
        coalesce(col("source_id"), lit("UNKNOWN"))
    )
    
    window_spec = Window.partitionBy("spider_name", "record_key") \
                        .orderBy(col("ingested_at_utc").desc())
    
    df_dedup = df_with_key \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    dedup_count = df_dedup.count()
    duplicates_removed = original_count - dedup_count
    dup_pct = (duplicates_removed / original_count * 100) if original_count > 0 else 0
    
    print(f"  ✅ Removed {duplicates_removed:,} duplicates ({dup_pct:.2f}%)")
    
    # 5.2: Handle Missing Values
    print("  📌 Handling missing values...")
    for field, default in DEFAULT_VALUES.items():
        if field in df_dedup.columns:
            df_dedup = df_dedup.fillna({field: default})
    
    # 5.3: Remove Outliers (for pricing and area fields)
    print("  📌 Removing outliers for valuation fields...")
    numeric_cols = ["min_selling_price", "max_selling_price", "min_unit_price", 
                    "max_unit_price", "total_area", "construction_area"]
    
    for col_name in numeric_cols:
        if col_name in df_dedup.columns:
            # Only process non-null values
            non_null_df = df_dedup.filter(col(col_name).isNotNull())
            if non_null_df.count() > 0:
                stats = non_null_df.select(
                    mean(col(col_name)).alias("mean_val"),
                    stddev(col(col_name)).alias("std_val")
                ).collect()[0]
                
                if stats.std_val and stats.std_val > 0:
                    before_count = df_dedup.count()
                    # Use 4 sigma for real estate (more lenient due to high variance)
                    df_dedup = df_dedup.filter(
                        col(col_name).isNull() |
                        (spark_abs((col(col_name) - stats.mean_val) / stats.std_val) < 4)
                    )
                    after_count = df_dedup.count()
                    outliers_removed = before_count - after_count
                    if outliers_removed > 0:
                        print(f"    • {col_name}: removed {outliers_removed:,} outliers")
    
    final_count = df_dedup.count()
    print(f"  ✅ Cleansing complete: {original_count:,} → {final_count:,} records")
    
    return df_dedup, duplicates_removed

# ===========================
# STEP 6: STANDARDIZATION
# ===========================
def standardize_data(df):
    """Apply all standardization transformations"""
    print(f"\n{'='*70}")
    print(f"🎯 STEP 6: DATA STANDARDIZATION")
    print(f"{'='*70}")
    

    transformer = DataTransformer()
    
    # Standardize phone numbers
    if "phone" in df.columns:
        print("  📱 Standardizing phone numbers...")
        df = transformer.standardize_phone_numbers(df, "phone")
    
    # Standardize emails
    if "email" in df.columns:
        print("  📧 Standardizing emails...")
        df = transformer.standardize_emails(df, "email")
    
    # Clean HTML tags from description (for Chotot & Meeyland)
    if "description" in df.columns:
        print("  🧹 Cleaning HTML tags from description...")
        df = transformer.clean_html_tags(df, ["description"])
    
    # Normalize text fields
    text_cols = [c for c in ["project_name", "address", "description"] if c in df.columns]
    if text_cols:
        print(f"  📝 Normalizing text fields: {', '.join(text_cols)}")
        df = transformer.normalize_vietnamese_text(df, text_cols)
    
    # Standardize city names
    if "city" in df.columns:
        print("  🏙️  Standardizing city names...")
        df = transformer.standardize_city_names(df, "city")
    
    # Timestamp already converted in apply_schema_mapping step
    # Just ensure ingested_at_utc exists
    if "ingested_at_utc" not in df.columns:
        print("  ⚠️  Warning: ingested_at_utc not found, using current_timestamp")
        df = df.withColumn("ingested_at_utc", to_date(current_timestamp()).cast("string"))
    
    # Create partition columns
    df = df \
        .withColumn("ingestion_year", date_format(col("ingested_at_utc"), "yyyy")) \
        .withColumn("ingestion_month", date_format(col("ingested_at_utc"), "MM")) \
        .withColumn("ingestion_date", to_date(col("ingested_at_utc")))
    
    print("  ✅ Standardization complete")
    return df

# ===========================
# STEP 7: ENRICHMENT
# ===========================
def enrich_data(df):
    """Enrich data with derived fields for real estate valuation"""
    print(f"\n{'='*70}")
    print(f"✨ STEP 7: DATA ENRICHMENT FOR VALUATION")
    print(f"{'='*70}")
    
    # Create universal ID
    print("  🔑 Creating universal IDs...")
    df = df.withColumn(
        "universal_id",
        sha2(concat_ws("_", col("spider_name"), col("record_key")), 256)
    )
    
    # Calculate average prices for valuation
    print("  💰 Calculating average pricing metrics...")
    
    # Average selling price (only if columns exist)
    if "min_selling_price" in df.columns or "max_selling_price" in df.columns:
        df = df.withColumn(
            "avg_selling_price",
            when(
                col("min_selling_price").isNotNull() & col("max_selling_price").isNotNull(),
                (col("min_selling_price") + col("max_selling_price")) / 2
            ).when(
                col("min_selling_price").isNotNull(),
                col("min_selling_price")
            ).when(
                col("max_selling_price").isNotNull(),
                col("max_selling_price")
            ).otherwise(lit(None))
        )
    else:
        df = df.withColumn("avg_selling_price", lit(None))
    
    # Average unit price (only if columns exist)
    if "min_unit_price" in df.columns or "max_unit_price" in df.columns:
        df = df.withColumn(
            "avg_unit_price",
            when(
                col("min_unit_price").isNotNull() & col("max_unit_price").isNotNull(),
                (col("min_unit_price") + col("max_unit_price")) / 2
            ).when(
                col("min_unit_price").isNotNull(),
                col("min_unit_price")
            ).when(
                col("max_unit_price").isNotNull(),
                col("max_unit_price")
            ).otherwise(lit(None))
        )
    else:
        df = df.withColumn("avg_unit_price", lit(None))
    
    # Calculate price range (only if columns exist)
    if "min_selling_price" in df.columns and "max_selling_price" in df.columns:
        df = df.withColumn(
            "price_range",
            when(
                col("min_selling_price").isNotNull() & col("max_selling_price").isNotNull(),
                col("max_selling_price") - col("min_selling_price")
            ).otherwise(lit(None))
        )
    else:
        df = df.withColumn("price_range", lit(None))
    
    # Calculate area range (only if columns exist)
    if "min_area" in df.columns and "max_area" in df.columns:
        df = df.withColumn(
            "area_range",
            when(
                col("min_area").isNotNull() & col("max_area").isNotNull(),
                col("max_area") - col("min_area")
            ).otherwise(lit(None))
        )
    else:
        df = df.withColumn("area_range", lit(None))
    
    # Create location quality score based on infrastructure grades
    print("  📍 Calculating location quality score...")
    
    # Check if grade columns exist
    has_grades = any(col_name in df.columns for col_name in ["trans_grade", "infra_grade", "school_grade"])
    
    if has_grades:
        grade_conditions = []
        if "trans_grade" in df.columns:
            grade_conditions.append(when(col("trans_grade").isNotNull(), 1).otherwise(0))
        if "infra_grade" in df.columns:
            grade_conditions.append(when(col("infra_grade").isNotNull(), 1).otherwise(0))
        if "school_grade" in df.columns:
            grade_conditions.append(when(col("school_grade").isNotNull(), 1).otherwise(0))
        
        if grade_conditions:
            score_expr = sum(grade_conditions) / float(len(grade_conditions))
            df = df.withColumn("location_quality_score", score_expr)
        else:
            df = df.withColumn("location_quality_score", lit(0.0))
    else:
        df = df.withColumn("location_quality_score", lit(0.0))
    
    # Calculate data completeness score for valuation features
    print("  📊 Calculating completeness scores...")
    valuation_fields = [
        "project_name", "address", "latitude", "longitude",
        "avg_selling_price", "avg_unit_price", "total_area",
        "district", "city", "project_type"
    ]
    existing_fields = [f for f in valuation_fields if f in df.columns]
    
    if existing_fields:
        score_expr = sum([
            when(col(f).isNotNull() & (col(f) != "") & (col(f) != 0), 1).otherwise(0)
            for f in existing_fields
        ]) / len(existing_fields)
        
        df = df.withColumn("data_completeness_score", score_expr)
    else:
        df = df.withColumn("data_completeness_score", lit(0.0))
    
    # Add audit columns
    print("  📝 Adding audit columns...")
    df = df \
        .withColumn("silver_processed_at", current_timestamp()) \
        .withColumn("silver_version", lit("2.0")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("valid_from", current_timestamp()) \
        .withColumn("valid_to", lit(None).cast("timestamp"))
    
    # Extract features from description if available
    if "description" in df.columns:
        print("  🏗️  Extracting project features...")
        transformer = DataTransformer()
        df = transformer.extract_project_features(df, "description")
    
    # Convert all timestamp fields to StringType (date format) - FINAL STEP
    print("  📅 Converting all timestamp fields to date format...")
    timestamp_fields = [
        "ingested_at_utc", "silver_processed_at", "valid_from", "valid_to",
        "handover_date_from", "handover_date", 
        "construction_start_date", "construction_end_date"
    ]
    
    for field in timestamp_fields:
        if field in df.columns:
            df = df.withColumn(
                field,
                when(
                    col(field).isNotNull(),
                    to_date(col(field)).cast("string")
                ).otherwise(lit(None))
            )
    
    print("  ✅ Enrichment complete")
    return df

# ===========================
# STEP 8: SCD TYPE 2 & WRITE
# ===========================
def apply_scd_and_write(spark: SparkSession, df_new):
    """Apply SCD Type 2 and write to Silver Layer"""
    print(f"\n{'='*70}")
    print(f"💾 STEP 8: SCD TYPE 2 & WRITE TO SILVER")
    print(f"{'='*70}")
    
    try:
        silver_table = DeltaTable.forPath(spark, SILVER_PROJECT_PATH)
        print("  📂 Existing Silver table found - applying SCD Type 2...")
        
        # Mark changed records as no longer current
        silver_table.alias("target").merge(
            df_new.alias("source"),
            """
            target.universal_id = source.universal_id 
            AND target.is_current = true
            """
        ).whenMatchedUpdate(
            condition="""
            target.project_name != source.project_name OR
            target.address != source.address OR
            target.phone != source.phone OR
            target.email != source.email
            """,
            set={
                "is_current": "false",
                "valid_to": "current_timestamp()"
            }
        ).execute()
        
        # Append new/changed records
        df_new.write \
            .format("delta") \
            .mode("append") \
            .save(SILVER_PROJECT_PATH)
        
        print("  ✅ SCD Type 2 merge completed")
        
    except Exception as e:
        print(f"  ℹ️  Creating new Silver table (first load)...")
        df_new.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("spider_name", "ingestion_year", "ingestion_month") \
            .save(SILVER_PROJECT_PATH)
        print("  ✅ Silver table created")
    
    record_count = df_new.count()
    print(f"  📊 Records written: {record_count:,}")
    return record_count

# ===========================
# STEP 9: OPTIMIZE
# ===========================
def optimize_silver_table(spark: SparkSession):
    """Optimize Delta table"""
    print(f"\n{'='*70}")
    print(f"⚡ STEP 9: OPTIMIZE TABLE")
    print(f"{'='*70}")
    
    try:
        print("  🔄 Running OPTIMIZE with Z-ORDER...")
        spark.sql(f"""
            OPTIMIZE delta.`{SILVER_PROJECT_PATH}`
            ZORDER BY (universal_id, spider_name, ingestion_date)
        """)
        print("  ✅ Optimization completed")
        
        print("  🗑️  Running VACUUM (retain 7 days)...")
        spark.sql(f"""
            VACUUM delta.`{SILVER_PROJECT_PATH}` RETAIN 168 HOURS
        """)
        print("  ✅ Vacuum completed")
        
    except Exception as e:
        print(f"  ⚠️  Optimization warning: {e}")

# ===========================
# STEP 10: QUARANTINE & METADATA
# ===========================
def write_quarantine_and_metadata(spark: SparkSession, df_invalid, start_date, stats):
    """Write quarantine data and log metadata"""
    print(f"\n{'='*70}")
    print(f"📋 STEP 10: QUARANTINE & METADATA")
    print(f"{'='*70}")
    
    # Quarantine
    if df_invalid and df_invalid.count() > 0:
        invalid_count = df_invalid.count()
        print(f"  🚨 Writing {invalid_count:,} invalid records to quarantine...")
        
        df_invalid_with_meta = df_invalid \
            .withColumn("quarantine_timestamp", current_timestamp()) \
            .withColumn("quarantine_reason", lit("Failed validation rules"))
        
        df_invalid_with_meta.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("spider_name") \
            .save(QUARANTINE_PATH)
        
        print("  ✅ Quarantine write completed")
    
    # Metadata
    print("  📊 Logging pipeline metadata...")
    metadata_record = spark.createDataFrame([{
        "pipeline_run_id": os.environ.get("AIRFLOW_RUN_ID", start_date),
        "execution_date": start_date,
        "source_layer": "bronze",
        "target_layer": "silver",
        "spiders": ",".join(PROJECT_SPIDERS),
        "records_read": stats.get("records_read", 0),
        "records_valid": stats.get("records_valid", 0),
        "records_invalid": stats.get("records_invalid", 0),
        "records_written": stats.get("records_written", 0),
        "duplicates_removed": stats.get("duplicates_removed", 0),
        "avg_completeness_score": stats.get("avg_completeness_score", 0.0),
        "processing_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "SUCCESS"
    }])
    
    metadata_record.write \
        .format("delta") \
        .mode("append") \
        .save(METADATA_PATH)
    
    print("  ✅ Metadata logged")
    
    # Print summary
    print(f"\n{'='*70}")
    print("📊 PIPELINE EXECUTION SUMMARY")
    print(f"{'='*70}")
    print(f"  Records Read:          {stats.get('records_read', 0):,}")
    print(f"  Valid Records:         {stats.get('records_valid', 0):,}")
    print(f"  Invalid Records:       {stats.get('records_invalid', 0):,}")
    print(f"  Duplicates Removed:    {stats.get('duplicates_removed', 0):,}")
    print(f"  Written to Silver:     {stats.get('records_written', 0):,}")
    print(f"  Avg Completeness:      {stats.get('avg_completeness_score', 0.0):.2%}")
    print(f"{'='*70}")

# ===========================
# MAIN PIPELINE
# ===========================
def run_silver_project_etl(spark: SparkSession, start_date: str):
    """Execute complete Silver ETL pipeline"""
    print("\n" + "="*70)
    print("🚀 SILVER LAYER ETL PIPELINE - PRODUCTION VERSION")
    print("="*70)
    print(f"Execution Date: {start_date}")
    print("="*70)
    
    stats = {}
    
    try:
        # Step 1: Ingestion
        df_bronze = read_bronze_data(spark, start_date)
        if df_bronze is None or df_bronze.rdd.isEmpty():
            print("\n⚠️  No data to process. Exiting.")
            return

        stats["records_read"] = df_bronze.count()
        
        # Step 2: Schema Mapping & Transformation
        df_mapped = apply_schema_mapping(df_bronze)
        
        # Step 3: Validation
        df_valid, df_invalid = validate_data_with_rules(df_mapped)
        stats["records_valid"] = df_valid.count()
        stats["records_invalid"] = df_invalid.count()
        
        # Step 4: Data Quality Checks
        quality_passed = run_data_quality_checks(df_valid)
        
        # Step 5: Cleansing
        df_cleaned, duplicates_removed = cleanse_data(df_valid)
        stats["duplicates_removed"] = duplicates_removed
        
        # Step 6: Standardization
        df_standardized = standardize_data(df_cleaned)
        
        # Step 7: Enrichment
        df_enriched = enrich_data(df_standardized)
        
        # Calculate average completeness score
        if "data_completeness_score" in df_enriched.columns:
            avg_score = df_enriched.agg({"data_completeness_score": "avg"}).collect()[0][0]
            stats["avg_completeness_score"] = avg_score if avg_score else 0.0
        
        # Step 8: SCD & Write
        records_written = apply_scd_and_write(spark, df_enriched)
        stats["records_written"] = records_written
        
        # Step 9: Optimize
        optimize_silver_table(spark)
        
        # Step 10: Quarantine & Metadata
        write_quarantine_and_metadata(spark, df_invalid, start_date, stats)
        
        print("\n" + "="*70)
        print("✅ SILVER LAYER ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"❌ PIPELINE FAILED")
        print(f"{'='*70}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        stats["status"] = "FAILED"
        stats["error_message"] = str(e)
        raise

# ===========================
# ENTRY POINT
# ===========================
if __name__ == "__main__":
    airflow_exec_date = os.environ.get("AIRFLOW_EXECUTION_DATE", datetime.now().strftime("%Y-%m-%d"))
    
    try:
        # Ensure MinIO buckets exist before starting Spark
        print("🪣 Checking MinIO buckets...")
        ensure_minio_buckets()
        
        spark_session = initialize_spark_session()
        run_silver_project_etl(spark_session, airflow_exec_date)
        spark_session.stop()
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        sys.exit(1)