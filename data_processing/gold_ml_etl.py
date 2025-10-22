"""
Gold Layer ETL for ML Features
Phase 1: Foundation - Basic features for OneHousing data
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, log, expr, count, avg, sum as spark_sum,
    year, quarter, month, current_timestamp, broadcast, coalesce,
    size, array_contains, lower, trim, regexp_replace
)
from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable

# Import schemas and enhancement functions
from gold_ml_schema import (
    ML_FEATURES_SCHEMA, CITY_ENCODING, STATUS_ENCODING, 
    GRADE_ENCODING, QUALITY_TIER_CRITERIA
)
from data_enhancement import enhance_silver_data

# ===========================
# CONFIGURATION
# ===========================

SILVER_PROJECT_PATH = os.environ.get(
    "SILVER_PROJECT_PATH",
    "s3a://real-estate-silver/silver/projects"
)

GOLD_ML_FEATURES_PATH = os.environ.get(
    "GOLD_ML_FEATURES_PATH",
    "s3a://real-estate-gold/marts/ml_features/current"
)

GOLD_VERSION = "1.0"

# ===========================
# STEP 1: READ SILVER DATA
# ===========================

def read_silver_data(spark):
    """Read data from Silver layer"""
    print("\n" + "="*70)
    print("📖 STEP 1: READING SILVER DATA")
    print("="*70)
    
    try:
        df = spark.read.format("delta").load(SILVER_PROJECT_PATH)
        
        # Filter only current records
        df = df.filter(col("is_current") == True)
        
        total_count = df.count()
        print(f"  ✅ Read {total_count:,} records from Silver layer")
        
        # Show distribution by source
        print("\n  📊 Distribution by source:")
        df.groupBy("spider_name").count().orderBy("count", ascending=False).show()
        
        return df
        
    except Exception as e:
        print(f"  ❌ Error reading Silver data: {e}")
        raise


# ===========================
# STEP 2: DATA ENHANCEMENT
# ===========================

def apply_data_enhancement(df):
    """Apply price imputation and geocoding"""
    print("\n" + "="*70)
    print("🔧 STEP 2: DATA ENHANCEMENT")
    print("="*70)
    
    df = enhance_silver_data(df)
    
    # Show improvement stats
    print("\n  📊 Enhancement Results:")
    df.groupBy("price_imputed", "coordinates_imputed").count().show()
    
    return df


# ===========================
# STEP 3: FEATURE ENGINEERING
# ===========================

def engineer_features(df):
    """Engineer ML features from Silver data"""
    print("\n" + "="*70)
    print("⚙️  STEP 3: FEATURE ENGINEERING")
    print("="*70)
    
    # 3.1: Target variables
    print("  🎯 Creating target variables...")
    df = create_target_variables(df)
    
    # 3.2: Location features
    print("  📍 Engineering location features...")
    df = engineer_location_features(df)
    
    # 3.3: Property features
    print("  🏢 Engineering property features...")
    df = engineer_property_features(df)
    
    # 3.4: Developer features
    print("  👔 Engineering developer features...")
    df = engineer_developer_features(df)
    
    # 3.5: Amenity features
    print("  🏊 Engineering amenity features...")
    df = engineer_amenity_features(df)
    
    # 3.6: Market features
    print("  📈 Engineering market features...")
    df = engineer_market_features(df)
    
    print("  ✅ Feature engineering completed")
    
    return df


def create_target_variables(df):
    """Create target variables for ML"""
    # Note: data_enhancement.py already overwrites avg_unit_price with imputed values
    # So we can use the fields directly
    df = df.withColumn(
        "target_price_per_sqm",
        col("avg_unit_price")
    )
    
    df = df.withColumn(
        "target_total_price",
        col("avg_selling_price")
    )
    
    df = df.withColumn(
        "target_min_price",
        col("min_selling_price")
    )
    
    df = df.withColumn(
        "target_max_price",
        col("max_selling_price")
    )
    
    df = df.withColumn(
        "target_price_range",
        when(
            (col("max_selling_price").isNotNull()) & 
            (col("min_selling_price").isNotNull()),
            col("max_selling_price") - col("min_selling_price")
        ).otherwise(lit(None))
    )
    
    return df


def engineer_location_features(df):
    """Engineer location-based features"""
    
    # Encode city
    city_mapping = broadcast(
        spark.createDataFrame(
            [(k, v) for k, v in CITY_ENCODING.items()],
            ["city_name", "city_code"]
        )
    )
    
    # Normalize city names
    df = df.withColumn(
        "city_normalized",
        trim(regexp_replace(
            regexp_replace(col("city"), "TP\\. ", ""),
            "Thành phố ", ""
        ))
    )
    
    df = df.join(
        city_mapping,
        df.city_normalized == city_mapping.city_name,
        "left"
    ).withColumn(
        "city_encoded",
        coalesce(col("city_code"), lit(0))
    ).drop("city_name", "city_code", "city_normalized")
    
    # Encode district (simple hash-based encoding for now)
    df = df.withColumn(
        "district_encoded",
        when(col("district").isNotNull(),
             expr("abs(hash(district)) % 1000")
        ).otherwise(lit(0))
    )
    
    return df


def engineer_property_features(df):
    """Engineer property-based features"""
    
    # Log transforms for skewed distributions
    df = df.withColumn(
        "log_total_area",
        when(col("total_area") > 0, 
             log(col("total_area"))
        ).otherwise(lit(None))
    )
    
    df = df.withColumn(
        "log_total_property",
        when(col("total_property") > 0,
             log(col("total_property"))
        ).otherwise(lit(None))
    )
    
    # Floor Area Ratio (FAR)
    df = df.withColumn(
        "floor_area_ratio",
        when(
            (col("construction_area").isNotNull()) & 
            (col("total_area").isNotNull()) &
            (col("total_area") > 0),
            col("construction_area") / col("total_area")
        ).otherwise(lit(None))
    )
    
    # Average property per floor
    df = df.withColumn(
        "avg_property_per_floor",
        when(
            (col("total_property").isNotNull()) & 
            (col("number_of_floors").isNotNull()) &
            (col("number_of_floors") > 0),
            col("total_property") / col("number_of_floors")
        ).otherwise(
            (col("min_prop_per_floor") + col("max_prop_per_floor")) / 2
        )
    )
    
    # Average area per unit
    df = df.withColumn(
        "avg_area_per_unit",
        when(
            (col("total_area").isNotNull()) & 
            (col("total_property").isNotNull()) &
            (col("total_property") > 0),
            col("total_area") / col("total_property")
        ).otherwise(lit(None))
    )
    
    # Average bedroom
    df = df.withColumn(
        "avg_bedroom",
        when(
            (col("min_bedroom").isNotNull()) & 
            (col("max_bedroom").isNotNull()),
            (col("min_bedroom") + col("max_bedroom")) / 2
        ).otherwise(lit(None))
    )
    
    return df


def engineer_developer_features(df):
    """Engineer developer-based features"""
    
    # Simple hash-based encoding for developers
    df = df.withColumn(
        "developer_encoded",
        when(
            col("developer_name").isNotNull(),
            expr("abs(hash(developer_name)) % 500")
        ).when(
            col("investor_name").isNotNull(),
            expr("abs(hash(investor_name)) % 500")
        ).otherwise(lit(0))
    )
    
    return df


def engineer_amenity_features(df):
    """Engineer amenity-based features"""
    
    # Count amenities
    df = df.withColumn(
        "amenity_count",
        (
            col("has_swimming_pool").cast("int") +
            col("has_gym").cast("int") +
            col("has_parking").cast("int") +
            col("has_garden").cast("int") +
            col("has_security").cast("int") +
            col("has_playground").cast("int")
        )
    )
    
    # Amenity score (0-1)
    df = df.withColumn(
        "amenity_score",
        col("amenity_count") / 6.0
    )
    
    return df


def engineer_market_features(df):
    """Engineer market/time-based features"""
    
    df = df.withColumn("year", year(col("ingestion_date")))
    df = df.withColumn("quarter", quarter(col("ingestion_date")))
    df = df.withColumn("month", month(col("ingestion_date")))
    
    return df


# ===========================
# STEP 4: QUALITY FILTERING
# ===========================

def assign_quality_tiers(df):
    """Assign quality tiers for ML training"""
    print("\n" + "="*70)
    print("🎯 STEP 4: QUALITY TIER ASSIGNMENT")
    print("="*70)
    
    df = df.withColumn(
        "quality_tier",
        when(
            # HIGH QUALITY: Ready for training
            (col("target_price_per_sqm").isNotNull()) &
            (col("target_price_per_sqm") > 0) &
            (col("latitude").isNotNull()) &
            (col("longitude").isNotNull()) &
            (col("total_area") > 0) &
            (col("data_completeness_score") >= 0.6),
            lit("high")
        ).when(
            # MEDIUM QUALITY: Use with caution
            (col("target_price_per_sqm").isNotNull()) &
            (col("target_price_per_sqm") > 0) &
            (col("data_completeness_score") >= 0.4),
            lit("medium")
        ).otherwise(
            # LOW QUALITY: Exclude from training
            lit("low")
        )
    )
    
    df = df.withColumn(
        "is_training_ready",
        col("quality_tier") == "high"
    )
    
    # Show distribution
    print("\n  📊 Quality Tier Distribution:")
    df.groupBy("quality_tier", "spider_name").count().orderBy("quality_tier", "count", ascending=False).show()
    
    # Show training-ready stats
    training_ready = df.filter(col("is_training_ready") == True).count()
    total = df.count()
    pct = (training_ready / total * 100) if total > 0 else 0
    
    print(f"\n  ✅ Training-ready records: {training_ready:,} / {total:,} ({pct:.1f}%)")
    
    return df


# ===========================
# STEP 5: SELECT FEATURES
# ===========================

def select_ml_features(df):
    """Select only ML-relevant features"""
    print("\n" + "="*70)
    print("📋 STEP 5: FEATURE SELECTION")
    print("="*70)
    
    # Rename universal_id to project_id
    df = df.withColumn("project_id", col("universal_id"))
    df = df.withColumn("snapshot_date", col("ingestion_date"))
    
    # Select features according to schema
    feature_columns = [
        # Identifiers
        "project_id", "source_id", "spider_name", "snapshot_date",
        
        # Basic info
        "project_name", "project_type", "status",
        
        # Target variables
        "target_price_per_sqm", "target_total_price",
        "target_min_price", "target_max_price", "target_price_range",
        
        # Location features
        "latitude", "longitude",
        "city", "district", "ward",
        "city_encoded", "district_encoded",
        "location_quality_score",
        
        # Property features
        "total_area", "log_total_area", "construction_area",
        "total_property", "log_total_property",
        "number_of_blocks", "number_of_floors", "total_floor",
        "number_of_basement", "number_of_elevators",
        "construction_density", "green_density", "floor_area_ratio",
        "avg_property_per_floor", "avg_area_per_unit",
        "min_bedroom", "max_bedroom", "avg_bedroom",
        
        # Developer features
        "developer_name", "investor_name", "developer_encoded",
        
        # Amenity features
        "has_swimming_pool", "has_gym", "has_parking",
        "has_garden", "has_security", "has_playground",
        "amenity_count", "amenity_score",
        "quality_indexes", "trans_grade", "infra_grade", "school_grade",
        
        # Market features
        "year", "quarter", "month",
        
        # Quality metadata
        "data_completeness_score", "quality_tier", "is_training_ready",
        "price_imputed", "coordinates_imputed"
    ]
    
    # Select only existing columns
    existing_columns = [c for c in feature_columns if c in df.columns]
    df = df.select(*existing_columns)
    
    # Add audit fields
    df = df.withColumn("gold_processed_at", current_timestamp().cast("string"))
    df = df.withColumn("gold_version", lit(GOLD_VERSION))
    
    print(f"  ✅ Selected {len(existing_columns)} features")
    
    return df


# ===========================
# STEP 6: WRITE TO GOLD
# ===========================

def write_to_gold(df):
    """Write ML features to Gold layer"""
    print("\n" + "="*70)
    print("💾 STEP 6: WRITING TO GOLD LAYER")
    print("="*70)
    
    try:
        # Write to Delta
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("quality_tier", "year", "month") \
            .option("overwriteSchema", "true") \
            .save(GOLD_ML_FEATURES_PATH)
        
        record_count = df.count()
        print(f"  ✅ Wrote {record_count:,} records to Gold layer")
        print(f"  📂 Path: {GOLD_ML_FEATURES_PATH}")
        
        return record_count
        
    except Exception as e:
        print(f"  ❌ Error writing to Gold: {e}")
        raise


# ===========================
# STEP 7: GENERATE STATISTICS
# ===========================

def generate_statistics(df):
    """Generate statistics for ML features"""
    print("\n" + "="*70)
    print("📊 STEP 7: GENERATING STATISTICS")
    print("="*70)
    
    # Overall stats
    total = df.count()
    high_quality = df.filter(col("quality_tier") == "high").count()
    medium_quality = df.filter(col("quality_tier") == "medium").count()
    low_quality = df.filter(col("quality_tier") == "low").count()
    
    print(f"\n  📈 Overall Statistics:")
    print(f"     Total records: {total:,}")
    print(f"     High quality: {high_quality:,} ({high_quality/total*100:.1f}%)")
    print(f"     Medium quality: {medium_quality:,} ({medium_quality/total*100:.1f}%)")
    print(f"     Low quality: {low_quality:,} ({low_quality/total*100:.1f}%)")
    
    # Price statistics (training set only)
    print(f"\n  💰 Price Statistics (High Quality):")
    df.filter(col("quality_tier") == "high").select(
        "target_price_per_sqm"
    ).summary("count", "mean", "stddev", "min", "max").show()
    
    # Feature completeness
    print(f"\n  📋 Feature Completeness (High Quality):")
    high_quality_df = df.filter(col("quality_tier") == "high")
    
    key_features = [
        "latitude", "longitude", "total_area", "total_property",
        "number_of_floors", "construction_density", "developer_name"
    ]
    
    for feature in key_features:
        if feature in df.columns:
            non_null = high_quality_df.filter(col(feature).isNotNull()).count()
            pct = (non_null / high_quality * 100) if high_quality > 0 else 0
            print(f"     {feature}: {non_null:,} / {high_quality:,} ({pct:.1f}%)")
    
    # Imputation stats
    print(f"\n  🔧 Enhancement Statistics:")
    df.groupBy("price_imputed", "coordinates_imputed").count().show()
    
    return {
        "total": total,
        "high_quality": high_quality,
        "medium_quality": medium_quality,
        "low_quality": low_quality
    }


# ===========================
# MAIN ETL FUNCTION
# ===========================

def run_gold_ml_etl(spark):
    """Main ETL function"""
    print("\n" + "="*70)
    print("🚀 GOLD LAYER ML FEATURES ETL - PHASE 1")
    print("="*70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    stats = {
        "start_time": datetime.now(),
        "status": "RUNNING"
    }
    
    try:
        # Step 1: Read Silver data
        df = read_silver_data(spark)
        
        # Step 2: Data enhancement
        df = apply_data_enhancement(df)
        
        # Step 3: Feature engineering
        df = engineer_features(df)
        
        # Step 4: Quality filtering
        df = assign_quality_tiers(df)
        
        # Step 5: Select features
        df = select_ml_features(df)
        
        # Step 6: Write to Gold
        record_count = write_to_gold(df)
        
        # Step 7: Generate statistics
        etl_stats = generate_statistics(df)
        
        stats["status"] = "SUCCESS"
        stats["end_time"] = datetime.now()
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()
        stats["records_written"] = record_count
        stats.update(etl_stats)
        
        print("\n" + "="*70)
        print("✅ GOLD LAYER ETL COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Duration: {stats['duration']:.1f} seconds")
        print(f"Records written: {record_count:,}")
        print(f"Training-ready: {etl_stats['high_quality']:,}")
        
        return stats
        
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
    from pyspark_init import initialize_spark_session
    
    try:
        print("🪣 Initializing Spark session...")
        spark = initialize_spark_session()
        
        run_gold_ml_etl(spark)
        
        spark.stop()
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        sys.exit(1)
