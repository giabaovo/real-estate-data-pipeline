"""
Gold Layer Schema for ML Features
Phase 1: Foundation - Focus on OneHousing data with basic features
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, DateType, ArrayType
)

# ===========================
# ML FEATURES SCHEMA (Phase 1)
# ===========================

ML_FEATURES_SCHEMA = StructType([
    # ===== IDENTIFIERS =====
    StructField("project_id", StringType(), False),  # universal_id from Silver
    StructField("source_id", StringType(), True),
    StructField("spider_name", StringType(), True),
    StructField("snapshot_date", DateType(), True),
    
    # ===== BASIC INFO =====
    StructField("project_name", StringType(), True),
    StructField("project_type", StringType(), True),
    StructField("status", StringType(), True),
    
    # ===== TARGET VARIABLES (Price) =====
    StructField("target_price_per_sqm", DoubleType(), True),  # Main target
    StructField("target_total_price", DoubleType(), True),     # Secondary target
    StructField("target_min_price", DoubleType(), True),
    StructField("target_max_price", DoubleType(), True),
    StructField("target_price_range", DoubleType(), True),
    
    # ===== LOCATION FEATURES =====
    # Geographic coordinates
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    
    # Administrative divisions (encoded)
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("city_encoded", IntegerType(), True),
    StructField("district_encoded", IntegerType(), True),
    
    # Location quality
    StructField("location_quality_score", DoubleType(), True),
    
    # ===== PROPERTY FEATURES =====
    # Size & Scale
    StructField("total_area", DoubleType(), True),
    StructField("log_total_area", DoubleType(), True),  # Log transform
    StructField("construction_area", DoubleType(), True),
    StructField("total_property", IntegerType(), True),
    StructField("log_total_property", DoubleType(), True),  # Log transform
    
    # Building characteristics
    StructField("number_of_blocks", IntegerType(), True),
    StructField("number_of_floors", IntegerType(), True),
    StructField("total_floor", IntegerType(), True),
    StructField("number_of_basement", IntegerType(), True),
    StructField("number_of_elevators", IntegerType(), True),
    
    # Density ratios
    StructField("construction_density", DoubleType(), True),
    StructField("green_density", DoubleType(), True),
    StructField("floor_area_ratio", DoubleType(), True),  # Derived
    
    # Per-unit metrics
    StructField("avg_property_per_floor", DoubleType(), True),
    StructField("avg_area_per_unit", DoubleType(), True),
    
    # Unit characteristics
    StructField("min_bedroom", IntegerType(), True),
    StructField("max_bedroom", IntegerType(), True),
    StructField("avg_bedroom", DoubleType(), True),
    
    # ===== DEVELOPER FEATURES =====
    StructField("developer_name", StringType(), True),
    StructField("investor_name", StringType(), True),
    StructField("developer_encoded", IntegerType(), True),
    
    # ===== AMENITY FEATURES =====
    StructField("has_swimming_pool", BooleanType(), True),
    StructField("has_gym", BooleanType(), True),
    StructField("has_parking", BooleanType(), True),
    StructField("has_garden", BooleanType(), True),
    StructField("has_security", BooleanType(), True),
    StructField("has_playground", BooleanType(), True),
    StructField("amenity_count", IntegerType(), True),
    StructField("amenity_score", DoubleType(), True),
    
    # Quality indexes (OneHousing specific)
    StructField("quality_indexes", ArrayType(StringType()), True),
    StructField("trans_grade", StringType(), True),
    StructField("infra_grade", StringType(), True),
    StructField("school_grade", StringType(), True),
    
    # ===== MARKET FEATURES =====
    StructField("year", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    
    # ===== DATA QUALITY METADATA =====
    StructField("data_completeness_score", DoubleType(), True),
    StructField("quality_tier", StringType(), True),  # high/medium/low
    StructField("is_training_ready", BooleanType(), True),
    StructField("price_imputed", BooleanType(), True),
    StructField("coordinates_imputed", BooleanType(), True),
    
    # ===== AUDIT FIELDS =====
    StructField("gold_processed_at", StringType(), True),
    StructField("gold_version", StringType(), True),
])

# ===========================
# FEATURE IMPORTANCE WEIGHTS
# ===========================

FEATURE_IMPORTANCE = {
    # Location features (40%)
    "latitude": 0.15,
    "longitude": 0.15,
    "city_encoded": 0.05,
    "district_encoded": 0.05,
    
    # Property features (30%)
    "total_area": 0.08,
    "log_total_area": 0.08,
    "total_property": 0.05,
    "construction_density": 0.04,
    "number_of_floors": 0.03,
    "avg_area_per_unit": 0.02,
    
    # Developer features (15%)
    "developer_encoded": 0.10,
    "investor_name": 0.05,
    
    # Amenity features (10%)
    "amenity_score": 0.05,
    "trans_grade": 0.03,
    "infra_grade": 0.02,
    
    # Market features (5%)
    "year": 0.02,
    "quarter": 0.02,
    "month": 0.01
}

# ===========================
# QUALITY TIER DEFINITIONS
# ===========================

QUALITY_TIER_CRITERIA = {
    "high": {
        "description": "Ready for ML training",
        "requirements": [
            "target_price_per_sqm IS NOT NULL",
            "target_price_per_sqm > 0",
            "latitude IS NOT NULL",
            "longitude IS NOT NULL",
            "total_area > 0",
            "data_completeness_score >= 0.6"
        ]
    },
    "medium": {
        "description": "Use with caution - may need imputation",
        "requirements": [
            "target_price_per_sqm IS NOT NULL",
            "data_completeness_score >= 0.4"
        ]
    },
    "low": {
        "description": "Metadata only - exclude from training",
        "requirements": [
            "All others"
        ]
    }
}

# ===========================
# CATEGORICAL ENCODINGS
# ===========================

# City encoding (major cities)
CITY_ENCODING = {
    "Hanoi": 1,
    "Ho Chi Minh": 2,
    "Da Nang": 3,
    "Hai Phong": 4,
    "Can Tho": 5,
    "Bien Hoa": 6,
    "Vung Tau": 7,
    "Nha Trang": 8,
    "Hue": 9,
    "Buon Ma Thuot": 10,
    "UNKNOWN": 0
}

# Status encoding
STATUS_ENCODING = {
    "handedOver": 1,
    "selling": 2,
    "comingSoon": 3,
    "underConstruction": 4,
    "UNKNOWN": 0
}

# Grade encoding (for trans_grade, infra_grade, school_grade)
GRADE_ENCODING = {
    "Rất thuận tiện": 5,
    "Rất tốt": 5,
    "Thuận tiện": 4,
    "Tốt": 4,
    "Trung bình": 3,
    "Khá": 3,
    "Kém": 2,
    "Rất kém": 1,
    "UNKNOWN": 0
}
