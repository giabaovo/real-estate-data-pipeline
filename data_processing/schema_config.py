"""
Schema Configuration and Mapping
Maps different source schemas to unified Silver schema
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType, ArrayType, LongType
)

# ===========================
# UNIFIED SILVER SCHEMA
# ===========================
SILVER_SCHEMA = StructType([
    # Primary Keys
    StructField("universal_id", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("spider_name", StringType(), False),
    
    # Basic Information
    StructField("project_name", StringType(), True),
    StructField("project_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True),
    StructField("segment", StringType(), True),
    
    # Location
    StructField("address", StringType(), True),
    StructField("full_address", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("district", StringType(), True),
    StructField("city", StringType(), True),
    StructField("province", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    
    # Property Details
    StructField("total_area", DoubleType(), True),
    StructField("area_unit", StringType(), True),
    StructField("construction_area", DoubleType(), True),
    StructField("number_of_blocks", IntegerType(), True),
    StructField("total_property", IntegerType(), True),
    StructField("unit_total", StringType(), True),
    StructField("number_of_floors", IntegerType(), True),
    StructField("number_of_basement", IntegerType(), True),
    StructField("number_of_elevators", IntegerType(), True),
    StructField("green_density", DoubleType(), True),
    StructField("construction_density", DoubleType(), True),
    StructField("swimming_pool_density", StringType(), True),
    StructField("min_prop_per_floor", IntegerType(), True),
    StructField("max_prop_per_floor", IntegerType(), True),
    
    # Bedroom/Area Insights
    StructField("min_bedroom", IntegerType(), True),
    StructField("max_bedroom", IntegerType(), True),
    StructField("min_bathroom", IntegerType(), True),
    StructField("max_bathroom", IntegerType(), True),
    StructField("min_area", DoubleType(), True),
    StructField("max_area", DoubleType(), True),
    
    # Pricing (for real estate valuation)
    StructField("min_selling_price", DoubleType(), True),
    StructField("max_selling_price", DoubleType(), True),
    StructField("min_unit_price", DoubleType(), True),
    StructField("max_unit_price", DoubleType(), True),
    StructField("min_rent_price", DoubleType(), True),
    StructField("max_rent_price", DoubleType(), True),
    StructField("price_unit", StringType(), True),
    
    # Apartment Pricing by Bedroom (from insight_by_bedroom)
    StructField("apartment_prices", ArrayType(StructType([
        StructField("number_of_bedroom", IntegerType(), True),
        StructField("min_price", DoubleType(), True),
        StructField("max_price", DoubleType(), True),
        StructField("min_area", DoubleType(), True),
        StructField("max_area", DoubleType(), True)
    ])), True),
    
    # Developer/Investor
    StructField("investor_id", StringType(), True),
    StructField("investor_name", StringType(), True),
    StructField("developer_name", StringType(), True),
    
    # Dates (stored as StringType like ingestion_date)
    StructField("handover_date_from", StringType(), True),
    StructField("handover_date", StringType(), True),
    StructField("construction_start_date", StringType(), True),
    StructField("construction_end_date", StringType(), True),
    StructField("release_year", StringType(), True),
    
    # Utilities & Facilities
    StructField("facilities", ArrayType(StringType()), True),
    StructField("utilities_internal", ArrayType(StringType()), True),
    StructField("utilities_external", ArrayType(StringType()), True),
    StructField("quality_indexes", ArrayType(StringType()), True),
    
    # Infrastructure Grades (for valuation)
    StructField("trans_grade", StringType(), True),
    StructField("infra_grade", StringType(), True),
    StructField("school_grade", StringType(), True),
    
    # Media
    StructField("images", ArrayType(StringType()), True),
    StructField("videos", ArrayType(StringType()), True),
    StructField("master_plan_url", StringType(), True),
    StructField("web_url", StringType(), True),
    
    # Metadata & Audit (stored as StringType like ingestion_date)
    StructField("record_key", StringType(), True),
    StructField("data_completeness_score", DoubleType(), True),
    StructField("ingested_at_utc", StringType(), False),
    StructField("silver_processed_at", StringType(), False),
    StructField("silver_version", StringType(), False),
    
    # SCD Type 2 Fields (stored as StringType like ingestion_date)
    StructField("is_current", BooleanType(), False),
    StructField("valid_from", StringType(), False),
    StructField("valid_to", StringType(), True),
    
    # Partition Columns
    StructField("ingestion_year", StringType(), False),
    StructField("ingestion_month", StringType(), False),
    StructField("ingestion_date", StringType(), False),
])                                                                                  

# ===========================
# SOURCE SCHEMA MAPPINGS
# ===========================

# Mapping for Chotot API
CHOTOT_MAPPING = {
    "source_id": "project_oid",
    "project_name": "project_name",
    "project_code": "alias",
    "project_type": "type_name",
    "status": "process",
    "transaction_status": "transaction_status",
    "description": "introduction",
    "address": "address",
    "full_address": "full_address",
    "street_name": "street_name",
    "ward": "ward_name",
    "district": "area_name",
    "city": "region_name",
    "province": "region_name",
    "total_area": "area_total",
    "construction_area": "area_construction",
    "unit_total": "unit_total",
    "min_selling_price": "sell_price_lower",
    "max_selling_price": "sell_price_higher",
    "min_unit_price": "price_lowest_per_m2",
    "max_unit_price": "price_highest_per_m2",
    "min_rent_price": "rent_price_lower",
    "max_rent_price": "rent_price_higher",
    "investor_id": "investor_id",
    "investor_name": "investor_name",
    "construction_start_date": "start_construction",
    "facilities": "facilities",
    "images": "project_images",
    "web_url": "web_url"
}

# Mapping for Meeyproject API
MEEYPROJECT_MAPPING = {
    "source_id": "_id",
    "project_name": "name",
    "project_code": "tradeName",
    "project_slug": "slug",
    "description": "juridical.description",
    "address": "address",
    "total_area": "totalArea",
    "investor_name": "investorRelated.investor.name",
    "images": "images",
    "videos": "videos",
    "utilities_internal": "utilities.basicUtilities"
}

# Mapping for OneHousing API
ONEHOUSING_MAPPING = {
    "source_id": "id",
    "project_name": "name",
    "project_code": "code",
    "project_slug": "slug",
    "description": "description",
    "address": "address",
    "ward": "ward",
    "district": "district",
    "city": "city",
    "province": "province",
    "latitude": "lat_cdnt",
    "longitude": "long_cdnt",
    "total_area": "total_area",
    "number_of_blocks": "blocks",
    "total_property": "total_property",
    "number_of_floors": "number_living_floor",
    "green_density": "green_dens",
    "construction_density": "cstn_dens",
    "swimming_pool_density": "swim_dens",
    "min_prop_per_floor": "min_prop_per_floor",
    "max_prop_per_floor": "max_prop_per_floor",
    "min_selling_price": "min_selling_price",
    "max_selling_price": "max_selling_price",
    "min_unit_price": "min_unit_price",
    "max_unit_price": "max_unit_price",
    "apartment_prices": "insight_by_bedroom",
    "developer_name": "developer_name",
    "handover_date_from": "handover_date_from",
    "construction_start_date": "construction_start_date_from",
    "trans_grade": "trans_grade",
    "infra_grade": "infra_grade",
    "school_grade": "school_grade",
    "master_plan_url": "master_plan",
    "quality_indexes": "quality_indexes",
    "images": "albums",
    "videos": "videos"
}

# Combined mapping dictionary
SOURCE_MAPPINGS = {
    "chotot_api": CHOTOT_MAPPING,
    "meeyproject_api": MEEYPROJECT_MAPPING,
    "onehousing_api": ONEHOUSING_MAPPING
}

# ===========================
# DATA TYPE CONVERSIONS
# ===========================

# Fields that need type conversion
TYPE_CONVERSIONS = {
    "min_selling_price": "double",
    "max_selling_price": "double",
    "min_unit_price": "double",
    "max_unit_price": "double",
    "min_rent_price": "double",
    "max_rent_price": "double",
    "total_area": "double",
    "construction_area": "double",
    "green_density": "double",
    "construction_density": "double",
    "min_area": "double",
    "max_area": "double",
    "latitude": "double",
    "longitude": "double",
    "number_of_blocks": "integer",
    "total_property": "integer",
    "number_of_floors": "integer",
    "number_of_basement": "integer",
    "number_of_elevators": "integer",
    "min_bedroom": "integer",
    "max_bedroom": "integer",
    "min_bathroom": "integer",
    "max_bathroom": "integer",
    "min_prop_per_floor": "integer",
    "max_prop_per_floor": "integer"
}

# ===========================
# VALIDATION RULES
# ===========================

# Field-level validation rules
VALIDATION_RULES = {
    "spider_name": {
        "required": True,
        "type": "string",
        "allowed_values": ["chotot_api", "meeyproject_api", "onehousing_api"]
    },
    "source_id": {
        "required": True,
        "type": "string",
        "min_length": 1
    },
    "project_name": {
        "required": True,
        "type": "string",
        "min_length": 1,
        "max_length": 500
    },
    "latitude": {
        "required": False,
        "type": "double",
        "min_value": -90.0,
        "max_value": 90.0
    },
    "longitude": {
        "required": False,
        "type": "double",
        "min_value": -180.0,
        "max_value": 180.0
    },
    "price": {
        "required": False,
        "type": "double",
        "min_value": 0.0
    },
    "total_area": {
        "required": False,
        "type": "double",
        "min_value": 0.0
    },
    "email": {
        "required": False,
        "type": "string",
        "pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    },
    "phone": {
        "required": False,
        "type": "string",
        "pattern": r'^\d{10,11}$'
    }
}

# ===========================
# DEFAULT VALUES
# ===========================

# Default values for missing fields
DEFAULT_VALUES = {
    "project_type": "UNKNOWN",
    "status": "UNKNOWN",
    "transaction_status": "UNKNOWN",
    "area_unit": "m²",
    "price_unit": "VND",
    "description": "",
    "address": "",
    "rank": "UNKNOWN",
    "trans_grade": "UNKNOWN",
    "infra_grade": "UNKNOWN",
    "school_grade": "UNKNOWN"
}

# ===========================
# ENRICHMENT RULES
# ===========================

# Rules for derived/calculated fields
ENRICHMENT_RULES = {
    "calculate_price_per_sqm": {
        "enabled": True,
        "formula": "price / total_area",
        "condition": "price IS NOT NULL AND total_area IS NOT NULL AND total_area > 0"
    },
    "standardize_city_names": {
        "enabled": True,
        "mappings": {
            "Hồ Chí Minh": "Ho Chi Minh City",
            "Tp. Hồ Chí Minh": "Ho Chi Minh City",
            "TPHCM": "Ho Chi Minh City",
            "Hà Nội": "Hanoi",
            "TP Hà Nội": "Hanoi",
            "Đà Nẵng": "Da Nang",
            "TP Đà Nẵng": "Da Nang"
        }
    },
    "extract_bedroom_from_name": {
        "enabled": True,
        "pattern": r'(\d+)\s*(PN|phòng ngủ|bedroom)',
        "field": "project_name"
    }
}

# ===========================
# QUARANTINE RULES
# ===========================

# Rules to determine if record should be quarantined
QUARANTINE_RULES = {
    "missing_critical_fields": {
        "fields": ["source_id", "spider_name"],
        "action": "quarantine",
        "reason": "Missing critical identification fields"
    },
    "invalid_coordinates": {
        "condition": "(latitude IS NULL OR longitude IS NULL OR latitude = 0 OR longitude = 0)",
        "action": "warn",  # warn but don't quarantine
        "reason": "Invalid or missing coordinates"
    },
    "suspicious_price": {
        "condition": "price < 0 OR price > 999999999999",
        "action": "quarantine",
        "reason": "Price value out of reasonable range"
    },
    "duplicate_within_source": {
        "check": "duplicate_source_id",
        "action": "quarantine",
        "reason": "Duplicate source_id within same spider"
    }
}

# ===========================
# SPECIAL FIELD TRANSFORMATIONS
# ===========================

# Special handling for nested/complex fields per source
SPECIAL_TRANSFORMATIONS = {
    "chotot_api": {
        "geo_split": {
            "source_field": "geo",
            "target_fields": ["latitude", "longitude"],
            "delimiter": ",",
            "transform": "split_and_cast_double"
        }
    },
    "onehousing_api": {
        "bedroom_insights": {
            "source_field": "insight_by_bedroom",
            "transform": "extract_bedroom_bathroom_ranges"
        },
        "quality_indexes_extract": {
            "source_field": "quality_indexes",
            "transform": "extract_quality_index_names"
        },
        "albums_extract": {
            "source_field": "albums",
            "transform": "extract_album_images"
        },
        "number_arrays": {
            "number_of_basement": "number_basement",
            "number_of_elevators": "number_ele",
            "transform": "extract_first_from_array"
        }
    },
    "meeyproject_api": {
        "location_extract": {
            "source_field": "location.coordinates",
            "target_fields": ["longitude", "latitude"],
            "transform": "array_extract"
        },
        "nested_fields": {
            "investor_name": "investorRelated.investor.name",
            "description": "juridical.description",
            "utilities_internal": "utilities.basicUtilities"
        },
        "translation_fields": {
            "ward": "ward.translation[0].name",
            "district": "district.translation[0].name",
            "city": "city.translation[0].name"
        }
    }
}

# ===========================
# HELPER FUNCTIONS
# ===========================

def get_mapping_for_source(spider_name: str) -> dict:
    """Get field mapping for a specific source"""
    return SOURCE_MAPPINGS.get(spider_name, {})

def get_required_fields() -> list:
    """Get list of required fields based on validation rules"""
    return [
        field for field, rules in VALIDATION_RULES.items()
        if rules.get("required", False)
    ]

def get_default_value(field_name: str):
    """Get default value for a field"""
    return DEFAULT_VALUES.get(field_name, None)

def get_special_transformations(spider_name: str) -> dict:
    """Get special transformations for a specific source"""
    return SPECIAL_TRANSFORMATIONS.get(spider_name, {})