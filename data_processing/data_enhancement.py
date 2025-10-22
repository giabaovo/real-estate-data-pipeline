"""
Data Enhancement Module for Silver Layer
Handles price imputation and geocoding for missing data
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, avg, median, stddev, count, 
    expr, udf, broadcast, coalesce
)
from pyspark.sql.types import DoubleType, StructType, StructField
import math

# ===========================
# PRICE IMPUTATION
# ===========================

def impute_missing_prices(df):
    """
    Impute missing prices using district-level statistics
    
    Strategy:
    1. Calculate median price per m² by city/district
    2. For records without price, use district median * total_area
    3. Mark imputed records for transparency
    """
    print("\n" + "="*70)
    print("💰 PRICE IMPUTATION")
    print("="*70)
    
    # Count records needing imputation
    missing_price_count = df.filter(
        (col("min_selling_price").isNull()) | 
        (col("min_selling_price") == 0)
    ).count()
    
    print(f"  📊 Records with missing prices: {missing_price_count:,}")
    
    if missing_price_count == 0:
        print("  ✅ No price imputation needed")
        df = df.withColumn("price_imputed", lit(False))
        return df
    
    # Step 1: Calculate district-level price statistics
    print("  🔢 Calculating district-level price statistics...")
    
    district_stats = df.filter(
        (col("avg_unit_price").isNotNull()) & 
        (col("avg_unit_price") > 0) &
        (col("city").isNotNull()) &
        (col("district").isNotNull())
    ).groupBy("city", "district").agg(
        median("avg_unit_price").alias("district_median_unit_price"),
        avg("avg_unit_price").alias("district_avg_unit_price"),
        stddev("avg_unit_price").alias("district_std_unit_price"),
        count("*").alias("district_sample_count")
    )
    
    # Step 2: Calculate city-level fallback (if district has no data)
    city_stats = df.filter(
        (col("avg_unit_price").isNotNull()) & 
        (col("avg_unit_price") > 0) &
        (col("city").isNotNull())
    ).groupBy("city").agg(
        median("avg_unit_price").alias("city_median_unit_price"),
        avg("avg_unit_price").alias("city_avg_unit_price")
    )
    
    # Step 3: Join statistics back to main dataframe
    df = df.join(
        broadcast(district_stats), 
        on=["city", "district"], 
        how="left"
    )
    
    df = df.join(
        broadcast(city_stats), 
        on=["city"], 
        how="left"
    )
    
    # Step 4: Impute prices
    print("  🔧 Imputing missing prices...")
    
    # Use district median, fallback to city median
    df = df.withColumn(
        "imputed_unit_price",
        coalesce(
            col("district_median_unit_price"),
            col("city_median_unit_price"),
            lit(80000000)  # National average fallback (80M VND/m²)
        )
    )
    
    # Calculate imputed selling price (unit_price * total_area)
    df = df.withColumn(
        "imputed_selling_price",
        when(col("total_area").isNotNull() & (col("total_area") > 0),
             col("imputed_unit_price") * col("total_area")
        ).otherwise(lit(None))
    )
    
    # Step 5: Apply imputation (keep original if exists)
    df = df.withColumn(
        "price_imputed",
        when(
            (col("avg_selling_price").isNull()) | (col("avg_selling_price") == 0),
            lit(True)
        ).otherwise(lit(False))
    )
    
    df = df.withColumn(
        "avg_selling_price",
        when(
            col("price_imputed") == True,
            col("imputed_selling_price")
        ).otherwise(col("avg_selling_price"))
    )
    
    df = df.withColumn(
        "avg_unit_price",
        when(
            col("price_imputed") == True,
            col("imputed_unit_price")
        ).otherwise(col("avg_unit_price"))
    )
    
    # Recalculate min/max prices if imputed
    df = df.withColumn(
        "min_selling_price",
        when(
            col("price_imputed") == True,
            col("avg_selling_price") * 0.9  # Assume ±10% range
        ).otherwise(col("min_selling_price"))
    )
    
    df = df.withColumn(
        "max_selling_price",
        when(
            col("price_imputed") == True,
            col("avg_selling_price") * 1.1
        ).otherwise(col("max_selling_price"))
    )
    
    # Clean up temporary columns
    df = df.drop(
        "district_median_unit_price", "district_avg_unit_price", 
        "district_std_unit_price", "district_sample_count",
        "city_median_unit_price", "city_avg_unit_price",
        "imputed_unit_price", "imputed_selling_price"
    )
    
    # Count imputed records
    imputed_count = df.filter(col("price_imputed") == True).count()
    print(f"  ✅ Imputed prices for {imputed_count:,} records")
    
    return df


# ===========================
# GEOCODING
# ===========================

def geocode_missing_coordinates(df):
    """
    Geocode missing coordinates using district centroids
    
    Strategy:
    1. Use predefined district centroids for major cities
    2. Mark geocoded records for transparency
    """
    print("\n" + "="*70)
    print("📍 GEOCODING MISSING COORDINATES")
    print("="*70)
    
    # Count records needing geocoding
    missing_coords_count = df.filter(
        col("latitude").isNull() | 
        col("longitude").isNull() |
        (col("latitude") == 0) |
        (col("longitude") == 0)
    ).count()
    
    print(f"  📊 Records with missing coordinates: {missing_coords_count:,}")
    
    if missing_coords_count == 0:
        print("  ✅ No geocoding needed")
        df = df.withColumn("coordinates_imputed", lit(False))
        return df
    
    # District centroids for major cities
    DISTRICT_CENTROIDS = {
        # Hanoi
        ("Hanoi", "Ba Dinh"): (21.0333, 105.8189),
        ("Hanoi", "Ba Đình"): (21.0333, 105.8189),
        ("Hanoi", "Hoan Kiem"): (21.0285, 105.8542),
        ("Hanoi", "Hoàn Kiếm"): (21.0285, 105.8542),
        ("Hanoi", "Dong Da"): (21.0167, 105.8250),
        ("Hanoi", "Đống Đa"): (21.0167, 105.8250),
        ("Hanoi", "Hai Ba Trung"): (21.0069, 105.8511),
        ("Hanoi", "Hai Bà Trưng"): (21.0069, 105.8511),
        ("Hanoi", "Cau Giay"): (21.0333, 105.7944),
        ("Hanoi", "Cầu Giấy"): (21.0333, 105.7944),
        ("Hanoi", "Thanh Xuan"): (20.9950, 105.8050),
        ("Hanoi", "Thanh Xuân"): (20.9950, 105.8050),
        ("Hanoi", "Tay Ho"): (21.0750, 105.8200),
        ("Hanoi", "Tây Hồ"): (21.0750, 105.8200),
        ("Hanoi", "Long Bien"): (21.0364, 105.8833),
        ("Hanoi", "Long Biên"): (21.0364, 105.8833),
        ("Hanoi", "Hoang Mai"): (20.9750, 105.8500),
        ("Hanoi", "Hoàng Mai"): (20.9750, 105.8500),
        ("Hanoi", "Ha Dong"): (20.9722, 105.7750),
        ("Hanoi", "Hà Đông"): (20.9722, 105.7750),
        ("Hanoi", "Nam Tu Liem"): (21.0167, 105.7500),
        ("Hanoi", "Nam Từ Liêm"): (21.0167, 105.7500),
        ("Hanoi", "Bac Tu Liem"): (21.0667, 105.7500),
        ("Hanoi", "Bắc Từ Liêm"): (21.0667, 105.7500),
        
        # Ho Chi Minh City
        ("Ho Chi Minh", "District 1"): (10.7769, 106.7009),
        ("Ho Chi Minh", "Quận 1"): (10.7769, 106.7009),
        ("Ho Chi Minh", "District 2"): (10.7833, 106.7500),
        ("Ho Chi Minh", "Quận 2"): (10.7833, 106.7500),
        ("Ho Chi Minh", "District 3"): (10.7833, 106.6833),
        ("Ho Chi Minh", "Quận 3"): (10.7833, 106.6833),
        ("Ho Chi Minh", "District 4"): (10.7583, 106.7000),
        ("Ho Chi Minh", "Quận 4"): (10.7583, 106.7000),
        ("Ho Chi Minh", "District 5"): (10.7583, 106.6667),
        ("Ho Chi Minh", "Quận 5"): (10.7583, 106.6667),
        ("Ho Chi Minh", "District 7"): (10.7333, 106.7167),
        ("Ho Chi Minh", "Quận 7"): (10.7333, 106.7167),
        ("Ho Chi Minh", "District 10"): (10.7750, 106.6667),
        ("Ho Chi Minh", "Quận 10"): (10.7750, 106.6667),
        ("Ho Chi Minh", "Binh Thanh"): (10.8083, 106.7000),
        ("Ho Chi Minh", "Bình Thạnh"): (10.8083, 106.7000),
        ("Ho Chi Minh", "Phu Nhuan"): (10.7972, 106.6833),
        ("Ho Chi Minh", "Phú Nhuận"): (10.7972, 106.6833),
        ("Ho Chi Minh", "Tan Binh"): (10.8000, 106.6500),
        ("Ho Chi Minh", "Tân Bình"): (10.8000, 106.6500),
        ("Ho Chi Minh", "Go Vap"): (10.8333, 106.6667),
        ("Ho Chi Minh", "Gò Vấp"): (10.8333, 106.6667),
        ("Ho Chi Minh", "Thu Duc"): (10.8500, 106.7500),
        ("Ho Chi Minh", "Thủ Đức"): (10.8500, 106.7500),
        
        # Hai Phong
        ("Hai Phong", "Hong Bang"): (20.8525, 106.6781),
        ("Hai Phong", "Hồng Bàng"): (20.8525, 106.6781),
        ("Hai Phong", "Le Chan"): (20.8450, 106.6900),
        ("Hai Phong", "Lê Chân"): (20.8450, 106.6900),
        ("Hai Phong", "Ngo Quyen"): (20.8600, 106.6850),
        ("Hai Phong", "Ngô Quyền"): (20.8600, 106.6850),
        
        # Da Nang
        ("Da Nang", "Hai Chau"): (16.0544, 108.2022),
        ("Da Nang", "Hải Châu"): (16.0544, 108.2022),
        ("Da Nang", "Thanh Khe"): (16.0611, 108.1667),
        ("Da Nang", "Thanh Khê"): (16.0611, 108.1667),
        ("Da Nang", "Son Tra"): (16.0833, 108.2500),
        ("Da Nang", "Sơn Trà"): (16.0833, 108.2500),
    }
    
    # Create lookup UDF
    def get_district_centroid(city, district):
        """Get centroid coordinates for city/district"""
        if not city or not district:
            return (None, None)
        
        # Normalize city names
        city_normalized = city.replace("TP. ", "").replace("Thành phố ", "").strip()
        district_normalized = district.replace("Q. ", "").replace("Quận ", "").replace("P. ", "").replace("Phường ", "").strip()
        
        # Try exact match
        key = (city_normalized, district_normalized)
        if key in DISTRICT_CENTROIDS:
            return DISTRICT_CENTROIDS[key]
        
        # Try case-insensitive match
        for (c, d), coords in DISTRICT_CENTROIDS.items():
            if c.lower() == city_normalized.lower() and d.lower() == district_normalized.lower():
                return coords
        
        return (None, None)
    
    centroid_udf = udf(get_district_centroid, StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ]))
    
    # Apply geocoding
    print("  🔧 Geocoding using district centroids...")
    
    df = df.withColumn(
        "centroid_coords",
        centroid_udf(col("city"), col("district"))
    )
    
    df = df.withColumn(
        "coordinates_imputed",
        when(
            (col("latitude").isNull()) | (col("latitude") == 0) |
            (col("longitude").isNull()) | (col("longitude") == 0),
            lit(True)
        ).otherwise(lit(False))
    )
    
    df = df.withColumn(
        "latitude",
        when(
            col("coordinates_imputed") == True,
            col("centroid_coords.lat")
        ).otherwise(col("latitude"))
    )
    
    df = df.withColumn(
        "longitude",
        when(
            col("coordinates_imputed") == True,
            col("centroid_coords.lon")
        ).otherwise(col("longitude"))
    )
    
    # Clean up
    df = df.drop("centroid_coords")
    
    # Count geocoded records
    geocoded_count = df.filter(col("coordinates_imputed") == True).count()
    print(f"  ✅ Geocoded {geocoded_count:,} records using district centroids")
    
    # Count still missing
    still_missing = df.filter(
        col("latitude").isNull() | col("longitude").isNull()
    ).count()
    
    if still_missing > 0:
        print(f"  ⚠️  {still_missing:,} records still missing coordinates (unknown districts)")
    
    return df


# ===========================
# MAIN ENHANCEMENT FUNCTION
# ===========================

def enhance_silver_data(df):
    """
    Main function to enhance Silver layer data
    """
    print("\n" + "="*70)
    print("🔧 DATA ENHANCEMENT PIPELINE")
    print("="*70)
    
    # Step 1: Price imputation
    df = impute_missing_prices(df)
    
    # Step 2: Geocoding
    df = geocode_missing_coordinates(df)
    
    # Step 3: Update quality scores
    df = update_quality_scores(df)
    
    print("\n" + "="*70)
    print("✅ DATA ENHANCEMENT COMPLETED")
    print("="*70)
    
    return df


def update_quality_scores(df):
    """
    Recalculate quality scores after enhancement
    """
    print("\n  📊 Updating quality scores...")
    
    # Update _has_valid_price
    df = df.withColumn(
        "_has_valid_price",
        when(
            (col("avg_selling_price").isNotNull()) & 
            (col("avg_selling_price") > 0),
            lit(True)
        ).otherwise(lit(False))
    )
    
    # Update _has_valid_coords
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
    
    # Recalculate location_quality_score
    df = df.withColumn(
        "location_quality_score",
        (
            col("_has_valid_coords").cast("double") * 0.4 +
            when(col("district").isNotNull() & (col("district") != ""), 0.3).otherwise(0.0) +
            when(col("ward").isNotNull() & (col("ward") != ""), 0.3).otherwise(0.0)
        )
    )
    
    # Recalculate data_completeness_score after enhancement
    # Count non-null critical fields
    critical_fields = [
        "_has_valid_price",
        "_has_valid_coords", 
        when(col("total_area").isNotNull() & (col("total_area") > 0), 1).otherwise(0),
        when(col("project_name").isNotNull() & (col("project_name") != ""), 1).otherwise(0),
        when(col("district").isNotNull() & (col("district") != ""), 1).otherwise(0),
        when(col("city").isNotNull() & (col("city") != ""), 1).otherwise(0)
    ]
    
    # Calculate completeness as ratio of filled fields
    completeness_expr = (
        col("_has_valid_price").cast("double") +
        col("_has_valid_coords").cast("double") +
        when(col("total_area").isNotNull() & (col("total_area") > 0), 1.0).otherwise(0.0) +
        when(col("project_name").isNotNull() & (col("project_name") != ""), 1.0).otherwise(0.0) +
        when(col("district").isNotNull() & (col("district") != ""), 1.0).otherwise(0.0) +
        when(col("city").isNotNull() & (col("city") != ""), 1.0).otherwise(0.0)
    ) / 6.0  # 6 critical fields
    
    df = df.withColumn("data_completeness_score", completeness_expr)
    
    # Clean up temporary columns
    df = df.drop("_has_valid_price", "_has_valid_coords")
    
    print("  ✅ Quality scores updated")
    
    return df
