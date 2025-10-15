"""
Transformation Utilities
Reusable transformation functions for Silver Layer ETL
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, lower, upper, 
    concat_ws, split, expr, udf, coalesce, lit,
    to_timestamp, to_date, year, month, dayofmonth,
    get_json_object, from_json, explode, collect_list,
    array_min, array_max, size, element_at
)
from pyspark.sql.types import StringType, DoubleType, ArrayType, IntegerType
from typing import Dict, List
import re


class DataTransformer:
    """Utility class for common data transformations"""
    
    @staticmethod
    def standardize_phone_numbers(df: DataFrame, phone_column: str) -> DataFrame:
        """
        Standardize phone numbers to consistent format
        Removes non-numeric characters and validates Vietnamese phone format
        
        Args:
            df: Input DataFrame
            phone_column: Name of the phone column
        
        Returns:
            DataFrame with standardized phone numbers
        """
        return df.withColumn(
            phone_column,
            when(
                col(phone_column).isNotNull(),
                # Remove all non-numeric characters
                regexp_replace(col(phone_column), r'[^\d]', '')
            ).otherwise(lit(""))
        ).withColumn(
            phone_column,
            when(
                # Vietnamese phone: 10-11 digits starting with 0
                col(phone_column).rlike(r'^0\d{9,10}$'),
                col(phone_column)
            ).otherwise(lit(""))
        )
    
    @staticmethod
    def standardize_emails(df: DataFrame, email_column: str) -> DataFrame:
        """
        Standardize email addresses to lowercase and trim whitespace
        
        Args:
            df: Input DataFrame
            email_column: Name of the email column
        
        Returns:
            DataFrame with standardized emails
        """
        return df.withColumn(
            email_column,
            when(
                col(email_column).isNotNull(),
                lower(trim(col(email_column)))
            ).otherwise(lit(""))
        ).withColumn(
            email_column,
            when(
                # Validate email format
                col(email_column).rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
                col(email_column)
            ).otherwise(lit(""))
        )
    
    @staticmethod
    def normalize_vietnamese_text(df: DataFrame, text_columns: List[str]) -> DataFrame:
        """
        Normalize Vietnamese text: trim, standardize spacing
        
        Args:
            df: Input DataFrame
            text_columns: List of columns to normalize
        
        Returns:
            DataFrame with normalized text
        """
        for col_name in text_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNotNull(),
                        # Trim and replace multiple spaces with single space
                        regexp_replace(trim(col(col_name)), r'\s+', ' ')
                    ).otherwise(col(col_name))
                )
        return df
    
    @staticmethod
    def extract_numeric_from_string(df: DataFrame, 
                                   source_column: str, 
                                   target_column: str) -> DataFrame:
        """
        Extract numeric values from string fields
        
        Args:
            df: Input DataFrame
            source_column: Source column with mixed content
            target_column: Target column for numeric values
        
        Returns:
            DataFrame with extracted numeric values
        """
        return df.withColumn(
            target_column,
            regexp_replace(col(source_column), r'[^\d.]', '').cast(DoubleType())
        )
    
    @staticmethod
    def standardize_city_names(df: DataFrame, city_column: str) -> DataFrame:
        """
        Standardize city names to English format
        
        Args:
            df: Input DataFrame
            city_column: Name of the city column
        
        Returns:
            DataFrame with standardized city names
        """
        city_mappings = {
            "Hồ Chí Minh": "Ho Chi Minh City",
            "Tp. Hồ Chí Minh": "Ho Chi Minh City",
            "TPHCM": "Ho Chi Minh City",
            "Sài Gòn": "Ho Chi Minh City",
            "Hà Nội": "Hanoi",
            "TP Hà Nội": "Hanoi",
            "Đà Nẵng": "Da Nang",
            "TP Đà Nẵng": "Da Nang",
            "Cần Thơ": "Can Tho",
            "Hải Phòng": "Hai Phong",
            "Biên Hòa": "Bien Hoa",
            "Nha Trang": "Nha Trang",
            "Vũng Tàu": "Vung Tau"
        }
        
        city_expr = col(city_column)
        for vietnamese, english in city_mappings.items():
            city_expr = when(
                trim(col(city_column)) == vietnamese,
                lit(english)
            ).otherwise(city_expr)
        
        return df.withColumn(city_column, city_expr)
    
    @staticmethod
    def parse_price_strings(df: DataFrame, 
                           price_column: str,
                           unit_column: str = "price_unit") -> DataFrame:
        """
        Parse price strings with units (e.g., "5.5 tỷ", "500 triệu")
        
        Args:
            df: Input DataFrame
            price_column: Column containing price strings
            unit_column: Column to store detected unit
        
        Returns:
            DataFrame with parsed numeric prices in VND
        """
        return df.withColumn(
            price_column,
            when(
                col(price_column).rlike(r'tỷ|ty|billion'),
                regexp_replace(col(price_column), r'[^\d.]', '').cast(DoubleType()) * 1000000000
            ).when(
                col(price_column).rlike(r'triệu|tr|million'),
                regexp_replace(col(price_column), r'[^\d.]', '').cast(DoubleType()) * 1000000
            ).when(
                col(price_column).rlike(r'nghìn|ngàn|k'),
                regexp_replace(col(price_column), r'[^\d.]', '').cast(DoubleType()) * 1000
            ).otherwise(
                regexp_replace(col(price_column), r'[^\d.]', '').cast(DoubleType())
            )
        ).withColumn(
            unit_column,
            lit("VND")
        )
    
    @staticmethod
    def calculate_price_per_sqm(df: DataFrame,
                               price_column: str = "price",
                               area_column: str = "total_area",
                               target_column: str = "price_per_sqm") -> DataFrame:
        """
        Calculate price per square meter
        
        Args:
            df: Input DataFrame
            price_column: Column with total price
            area_column: Column with area
            target_column: Target column for result
        
        Returns:
            DataFrame with calculated price per sqm
        """
        return df.withColumn(
            target_column,
            when(
                (col(price_column).isNotNull()) & 
                (col(area_column).isNotNull()) & 
                (col(area_column) > 0),
                col(price_column) / col(area_column)
            ).otherwise(lit(None))
        )
    
    @staticmethod
    def extract_coordinates(df: DataFrame,
                          location_column: str,
                          lat_column: str = "latitude",
                          lng_column: str = "longitude") -> DataFrame:
        """
        Extract coordinates from location strings or objects
        
        Args:
            df: Input DataFrame
            location_column: Column containing location data
            lat_column: Target latitude column
            lng_column: Target longitude column
        
        Returns:
            DataFrame with extracted coordinates
        """
        # Handle nested structures (common in API responses)
        if location_column in df.columns:
            df = df.withColumn(
                lat_column,
                when(
                    col(f"{location_column}.lat").isNotNull(),
                    col(f"{location_column}.lat")
                ).when(
                    col(f"{location_column}.latitude").isNotNull(),
                    col(f"{location_column}.latitude")
                ).otherwise(col(lat_column))
            ).withColumn(
                lng_column,
                when(
                    col(f"{location_column}.lng").isNotNull(),
                    col(f"{location_column}.lng")
                ).when(
                    col(f"{location_column}.longitude").isNotNull(),
                    col(f"{location_column}.longitude")
                ).otherwise(col(lng_column))
            )
        
        return df
    
    @staticmethod
    def flatten_nested_json(df: DataFrame, 
                           nested_column: str,
                           target_columns: Dict[str, str]) -> DataFrame:
        """
        Flatten nested JSON structures
        
        Args:
            df: Input DataFrame
            nested_column: Column with nested structure
            target_columns: Dict mapping nested paths to target column names
        
        Returns:
            DataFrame with flattened structure
        """
        for nested_path, target_col in target_columns.items():
            full_path = f"{nested_column}.{nested_path}"
            if full_path in df.columns:
                df = df.withColumn(target_col, col(full_path))
        
        return df
    
    @staticmethod
    def standardize_dates(df: DataFrame,
                         date_columns: List[str],
                         date_format: str = "yyyy-MM-dd") -> DataFrame:
        """
        Standardize date columns to consistent format
        
        Args:
            df: Input DataFrame
            date_columns: List of date column names
            date_format: Target date format
        
        Returns:
            DataFrame with standardized dates
        """
        for col_name in date_columns:
            if col_name in df.columns:
                # Try multiple common formats
                df = df.withColumn(
                    col_name,
                    coalesce(
                        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
                        to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"),
                        to_timestamp(col(col_name), "dd/MM/yyyy"),
                        to_timestamp(col(col_name), "yyyy-MM-dd"),
                        to_timestamp(col(col_name))
                    )
                )
        
        return df
    
    @staticmethod
    def add_hash_id(df: DataFrame,
                   columns_to_hash: List[str],
                   target_column: str = "hash_id") -> DataFrame:
        """
        Create hash ID from multiple columns
        
        Args:
            df: Input DataFrame
            columns_to_hash: Columns to include in hash
            target_column: Target column for hash
        
        Returns:
            DataFrame with hash ID
        """
        from pyspark.sql.functions import sha2, concat_ws
        
        return df.withColumn(
            target_column,
            sha2(concat_ws("_", *[col(c) for c in columns_to_hash]), 256)
        )
    
    @staticmethod
    def categorize_price_range(df: DataFrame,
                              price_column: str = "avg_selling_price",
                              category_column: str = "price_category") -> DataFrame:
        """
        Categorize properties into price ranges for valuation
        
        Args:
            df: Input DataFrame
            price_column: Column with price values
            category_column: Target category column
        
        Returns:
            DataFrame with price categories
        """
        return df.withColumn(
            category_column,
            when(col(price_column) < 1000000000, "Under 1B")  # < 1 billion VND
            .when(col(price_column) < 3000000000, "1B-3B")
            .when(col(price_column) < 5000000000, "3B-5B")
            .when(col(price_column) < 10000000000, "5B-10B")
            .when(col(price_column) >= 10000000000, "Over 10B")
            .otherwise("Unknown")
        )
    
    @staticmethod
    def calculate_average_prices(df: DataFrame) -> DataFrame:
        """
        Calculate average prices from min/max ranges for valuation
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with calculated average prices
        """
        # Average selling price
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
        
        # Average unit price
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
        
        # Average rent price
        df = df.withColumn(
            "avg_rent_price",
            when(
                col("min_rent_price").isNotNull() & col("max_rent_price").isNotNull(),
                (col("min_rent_price") + col("max_rent_price")) / 2
            ).when(
                col("min_rent_price").isNotNull(),
                col("min_rent_price")
            ).when(
                col("max_rent_price").isNotNull(),
                col("max_rent_price")
            ).otherwise(lit(None))
        )
        
        return df
    
    @staticmethod
    def calculate_price_ranges(df: DataFrame) -> DataFrame:
        """
        Calculate price and area ranges for variance analysis
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with calculated ranges
        """
        # Price range
        df = df.withColumn(
            "price_range",
            when(
                col("min_selling_price").isNotNull() & col("max_selling_price").isNotNull(),
                col("max_selling_price") - col("min_selling_price")
            ).otherwise(lit(None))
        )
        
        # Area range
        df = df.withColumn(
            "area_range",
            when(
                col("min_area").isNotNull() & col("max_area").isNotNull(),
                col("max_area") - col("min_area")
            ).otherwise(lit(None))
        )
        
        return df
    
    @staticmethod
    def calculate_location_quality_score(df: DataFrame) -> DataFrame:
        """
        Calculate location quality score based on infrastructure grades
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with location quality score
        """
        return df.withColumn(
            "location_quality_score",
            when(
                col("trans_grade").isNotNull() | 
                col("infra_grade").isNotNull() | 
                col("school_grade").isNotNull(),
                # Count non-null grades and divide by 3
                (when(col("trans_grade").isNotNull(), 1).otherwise(0) +
                 when(col("infra_grade").isNotNull(), 1).otherwise(0) +
                 when(col("school_grade").isNotNull(), 1).otherwise(0)) / 3.0
            ).otherwise(lit(0.0))
        )
    
    @staticmethod
    def extract_project_features(df: DataFrame,
                                description_column: str = "description") -> DataFrame:
        """
        Extract features from project descriptions using keywords
        
        Args:
            df: Input DataFrame
            description_column: Column with text descriptions
        
        Returns:
            DataFrame with extracted boolean feature flags
        """
        features = {
            "has_swimming_pool": r'(bể bơi|hồ bơi|swimming pool)',
            "has_gym": r'(phòng gym|gym|fitness)',
            "has_parking": r'(bãi đỗ xe|chỗ đậu xe|parking)',
            "has_garden": r'(vườn|sân vườn|garden)',
            "has_security": r'(bảo vệ|an ninh|security)',
            "has_playground": r'(khu vui chơi|sân chơi|playground)'
        }
        
        for feature_name, pattern in features.items():
            df = df.withColumn(
                feature_name,
                when(
                    col(description_column).rlike(pattern),
                    lit(True)
                ).otherwise(lit(False))
            )
        
        return df
    
    @staticmethod
    def extract_bedroom_bathroom_ranges(df: DataFrame) -> DataFrame:
        """
        Extract bedroom and bathroom ranges from OneHousing insight_by_bedroom array
        
        Args:
            df: Input DataFrame with insight_by_bedroom field
        
        Returns:
            DataFrame with min/max bedroom and bathroom counts
        """
        if "insight_by_bedroom" in df.columns:
            df = df.withColumn(
                "min_bedroom",
                when(
                    col("insight_by_bedroom").isNotNull() & (size(col("insight_by_bedroom")) > 0),
                    element_at(col("insight_by_bedroom"), 1).getField("number_of_bedroom").cast(IntegerType())
                ).otherwise(lit(None))
            ).withColumn(
                "max_bedroom",
                when(
                    col("insight_by_bedroom").isNotNull() & (size(col("insight_by_bedroom")) > 0),
                    element_at(col("insight_by_bedroom"), -1).getField("number_of_bedroom").cast(IntegerType())
                ).otherwise(lit(None))
            )
        
        return df
    
    @staticmethod
    def extract_quality_index_names(df: DataFrame) -> DataFrame:
        """
        Extract quality index names from OneHousing quality_indexes array
        
        Args:
            df: Input DataFrame with quality_indexes field
        
        Returns:
            DataFrame with quality_indexes as array of strings
        """
        if "quality_indexes" in df.columns:
            # Extract 'name' field from each quality index object
            df = df.withColumn(
                "quality_indexes",
                when(
                    col("quality_indexes").isNotNull(),
                    expr("transform(quality_indexes, x -> x.name)")
                ).otherwise(lit(None))
            )
        
        return df
    
    @staticmethod
    def extract_album_images(df: DataFrame) -> DataFrame:
        """
        Extract image URLs from OneHousing albums array
        
        Args:
            df: Input DataFrame with albums field
        
        Returns:
            DataFrame with flattened images array
        """
        if "albums" in df.columns:
            # Extract all images from albums
            df = df.withColumn(
                "images",
                when(
                    col("albums").isNotNull() & (size(col("albums")) > 0),
                    expr("flatten(transform(albums, x -> x.images))")
                ).otherwise(lit(None))
            )
        
        return df
    
    @staticmethod
    def extract_first_from_array(df: DataFrame, field_mappings: Dict[str, str]) -> DataFrame:
        """
        Extract first element from array fields (for number_basement, number_ele)
        
        Args:
            df: Input DataFrame
            field_mappings: Dict mapping target field to source array field
        
        Returns:
            DataFrame with extracted values
        """
        for target_field, source_field in field_mappings.items():
            if source_field in df.columns:
                df = df.withColumn(
                    target_field,
                    when(
                        col(source_field).isNotNull() & (size(col(source_field)) > 0),
                        element_at(col(source_field), 1).cast(IntegerType())
                    ).otherwise(lit(None))
                )
        
        return df
    
    @staticmethod
    def extract_nested_translation(df: DataFrame, field_mappings: dict) -> DataFrame:
        """
        Extract translation fields from Meeyproject API nested structures
        Example: ward.translation[0].name → ward
        
        Args:
            df: Input DataFrame
            field_mappings: Dict mapping target field to nested path
        
        Returns:
            DataFrame with extracted translation values
        """
        for target_field, nested_path in field_mappings.items():
            # Parse nested path like "ward.translation[0].name"
            if "." in nested_path:
                parts = nested_path.split(".")
                base_field = parts[0]
                
                if base_field in df.columns:
                    # Check if base_field is a complex type (struct/map/array)
                    field_type = str(df.schema[base_field].dataType)
                    
                    # Only extract if it's a complex type, otherwise skip
                    if "StructType" in field_type or "MapType" in field_type or "ArrayType" in field_type:
                        try:
                            # Build nested access expression
                            if "[0]" in nested_path:
                                # Array access: ward.translation[0].name
                                df = df.withColumn(
                                    target_field,
                                    when(
                                        col(base_field).isNotNull(),
                                        col(f"{base_field}.translation").getItem(0).getField("name")
                                    ).otherwise(lit(None))
                                )
                            else:
                                # Simple nested: investorRelated.investor.name
                                df = df.withColumn(
                                    target_field,
                                    when(
                                        col(base_field).isNotNull(),
                                        col(nested_path)
                                    ).otherwise(lit(None))
                                )
                        except Exception:
                            # If extraction fails, skip this field
                            pass
        
        return df
    
    @staticmethod
    def split_geo_coordinates(df: DataFrame, geo_column: str = "geo") -> DataFrame:
        """
        Split Chotot API geo field (format: "lat,lng") into separate columns
        
        Args:
            df: Input DataFrame
            geo_column: Column containing "lat,lng" string
        
        Returns:
            DataFrame with latitude and longitude columns
        """
        if geo_column in df.columns:
            df = df.withColumn(
                "latitude",
                when(
                    col(geo_column).isNotNull() & col(geo_column).contains(","),
                    split(col(geo_column), ",").getItem(0).cast(DoubleType())
                ).otherwise(lit(None))
            ).withColumn(
                "longitude",
                when(
                    col(geo_column).isNotNull() & col(geo_column).contains(","),
                    split(col(geo_column), ",").getItem(1).cast(DoubleType())
                ).otherwise(lit(None))
            )
        
        return df
    
    @staticmethod
    def extract_meeyproject_location(df: DataFrame) -> DataFrame:
        """
        Extract location coordinates from Meeyproject API location.coordinates array
        Format: [longitude, latitude]
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with extracted coordinates
        """
        if "location" in df.columns:
            df = df.withColumn(
                "longitude",
                when(
                    col("location.coordinates").isNotNull() & (size(col("location.coordinates")) >= 2),
                    element_at(col("location.coordinates"), 1).cast(DoubleType())
                ).otherwise(lit(None))
            ).withColumn(
                "latitude",
                when(
                    col("location.coordinates").isNotNull() & (size(col("location.coordinates")) >= 2),
                    element_at(col("location.coordinates"), 2).cast(DoubleType())
                ).otherwise(lit(None))
            )
        
        return df


# Singleton instance for easy import
transformer = DataTransformer()