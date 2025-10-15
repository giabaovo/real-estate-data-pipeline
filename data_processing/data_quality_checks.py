"""
Data Quality Checks Module
Provides reusable validation functions for Silver Layer ETL
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, countDistinct
from typing import Dict, List, Tuple

class DataQualityChecker:
    """Class to perform data quality checks on DataFrames"""
    
    def __init__(self, df: DataFrame):
        self.df = df
        self.results = {}
    
    def check_null_percentage(self, columns: List[str], threshold: float = 0.5) -> Dict:
        """
        Check null percentage for specified columns
        
        Args:
            columns: List of column names to check
            threshold: Maximum acceptable null percentage (0.0 to 1.0)
        
        Returns:
            Dictionary with column names and their null percentages
        """
        total_count = self.df.count()
        null_stats = {}
        
        for col_name in columns:
            if col_name in self.df.columns:
                null_count = self.df.filter(col(col_name).isNull()).count()
                null_percentage = null_count / total_count if total_count > 0 else 0
                
                null_stats[col_name] = {
                    "null_count": null_count,
                    "null_percentage": null_percentage,
                    "passed": null_percentage <= threshold
                }
        
        self.results["null_check"] = null_stats
        return null_stats
    
    def check_uniqueness(self, columns: List[str]) -> Dict:
        """
        Check uniqueness constraint for specified columns
        
        Args:
            columns: List of column names that should be unique
        
        Returns:
            Dictionary with uniqueness statistics
        """
        uniqueness_stats = {}
        
        for col_name in columns:
            if col_name in self.df.columns:
                total_count = self.df.count()
                distinct_count = self.df.select(col_name).distinct().count()
                duplicate_count = total_count - distinct_count
                
                uniqueness_stats[col_name] = {
                    "total_count": total_count,
                    "distinct_count": distinct_count,
                    "duplicate_count": duplicate_count,
                    "is_unique": duplicate_count == 0
                }
        
        self.results["uniqueness_check"] = uniqueness_stats
        return uniqueness_stats
    
    def check_value_range(self, column: str, min_val: float, max_val: float) -> Dict:
        """
        Check if numeric values are within acceptable range
        
        Args:
            column: Column name to check
            min_val: Minimum acceptable value
            max_val: Maximum acceptable value
        
        Returns:
            Dictionary with range check results
        """
        if column not in self.df.columns:
            return {"error": f"Column {column} not found"}
        
        out_of_range = self.df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        
        total_count = self.df.count()
        
        range_stats = {
            "column": column,
            "min_expected": min_val,
            "max_expected": max_val,
            "out_of_range_count": out_of_range,
            "out_of_range_percentage": out_of_range / total_count if total_count > 0 else 0,
            "passed": out_of_range == 0
        }
        
        self.results[f"range_check_{column}"] = range_stats
        return range_stats
    
    def check_data_freshness(self, timestamp_column: str, max_age_hours: int = 24) -> Dict:
        """
        Check data freshness based on timestamp column
        
        Args:
            timestamp_column: Column containing timestamps
            max_age_hours: Maximum acceptable age in hours
        
        Returns:
            Dictionary with freshness statistics
        """
        from pyspark.sql.functions import current_timestamp, unix_timestamp
        
        if timestamp_column not in self.df.columns:
            return {"error": f"Column {timestamp_column} not found"}
        
        df_with_age = self.df.withColumn(
            "age_hours",
            (unix_timestamp(current_timestamp()) - 
             unix_timestamp(col(timestamp_column))) / 3600
        )
        
        stale_count = df_with_age.filter(col("age_hours") > max_age_hours).count()
        total_count = self.df.count()
        
        freshness_stats = {
            "timestamp_column": timestamp_column,
            "max_age_hours": max_age_hours,
            "stale_records": stale_count,
            "stale_percentage": stale_count / total_count if total_count > 0 else 0,
            "passed": stale_count == 0
        }
        
        self.results["freshness_check"] = freshness_stats
        return freshness_stats
    
    def check_referential_integrity(self, 
                                   column: str, 
                                   reference_df: DataFrame, 
                                   reference_column: str) -> Dict:
        """
        Check referential integrity between two DataFrames
        
        Args:
            column: Column in current DataFrame
            reference_df: Reference DataFrame
            reference_column: Column in reference DataFrame
        
        Returns:
            Dictionary with referential integrity results
        """
        # Left anti join to find orphan records
        orphan_records = self.df.join(
            reference_df,
            self.df[column] == reference_df[reference_column],
            "left_anti"
        ).count()
        
        total_count = self.df.count()
        
        integrity_stats = {
            "column": column,
            "reference_column": reference_column,
            "orphan_records": orphan_records,
            "orphan_percentage": orphan_records / total_count if total_count > 0 else 0,
            "passed": orphan_records == 0
        }
        
        self.results["referential_integrity_check"] = integrity_stats
        return integrity_stats
    
    def check_format(self, column: str, regex_pattern: str) -> Dict:
        """
        Check if values match expected format using regex
        
        Args:
            column: Column name to check
            regex_pattern: Regex pattern for validation
        
        Returns:
            Dictionary with format check results
        """
        from pyspark.sql.functions import regexp_extract
        
        if column not in self.df.columns:
            return {"error": f"Column {column} not found"}
        
        invalid_count = self.df.filter(
            ~col(column).rlike(regex_pattern) & col(column).isNotNull()
        ).count()
        
        total_count = self.df.filter(col(column).isNotNull()).count()
        
        format_stats = {
            "column": column,
            "pattern": regex_pattern,
            "invalid_format_count": invalid_count,
            "invalid_percentage": invalid_count / total_count if total_count > 0 else 0,
            "passed": invalid_count == 0
        }
        
        self.results[f"format_check_{column}"] = format_stats
        return format_stats
    
    def check_completeness(self, required_columns: List[str]) -> Dict:
        """
        Check overall data completeness
        
        Args:
            required_columns: List of columns that must have values
        
        Returns:
            Dictionary with completeness statistics
        """
        total_rows = self.df.count()
        complete_rows = self.df.dropna(subset=required_columns).count()
        incomplete_rows = total_rows - complete_rows
        
        completeness_stats = {
            "required_columns": required_columns,
            "total_rows": total_rows,
            "complete_rows": complete_rows,
            "incomplete_rows": incomplete_rows,
            "completeness_percentage": complete_rows / total_rows if total_rows > 0 else 0,
            "passed": incomplete_rows == 0
        }
        
        self.results["completeness_check"] = completeness_stats
        return completeness_stats
    
    def generate_report(self) -> str:
        """Generate a summary report of all quality checks"""
        report_lines = [
            "=" * 70,
            "DATA QUALITY REPORT",
            "=" * 70,
            ""
        ]
        
        for check_name, check_results in self.results.items():
            report_lines.append(f"\n{check_name.upper().replace('_', ' ')}:")
            report_lines.append("-" * 70)
            
            if isinstance(check_results, dict):
                for key, value in check_results.items():
                    if isinstance(value, dict):
                        report_lines.append(f"\n  {key}:")
                        for sub_key, sub_value in value.items():
                            report_lines.append(f"    {sub_key}: {sub_value}")
                    else:
                        report_lines.append(f"  {key}: {value}")
            
            report_lines.append("")
        
        report_lines.extend([
            "=" * 70,
            "END OF REPORT",
            "=" * 70
        ])
        
        return "\n".join(report_lines)
    
    def get_failed_checks(self) -> List[str]:
        """Return list of failed check names"""
        failed = []
        for check_name, check_results in self.results.items():
            if isinstance(check_results, dict):
                if "passed" in check_results and not check_results["passed"]:
                    failed.append(check_name)
                elif any(isinstance(v, dict) and "passed" in v and not v["passed"] 
                        for v in check_results.values()):
                    failed.append(check_name)
        return failed


# Predefined quality check configurations for real estate data
REAL_ESTATE_QUALITY_CHECKS = {
    "required_fields": [
        "spider_name",
        "timestamp",
        "source_id",
        "project_name"
    ],
    
    # Valuation-critical fields
    "valuation_fields": [
        "latitude",
        "longitude",
        "total_area",
        "district",
        "city"
    ],
    
    # Pricing fields (at least one should exist)
    "pricing_fields": [
        "min_selling_price",
        "max_selling_price",
        "min_unit_price",
        "max_unit_price"
    ],
    
    "email_pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    
    "phone_pattern": r'^0\d{9,10}$',
    
    "null_threshold": 0.3,  # 30% max null values
    
    "unique_fields": [
        "universal_id"
    ],
    
    # Coordinate ranges
    "latitude_range": (-90, 90),
    "longitude_range": (-180, 180),
    
    # Vietnam specific ranges
    "vietnam_latitude_range": (8.0, 24.0),
    "vietnam_longitude_range": (102.0, 110.0),
    
    # Price ranges (VND)
    "min_reasonable_price": 100000000,  # 100 million VND
    "max_reasonable_price": 100000000000,  # 100 billion VND
    
    # Area ranges (m²)
    "min_reasonable_area": 10,
    "max_reasonable_area": 1000000
}