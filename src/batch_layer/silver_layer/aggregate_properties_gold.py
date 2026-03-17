"""
Batch Layer - Gold: Aggregate and analyze item properties
Creates analytics tables with aggregated metrics
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, first, 
    min as spark_min, max as spark_max,
    collect_list, sort_array
)
from datetime import datetime

def get_spark():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("ItemPropertiesGold") \
        .getOrCreate()

def create_properties_summary(
    spark: SparkSession,
    source_table: str = 'item_properties_silver',
    target_table: str = 'item_properties_summary'
) -> DataFrame:
    """
    Create aggregated summary of item properties.
    Groups properties by item and provides statistics.
    
    Args:
        spark: Spark session
        source_table: Source Silver table
        target_table: Target Gold table
    
    Returns:
        Aggregated DataFrame
    """
    print(f"🔄 Creating property summary from {source_table}")
    
    # Read from Silver
    df = spark.table(source_table)
    
    # Aggregate by item_id
    summary = df.groupBy("item_id") \
        .agg(
            count("property_id").alias("total_properties"),
            countDistinct("property_id").alias("unique_properties"),
            first(col("date")).alias("last_updated"),
            collect_list(
                col("property_id")
            ).alias("property_ids"),
            collect_list(
                col("property_value")
            ).alias("property_values")
        ) \
        .orderBy(col("total_properties").desc())
    
    # Write to Gold table
    summary.write \
        .mode("overwrite") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"✅ Summary created: {summary.count():,} unique items")
    print(f"💾 Data written to {target_table}")
    
    return summary

def create_property_statistics(
    spark: SparkSession,
    source_table: str = 'item_properties_silver',
    target_table: str = 'property_statistics'
) -> DataFrame:
    """
    Create statistics by property_id.
    Shows how many items have each property.
    
    Args:
        spark: Spark session
        source_table: Source Silver table
        target_table: Target Gold table
    
    Returns:
        Statistics DataFrame
    """
    print(f"🔄 Creating property statistics")
    
    df = spark.table(source_table)
    
    stats = df.groupBy("property_id") \
        .agg(
            countDistinct("item_id").alias("items_with_property"),
            count("*").alias("total_records"),
            countDistinct("property_value").alias("unique_values")
        ) \
        .orderBy(col("items_with_property").desc())
    
    stats.write \
        .mode("overwrite") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"✅ Property statistics created")
    print(f"💾 Data written to {target_table}")
    
    return stats

if __name__ == "__main__":
    spark = get_spark()
    
    create_properties_summary(
        spark=spark,
        source_table="default.item_properties_silver",
        target_table="default.item_properties_summary"
    )
    
    create_property_statistics(
        spark=spark,
        source_table="default.item_properties_silver",
        target_table="default.property_statistics"
    )
