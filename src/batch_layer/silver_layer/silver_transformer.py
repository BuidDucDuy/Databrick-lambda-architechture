"""
Batch Layer - Silver Transformer
Production job for cleaning and deduplicating Bronze data.

Performs:
- Type conversion
- Deduplication (latest record per item-property)
- Date partitioning
- Data quality validation

Usage:
  python silver_transformer.py --source dev.bronze.batch_item_properties --target dev.silver.batch_item_properties
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, to_date, to_timestamp, 
    row_number, year, month, desc
)
from pyspark.sql.window import Window
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_silver(spark, source_table, target_table):

    logger.info(f"Starting Silver transformation from {source_table}")
    
    # Read Bronze
    df = spark.sql(f"SELECT * FROM {source_table}")
    initial_count = df.count()
    logger.info(f"Input records: {initial_count}")
    
    # Data Cleaning
    cleaned_df = df.select(
        col("timestamp").cast("long").alias("timestamp_unix"),
        from_unixtime(col("timestamp") / 1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp").alias("event_time"),
        col("itemid").cast("integer").alias("item_id"),
        col("property").cast("integer").alias("property_id"),
        col("value").cast("string").alias("property_value"),
        col("_ingestion_time").alias("ingestion_timestamp"),
        col("_source_file").alias("source_file")
    ).filter(
        col("item_id").isNotNull() & col("property_id").isNotNull()
    )
    
    # Add date columns
    cleaned_df = cleaned_df.select(
        "*",
        to_date(col("event_time")).alias("event_date"),
        year(col("event_time")).alias("event_year"),
        month(col("event_time")).alias("event_month")
    )
    
    # Deduplication: Keep latest per (item_id, property_id)
    window_spec = Window.partitionBy("item_id", "property_id").orderBy(desc("timestamp_unix"))
    deduped_df = cleaned_df.select(
        "*",
        row_number().over(window_spec).alias("_rn")
    ).filter(col("_rn") == 1).drop("_rn")
    
    dedup_count = deduped_df.count()
    logger.info(f"After deduplication: {dedup_count} records (ratio: {initial_count/dedup_count:.2f}x)")
    
    # Write to Silver (partitioned by date)
    deduped_df.coalesce(4).write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_year", "event_month", "event_date") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    logger.info(f"✅ Data written to {target_table}")
    
    # Quality validation
    null_check = spark.sql(f"""
        SELECT 
            SUM(CASE WHEN item_id IS NULL THEN 1 ELSE 0 END) as null_item_id,
            SUM(CASE WHEN property_id IS NULL THEN 1 ELSE 0 END) as null_property_id
        FROM {target_table}
    """).collect()[0]
    
    if null_check[0] > 0 or null_check[1] > 0:
        logger.warning(f"Data quality issues detected: {null_check}")
    
    return dedup_count


def main():
    """Main entry point."""
    spark = SparkSession.builder.getOrCreate()
    
    source_table = "dev.bronze_batch.batch_item_properties"
    target_table = "dev.silver_batch.batch_silver"
    
    try:
        count = transform_silver(spark, source_table, target_table)
        logger.info(f"✅ Job completed. Transformed {count} records.")
        return 0
    except Exception as e:
        logger.error(f"❌ Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
