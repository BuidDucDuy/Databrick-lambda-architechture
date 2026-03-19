"""
Speed Layer - Silver: Clean and enrich real-time event stream
Applies business logic and data validation to events
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, unix_timestamp, cast,
    when, expr, is_valid_json,
    window, approx_percentile
)
from pyspark.sql.types import LongType, IntegerType

def get_spark():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("EventsSilver") \
        .getOrCreate()

def transform_events(
    spark: SparkSession,
    source_table: str = 'events_bronze',
    target_table: str = 'events_silver'
) -> DataFrame:
    """
    Clean, validate and enrich events from Bronze layer.
    - Convert timestamp from milliseconds to proper format
    - Validate event types
    - Filter invalid records
    - Add calculated fields
    
    Args:
        spark: Spark session
        source_table: Source Bronze streaming table
        target_table: Target Silver streaming table
    
    Returns:
        Transformed streaming DataFrame
    """
    print(f"🔄 Transforming events: {source_table} → {target_table}")
    
    # Read from Bronze (use readStream for continuous)
    df = spark.readStream.table(source_table)
    
    # Convert timestamp from milliseconds to proper datetime
    df_with_time = df.withColumn(
        "event_datetime",
        to_timestamp(col("timestamp").cast(LongType()) / 1000)
    )
    
    # Validate and clean event types
    valid_events = ['view', 'click', 'purchase', 'add_to_cart', 'remove_from_cart']
    df_validated = df_with_time.filter(
        col("event").isin(valid_events) &
        col("itemid").isNotNull() &
        col("visitorid").isNotNull()
    )
    
    # Convert numeric columns
    df_typed = df_validated.select(
        col("event_datetime").alias("event_time"),
        col("visitorid").alias("visitor_id"),
        col("event").alias("event_type"),
        col("itemid").cast(IntegerType()).alias("item_id"),
        col("transactionid").alias("transaction_id"),
        col("_ingestion_id"),
        col("_ingestion_timestamp"),
        col("_processing_time").alias("processing_time")
    )
    
    # Add calculated fields
    df_enriched = df_typed.withColumn(
        "event_date",
        expr("DATE(event_time)")
    ).withColumn(
        "event_hour",
        expr("HOUR(event_time)")
    ).withColumn(
        "ingestion_latency_seconds",
        expr("CAST(UNIX_TIMESTAMP(processing_time) - UNIX_TIMESTAMP(event_time) AS INT)")
    )
    
    print(f"✅ Transformation logic defined")
    
    # Write to Silver table using stream
    query = df_enriched.writeStream \
        .format("delta") \
        .mode("append") \
        .option("checkpointLocation", "/mnt/dbfs/checkpoints/events_silver") \
        .option("mergeSchema", "true") \
        .partitionBy("event_date") \
        .table(target_table)
    
    print(f"💾 Streaming write initialized for {target_table}")
    
    return query

def run_windowed_validation(
    spark: SparkSession,
    source_table: str = 'events_bronze'
) -> None:
    """
    Run windowed analytics to monitor data quality.
    Logs event counts and latency metrics every minute.
    
    Args:
        spark: Spark session
        source_table: Source table to monitor
    """
    print(f"📊 Starting windowed validation on {source_table}")
    
    df = spark.readStream.table(source_table)
    
    validation_stats = df.withColumn(
        "event_datetime",
        to_timestamp(col("timestamp").cast(LongType()) / 1000)
    ).groupBy(
        window(col("event_datetime"), "5 minutes")
    ).agg({
        "event": "count",
        "visitorid": "approx_percentile(*, array(0.5))",  # median
        "_ingestion_id": "count"
    })
    
    query = validation_stats.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    return query

if __name__ == "__main__":
    spark = get_spark()
    
    query = transform_events(
        spark=spark,
        source_table="default.events_bronze",
        target_table="default.events_silver"
    )
    
    # Keep the stream running
    query.awaitTermination()
