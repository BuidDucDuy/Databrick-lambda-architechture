"""
Speed Layer - Gold: Real-time analytics and aggregations
Creates real-time dashboards and metrics from event stream
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    window, approx_percentile, max as spark_max,
    date_format, hour, minute
)
from datetime import datetime

def get_spark():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("EventsGold") \
        .getOrCreate()

def create_event_metrics(
    spark: SparkSession,
    source_table: str = 'events_silver',
    target_table: str = 'event_metrics_by_time'
) -> DataFrame:
    """
    Create real-time event metrics aggregated by time windows.
    Tracks event counts, user engagement, and conversion metrics.
    
    Args:
        spark: Spark session
        source_table: Source Silver streaming table
        target_table: Target Gold table for metrics
    
    Returns:
        Streaming DataFrame with metrics
    """
    print(f"🔄 Creating real-time event metrics")
    
    df = spark.readStream.table(source_table)
    
    # Create metrics aggregated by 5-minute windows
    metrics = df.groupBy(
        window(col("event_time"), "5 minutes", "1 minute")
    ).agg(
        count("*").alias("total_events"),
        countDistinct("visitor_id").alias("unique_visitors"),
        countDistinct("item_id").alias("unique_items"),
        countDistinct("transaction_id").alias("transactions"),
        spark_sum(
            col("event_type") == "purchase"
        ).alias("purchases"),
        approx_percentile("ingestion_latency_seconds", 0.5).alias("median_latency_seconds")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "*"
    ).drop("window")
    
    print(f"✅ Event metrics defined")
    
    # Write to Gold table
    query = metrics.writeStream \
        .format("delta") \
        .mode("append") \
        .option("checkpointLocation", "/mnt/dbfs/checkpoints/event_metrics") \
        .option("mergeSchema", "true") \
        .table(target_table)
    
    print(f"💾 Streaming write to {target_table}")
    
    return query

def create_visitor_behavior(
    spark: SparkSession,
    source_table: str = 'events_silver',
    target_table: str = 'visitor_behavior_real_time'
) -> DataFrame:
    """
    Create real-time visitor behavior analysis.
    Tracks engagement metrics per visitor over time windows.
    
    Args:
        spark: Spark session
        source_table: Source Silver streaming table
        target_table: Target Gold table
    
    Returns:
        Streaming DataFrame
    """
    print(f"🔄 Creating visitor behavior analytics")
    
    df = spark.readStream.table(source_table)
    
    behavior = df.groupBy(
        window(col("event_time"), "10 minutes"),
        "visitor_id"
    ).agg(
        count("*").alias("event_count"),
        countDistinct("item_id").alias("items_viewed"),
        spark_sum(
            (col("event_type") == "purchase").cast("int")
        ).alias("purchases"),
        spark_sum(
            (col("event_type") == "view").cast("int")
        ).alias("views"),
        approx_percentile("ingestion_latency_seconds", 0.5).alias("avg_latency_ms")
    ).select(
        col("window.start").alias("window_start"),
        col("visitor_id"),
        "event_count",
        "items_viewed",
        "purchases",
        "views",
        "avg_latency_ms"
    )
    
    # Write to Gold table
    query = behavior.writeStream \
        .format("delta") \
        .mode("append") \
        .option("checkpointLocation", "/mnt/dbfs/checkpoints/visitor_behavior") \
        .option("mergeSchema", "true") \
        .table(target_table)
    
    print(f"💾 Streaming write to {target_table}")
    
    return query

def create_item_popularity(
    spark: SparkSession,
    source_table: str = 'events_silver',
    target_table: str = 'item_popularity_real_time'
) -> DataFrame:
    """
    Create real-time item popularity ranking.
    Shows trending items based on view/purchase counts.
    
    Args:
        spark: Spark session
        source_table: Source Silver streaming table
        target_table: Target Gold table
    
    Returns:
        Streaming DataFrame
    """
    print(f"🔄 Creating item popularity ranking")
    
    df = spark.readStream.table(source_table)
    
    popularity = df.groupBy(
        window(col("event_time"), "15 minutes"),
        "item_id"
    ).agg(
        count("*").alias("total_interactions"),
        spark_sum(
            (col("event_type") == "view").cast("int")
        ).alias("total_views"),
        spark_sum(
            (col("event_type") == "purchase").cast("int")
        ).alias("total_purchases"),
        countDistinct("visitor_id").alias("unique_visitors")
    ).select(
        col("window.start").alias("window_start"),
        "item_id",
        "total_interactions",
        "total_views",
        "total_purchases",
        "unique_visitors"
    )
    
    # Write to Gold table
    query = popularity.writeStream \
        .format("delta") \
        .mode("append") \
        .option("checkpointLocation", "/mnt/dbfs/checkpoints/item_popularity") \
        .option("mergeSchema", "true") \
        .table(target_table)
    
    print(f"💾 Streaming write to {target_table}")
    
    return query

if __name__ == "__main__":
    spark = get_spark()
    
    # Start all streaming jobs
    queries = []
    
    queries.append(create_event_metrics(
        spark=spark,
        source_table="default.events_silver",
        target_table="default.event_metrics_by_time"
    ))
    
    queries.append(create_visitor_behavior(
        spark=spark,
        source_table="default.events_silver",
        target_table="default.visitor_behavior_real_time"
    ))
    
    queries.append(create_item_popularity(
        spark=spark,
        source_table="default.events_silver",
        target_table="default.item_popularity_real_time"
    ))
    
    print(f"\n✅ All streaming queries started")
    print(f"   Active streams: {len(queries)}")
    
    # Keep all streams running
    spark.streams.awaitAnyTermination()
