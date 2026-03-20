"""
Speed Layer - Gold (tối giản và chú thích tiếng Việt)

Thay đổi trong file này (chỉ sửa trong `src/speed_layer`):
- Rút gọn các hàm aggregation, thêm chú thích tiếng Việt.
- Giữ cấu trúc streaming write đơn giản (append + checkpoint).

Mục đích: file này chứa các hàm để tạo các bảng Gold realtime (metrics, visitor behavior, item popularity).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, window, expr


def get_spark():
    """Khởi tạo Spark session (dùng trên Databricks)."""
    return SparkSession.builder.appName("EventsGold").getOrCreate()


def create_event_metrics(spark: SparkSession, source_table: str = 'events_silver', target_table: str = 'event_metrics_by_time') -> DataFrame:
    """Tạo các metrics theo cửa sổ thời gian (5 phút).

    Ghi chú bằng tiếng Việt: hàm này gom các chỉ số cơ bản để demo dashboard thời gian thực.
    """
    print("🔄 Creating real-time event metrics")

    df = spark.readStream.table(source_table)

    metrics = df.groupBy(window(col("event_time"), "5 minutes"))\
        .agg(
            count("*").alias("total_events"),
            countDistinct("visitor_id").alias("unique_visitors"),
            countDistinct("item_id").alias("unique_items"),
            spark_sum((col("event_type") == 'purchase').cast('int')).alias('purchases'),
            expr("percentile_approx(ingestion_latency_seconds, 0.5)").alias('median_latency_seconds')
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_events", "unique_visitors", "unique_items", "purchases", "median_latency_seconds"
        )

    print("✅ Event metrics defined")

    query = metrics.writeStream.format("delta").outputMode("append")\
        .option("checkpointLocation", "/mnt/dbfs/checkpoints/event_metrics")\
        .option("mergeSchema", "true").table(target_table)

    print(f"💾 Streaming write to {target_table}")
    return query


def create_visitor_behavior(spark: SparkSession, source_table: str = 'events_silver', target_table: str = 'visitor_behavior_real_time') -> DataFrame:
    """Phân tích hành vi visitor theo cửa sổ 10 phút.

    Ghi chú: đếm sự kiện, lượt mua, lượt xem cho mỗi visitor.
    """
    print("🔄 Creating visitor behavior analytics")
    df = spark.readStream.table(source_table)

    behavior = df.groupBy(window(col("event_time"), "10 minutes"), col("visitor_id")).agg(
        count("*").alias("event_count"),
        countDistinct("item_id").alias("items_viewed"),
        spark_sum((col("event_type") == 'purchase').cast('int')).alias('purchases'),
        spark_sum((col("event_type") == 'view').cast('int')).alias('views'),
        expr("percentile_approx(ingestion_latency_seconds, 0.5)").alias('median_latency_seconds')
    ).select(col("window.start").alias("window_start"), col("visitor_id"), "event_count", "items_viewed", "purchases", "views", "median_latency_seconds")

    query = behavior.writeStream.format("delta").outputMode("append")\
        .option("checkpointLocation", "/mnt/dbfs/checkpoints/visitor_behavior")\
        .option("mergeSchema", "true").table(target_table)

    print(f"💾 Streaming write to {target_table}")
    return query


def create_item_popularity(spark: SparkSession, source_table: str = 'events_silver', target_table: str = 'item_popularity_real_time') -> DataFrame:
    """Xác định item trending theo cửa sổ 15 phút (views/purchases).
    """
    print("🔄 Creating item popularity ranking")
    df = spark.readStream.table(source_table)

    popularity = df.groupBy(window(col("event_time"), "15 minutes"), col("item_id")).agg(
        count("*").alias("total_interactions"),
        spark_sum((col("event_type") == 'view').cast('int')).alias('total_views'),
        spark_sum((col("event_type") == 'purchase').cast('int')).alias('total_purchases'),
        countDistinct("visitor_id").alias("unique_visitors")
    ).select(col("window.start").alias("window_start"), "item_id", "total_interactions", "total_views", "total_purchases", "unique_visitors")

    query = popularity.writeStream.format("delta").outputMode("append")\
        .option("checkpointLocation", "/mnt/dbfs/checkpoints/item_popularity")\
        .option("mergeSchema", "true").table(target_table)

    print(f"💾 Streaming write to {target_table}")
    return query


if __name__ == "__main__":
    spark = get_spark()
    queries = []

    queries.append(create_event_metrics(spark=spark, source_table="default.events_silver", target_table="default.event_metrics_by_time"))
    queries.append(create_visitor_behavior(spark=spark, source_table="default.events_silver", target_table="default.visitor_behavior_real_time"))
    queries.append(create_item_popularity(spark=spark, source_table="default.events_silver", target_table="default.item_popularity_real_time"))

    print(f"\n✅ All streaming queries started — active: {len(queries)}")
    spark.streams.awaitAnyTermination()
