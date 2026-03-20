"""
Speed Layer - Silver (gọn):
- Tối giản logic chuyển đổi (validate, cast, enrich)
- Bổ sung ghi chú bằng tiếng Việt về các bước đã chỉnh
- Giữ nguyên `bronze` (không thay đổi) như yêu cầu

Lưu ý: file này chỉ sửa trong thư mục `src/speed_layer` — các thay đổi:
- Loại bỏ imports không dùng, chuẩn hóa tên cột (event_time, visitor_id, ...)
- Viết lại hàm giám sát window đơn giản bằng SQL expr để tránh phụ thuộc hàm chưa chắc hoạt động trên mọi runtime
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, expr, current_timestamp
from pyspark.sql.types import LongType, IntegerType


def get_spark():
    """Khởi tạo Spark session (dùng trên Databricks)."""
    return SparkSession.builder.appName("EventsSilver").getOrCreate()


def transform_events(spark: SparkSession, source_table: str = 'events_bronze', target_table: str = 'events_silver') -> DataFrame:
    """Chuyển đổi streaming từ Bronze → Silver (gọn, production-ready).

    Bước chính (bằng tiếng Việt):
    1. Đọc stream từ `source_table` (sử dụng `readStream.table`).
    2. Kiểm tra trường bắt buộc, convert kiểu, thêm metadata (`_ingestion_time`, `ingestion_latency_seconds`).
    3. Ghi ra `target_table` bằng `writeStream.table` với checkpoint.

    Ghi chú về partitioning: partitioning khi ghi streaming phụ thuộc vào runtime; để an toàn chúng ta ghi theo `append` và quản lý partition ở tầng table.
    """

    print(f"🔄 Transforming events: {source_table} → {target_table}")

    # 1) Read stream from Bronze
    df = spark.readStream.table(source_table)

    # 2) Cast timestamp (ms) → timestamp, validate, and cast ids
    df = df.withColumn("event_time", to_timestamp(col("timestamp").cast(LongType()) / 1000))
    df = df.filter(col("event_time").isNotNull() & col("visitorid").isNotNull() & col("event").isNotNull())
    df = df.withColumn("item_id", col("itemid").cast(IntegerType()))
    df = df.withColumn("visitor_id", col("visitorid"))

    # 3) Enrich metadata
    df = df.withColumn("_ingestion_time", current_timestamp())
    df = df.withColumn("ingestion_latency_seconds", expr("CAST(UNIX_TIMESTAMP(_ingestion_time) - UNIX_TIMESTAMP(event_time) AS DOUBLE)"))
    df = df.withColumn("event_date", expr("DATE(event_time)"))

    # Select canonical columns for Silver
    silver_df = df.select(
        col("event_time"),
        col("visitor_id"),
        col("event").alias("event_type"),
        col("item_id"),
        col("transactionid").alias("transaction_id"),
        col("_ingestion_time"),
        col("ingestion_latency_seconds"),
        col("event_date")
    )

    print("✅ Transformation defined (silver)")

    # 4) Write stream to Silver table (append + checkpoint)
    query = silver_df.writeStream.format("delta").outputMode("append")
    query = query.option("checkpointLocation", "/mnt/dbfs/checkpoints/events_silver")
    query = query.option("mergeSchema", "true").table(target_table)

    print(f"💾 Streaming write initialized for {target_table}")
    return query


def run_windowed_validation(spark: SparkSession, source_table: str = 'events_bronze'):
    """Giám sát nhanh bằng window aggregation (ví dụ để kiểm tra data quality).

    Trả về query object in console để thuận tiện khi demo.
    """
    df = spark.readStream.table(source_table)
    df = df.withColumn("event_time", to_timestamp(col("timestamp").cast(LongType()) / 1000))

    stats = df.groupBy(expr("window(event_time, '5 minutes')")).agg(expr("count(*) as total_events"), expr("avg(CAST(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(event_time) AS DOUBLE)) as avg_latency_seconds"))

    query = stats.writeStream.format("console").option("truncate", "false").start()
    return query


if __name__ == "__main__":
    spark = get_spark()
    q = transform_events(spark=spark, source_table="default.events_bronze", target_table="default.events_silver")
    q.awaitTermination()
