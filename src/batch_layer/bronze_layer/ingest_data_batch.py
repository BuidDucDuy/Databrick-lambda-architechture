from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType
)
from pyspark.sql.functions import current_timestamp, lit

# Tạo Spark session
spark = SparkSession.builder.getOrCreate()

# Định nghĩa schema
schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("itemid", IntegerType(), True),
    StructField("property", IntegerType(), True),
    StructField("value", StringType(), True)
])

# Path dữ liệu và checkpoints
bronze_path = "s3://databricks-batch-data-demo/raw/"
checkpoint_base = "s3://databricks-batch-data-demo/_checkpoints/bronze_ingestion/"

# Đọc file CSV từ S3 bằng Auto Loader (cloudFiles)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_base + "schema")
    .option("header", "true")
    .schema(schema)   # schema tĩnh
    .load(bronze_path)
)

# Thêm các cột metadata
df = df.select(
    "*",
    current_timestamp().alias("_ingestion_time"),
    lit("manual_csv").alias("_source_file")
)

# Ghi dữ liệu vào Delta table theo kiểu batch
query = (
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint_base + "data")
      .option("mergeSchema", "true")
      .trigger(availableNow=True)    # dùng availableNow để chạy batch ingestion một lần
      .toTable("dev.bronze.batch_bronze")
)

query.awaitTermination()
