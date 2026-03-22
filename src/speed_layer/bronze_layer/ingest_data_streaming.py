from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from pyspark.sql.functions import from_json, col, current_timestamp

# Tạo Spark session
spark = SparkSession.builder.getOrCreate()

# Định nghĩa schema event
event_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("visitorid", StringType(), True),
    StructField("event", StringType(), True),
    StructField("itemid", StringType(), True),
    StructField("transactionid", StringType(), True),
    StructField("_ingestion_id", StringType(), True),
    StructField("_ingestion_timestamp", StringType(), True),
])


KINESIS_STREAM_NAME = "databrick-lambda-architecture-events"   
KINESIS_REGION       = "us-east-1"            
STARTING_POSITION    = "earliest"                     
TARGET_TABLE       = "dev.bronze_stream.bronze_streaming_item_properties" 
CHECKPOINT_LOCATION = "/Volumes/dev/bronze_stream/checkpoint/_checkpoints/bronze_streaming_ingestion/"  
awsAccessKey = "AKIATOZJ4IAR5QSFVP6X"
awsSecretKey = "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"

# Đọc dữ liệu từ Kinesis
df_kinesis = (
    spark.readStream
         .format("kinesis")
         .option("streamName", KINESIS_STREAM_NAME)    
         .option("region", KINESIS_REGION)             
         .option("initialPosition", STARTING_POSITION) 
         .option("awsAccessKey", awsAccessKey)
         .option("awsSecretKey", awsSecretKey)
         .load()
)

# Parse JSON payload từ cột data
df_parsed = (
    df_kinesis.select(
        from_json(col("data").cast("string"), event_schema).alias("event_data"),
        col("approximateArrivalTimestamp").alias("kinesis_timestamp")
    )
    .select(
        col("event_data.*"), 
        col("kinesis_timestamp")
    )
)

# Enrich dữ liệu với processing time
df_enriched = df_parsed.withColumn("_processing_time", current_timestamp())

# Ghi streaming vào Delta table
query = (
    df_enriched.writeStream
               .format("delta")
               .outputMode("append")
               .option("checkpointLocation", CHECKPOINT_LOCATION)
               .option("mergeSchema", "true")
               .trigger(availableNow=True)
               .toTable(TARGET_TABLE)
)

query.awaitTermination()
