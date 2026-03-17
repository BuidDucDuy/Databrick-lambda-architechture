"""
Speed Layer - Bronze: Ingest events from Kinesis stream
Real-time ingestion of visitor events for immediate processing
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from datetime import datetime
from pyspark.sql.functions import from_json, col, get_json_object, current_timestamp

def get_spark():
    """Initialize Spark session with Kinesis support"""
    return SparkSession.builder \
        .appName("EventsBronze") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kinesis_2.12:3.2.0") \
        .getOrCreate()

def load_events_from_kinesis(
    spark: SparkSession,
    kinesis_stream: str = 'events-stream',
    region: str = 'us-east-1',
    checkpoint_location: str = '/mnt/dbfs/checkpoints/events_bronze',
    target_table: str = 'events_bronze'
) -> DataFrame:
    """
    Load events from Kinesis stream using structured streaming.
    Provides real-time event ingestion with exactly-once semantics.
    
    Args:
        spark: Spark session
        kinesis_stream: Kinesis stream name
        region: AWS region
        checkpoint_location: Checkpoint path for fault tolerance
        target_table: Target table name in Databricks
    
    Returns:
        Streaming DataFrame
    """
    
    # Define schema for incoming event data
    event_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("visitorid", StringType(), True),
        StructField("event", StringType(), True),
        StructField("itemid", StringType(), True),
        StructField("transactionid", StringType(), True),
        StructField("_ingestion_id", StringType(), True),
        StructField("_ingestion_timestamp", StringType(), True)
    ])
    
    print(f"🔄 Connecting to Kinesis stream: {kinesis_stream}")
    
    # Read from Kinesis
    df_stream = spark.readStream \
        .format("kinesis") \
        .option("streamName", kinesis_stream) \
        .option("region", region) \
        .option("initialPosition", "LATEST") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON data from Kinesis records
    df_parsed = df_stream.select(
        from_json(
            col("data").cast("string"),
            event_schema
        ).alias("event_data"),
        col("kinesis.approximateArrivalTimestamp").alias("kinesis_timestamp")
    ).select(
        col("event_data.*"),
        col("kinesis_timestamp")
    )
    
    # Add processing metadata
    df_enriched = df_parsed.withColumn(
        "_processing_time", current_timestamp()
    )
    
    print(f"✅ Kinesis stream connection established")
    
    # Write to Bronze table (append mode)
    query = df_enriched.writeStream \
        .format("delta") \
        .mode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .option("mergeSchema", "true") \
        .table(target_table)
    
    print(f"✅ Streaming write initialized for {target_table}")
    print(f"   Kinesis: {kinesis_stream}")
    print(f"   Region: {region}")
    print(f"   Checkpoint: {checkpoint_location}")
    
    return query

if __name__ == "__main__":
    spark = get_spark()
    
    # Get configuration from Databricks or environment
    kinesis_stream = spark.conf.get("spark.databricks.kinesis.stream", "events-stream")
    region = spark.conf.get("spark.databricks.aws.region", "us-east-1")
    
    query = load_events_from_kinesis(
        spark=spark,
        kinesis_stream=kinesis_stream,
        region=region,
        target_table="default.events_bronze"
    )
    
    # Keep the stream running
    query.awaitTermination()
