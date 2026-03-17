"""
Batch Layer - Bronze: Load item properties from S3 using Autoloader
Uses Databricks Autoloader to continuously monitor S3 for new CSV files
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from datetime import datetime

def get_spark():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("ItemPropertiesBronze") \
        .getOrCreate()

def load_item_properties_from_s3(
    spark: SparkSession,
    s3_bucket: str,
    s3_prefix: str = 'batch/item_properties',
    checkpoint_location: str = '/mnt/dbfs/checkpoints/item_properties_bronze',
    target_table: str = 'item_properties_bronze'
) -> DataFrame:
    """
    Load item properties from S3 using Autoloader.
    Autoloader automatically processes new files as they arrive.
    
    Args:
        spark: Spark session
        s3_bucket: Target S3 bucket name
        s3_prefix: S3 prefix/path to monitor
        checkpoint_location: Checkpoint path for tracking processed files
        target_table: Target table name in Databricks
    
    Returns:
        DataFrame with loaded data
    """
    s3_path = f"s3a://{s3_bucket}/{s3_prefix}/"
    
    # Define schema for item properties
    schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("itemid", IntegerType(), True),
        StructField("property", IntegerType(), True),
        StructField("value", StringType(), True)
    ])
    
    print(f"🔄 Loading item properties from {s3_path}")
    
    # Use Autoloader to incrementally process CSV files
    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation", checkpoint_location) \
        .option("schemaHints", "timestamp long, itemid int, property int, value string") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .load(s3_path)
    
    # Add ingestion metadata
    from pyspark.sql.functions import current_timestamp, input_file_name
    df = df.withColumn("_ingestion_time", current_timestamp()) \
           .withColumn("_source_file", input_file_name())
    
    # Write to Bronze table (append mode for incremental loading)
    query = df.writeStream \
        .format("delta") \
        .mode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .table(target_table)
    
    print(f"✅ Bronze layer initialized for {target_table}")
    print(f"   S3 Location: {s3_path}")
    print(f"   Checkpoint: {checkpoint_location}")
    
    return query

if __name__ == "__main__":
    spark = get_spark()
    
    # Configure Databricks environment variables
    s3_bucket = spark.conf.get("spark.databricks.s3.bucket", "my-databricks-bucket")
    
    query = load_item_properties_from_s3(
        spark=spark,
        s3_bucket=s3_bucket,
        target_table="default.item_properties_bronze"
    )
    
    # Keep the stream running
    query.awaitTermination()
