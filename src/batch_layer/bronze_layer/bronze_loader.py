"""
Batch Layer - Bronze Loader
Production job for ingesting raw data into Bronze table via Auto Loader.

Usage:
  python bronze_loader.py --source-path s3://bucket/raw --table dev.bronze.batch_item_properties
  
  Or directly in Databricks Jobs with fixed config.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_schema():
    """Define the schema for batch data."""
    return StructType([
        StructField("timestamp", LongType(), True),
        StructField("itemid", IntegerType(), True),
        StructField("property", IntegerType(), True),
        StructField("value", StringType(), True)
    ])


def ingest_bronze(spark, source_path, checkpoint_path, target_table):
    """
    Ingest raw data into Bronze table using Auto Loader.
    
    Args:
        spark: SparkSession
        source_path: S3 path to CSV files
        checkpoint_path: Checkpoint location for schema and state
        target_table: Target Delta table name
    """
    logger.info(f"Starting Bronze ingestion from {source_path}")
    
    schema = create_schema()
    
    # Read with Auto Loader (cloudFiles)
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}schema")
        .option("header", "true")
        .schema(schema)
        .load(source_path)
    )
    
    # Add metadata
    df = df.select(
        "*",
        current_timestamp().alias("_ingestion_time"),
        lit("s3_csv").alias("_source_file"),
        lit(datetime.now().isoformat()).alias("_load_id")
    )
    
    # Write to Delta (batch mode with availableNow)
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    
    logger.info(f"✅ Data ingested into {target_table}")
    
    # Return stats
    result = spark.sql(f"SELECT COUNT(*) as count FROM {target_table}")
    count = result.collect()[0][0]
    logger.info(f"Total records in {target_table}: {count}")
    
    return count


def main():
    """Main entry point for Databricks job."""
    spark = SparkSession.builder.getOrCreate()
    
    # Configuration (override via job parameters)
    source_path = "s3://databricks-batch-data-demo/raw/"
    checkpoint_path = "/Volumes/dev/bronze/checkpoint/_checkpoints/batch_ingestion/"
    target_table = "dev.bronze.batch_item_properties"
    
    try:
        count = ingest_bronze(spark, source_path, checkpoint_path, target_table)
        logger.info(f"✅ Job completed successfully. Ingested {count} records.")
        return 0
    except Exception as e:
        logger.error(f"❌ Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
