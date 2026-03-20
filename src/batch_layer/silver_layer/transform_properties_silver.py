from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    row_number,
    first,
    window,
    max as spark_max,
    to_date,
    unix_timestamp
)
from pyspark.sql.window import Window as SparkWindow
from datetime import datetime

def get_spark():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("ItemPropertiesSilver") \
        .getOrCreate()

def transform_properties(
    spark: SparkSession,
    source_table: str = 'dev.bronze.batch_bronze',
    target_table: str = 'dev.silver.batch_silver'
) -> DataFrame:
    print(f"🔄 Transforming {source_table} → {target_table}")
    
    # Read from Bronze
    df = spark.table(source_table)
    
    # Convert timestamp (milliseconds since epoch) to date
    df = df.withColumn(
        "date",
        to_date((col("timestamp").cast("float") / 1000).cast("timestamp"))
    )
    
    # Deduplicate: keep most recent record per itemid-property combination
    window_spec = SparkWindow.partitionBy("itemid", "property") \
        .orderBy(col("timestamp").desc())
    
    df_deduped = df.withColumn(
        "rn",
        row_number().over(window_spec)
    ).filter(col("rn") == 1).drop("rn")
    
    # Rename columns for clarity
    df_cleaned = df_deduped.select(
        col("itemid").alias("item_id"),
        col("property").alias("property_id"),
        col("value").alias("property_value"),
        col("timestamp"),
        col("date"),
        col("_ingestion_time").alias("ingestion_timestamp"),
        col("_source_file").alias("source_file")
    )
    
    print(f" complete")
    print(f" Records: {df_cleaned.count():,}")
    
    # Write to Silver table
    df_cleaned.write \
        .mode("overwrite") \
        .format("delta") \
        .partitionBy("date") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
        
    print(f"Data written to {target_table}")
    
    return df_cleaned

if __name__ == "__main__":
    spark = get_spark()
    transform_properties(
        spark=spark,
        source_table="dev.bronze.batch_bronze",
        target_table="dev.silver.batch_silver"
    )
