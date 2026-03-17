"""
Databricks Configuration: Batch Layer
Environment setup and table initialization
"""
# Define database and table names
DATABASE = "default"

# Batch tables
BRONZE_TABLE = "item_properties_bronze"
SILVER_TABLE = "item_properties_silver"
GOLD_SUMMARY_TABLE = "item_properties_summary"
GOLD_STATS_TABLE = "property_statistics"

# S3 locations
S3_BUCKET_BATCH = "databricks-batch-data"
S3_PREFIX_BATCH = "batch/item_properties"
S3_PATH_BATCH = f"s3a://{S3_BUCKET_BATCH}/{S3_PREFIX_BATCH}/"

# Checkpoint locations
CHECKPOINT_BRONZE = "/mnt/dbfs/checkpoints/item_properties_bronze"
CHECKPOINT_SILVER = "/mnt/dbfs/checkpoints/item_properties_silver"

# Initialization SQL
INIT_SQL = f"""
-- Create database if not exists
CREATE DATABASE IF NOT EXISTS {DATABASE};

-- Create Bronze table (Delta)
CREATE TABLE IF NOT EXISTS {DATABASE}.{BRONZE_TABLE} (
    timestamp LONG,
    itemid INT,
    property INT,
    value STRING,
    _ingestion_time TIMESTAMP,
    _source_file STRING
)
USING DELTA
LOCATION 's3a://{S3_BUCKET_BATCH}/delta/{BRONZE_TABLE}';

-- Create Silver table (Delta, partitioned)
CREATE TABLE IF NOT EXISTS {DATABASE}.{SILVER_TABLE} (
    item_id INT,
    property_id INT,
    property_value STRING,
    timestamp LONG,
    date DATE,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING DELTA
PARTITIONED BY (date)
LOCATION 's3a://{S3_BUCKET_BATCH}/delta/{SILVER_TABLE}';

-- Create Gold summary table
CREATE TABLE IF NOT EXISTS {DATABASE}.{GOLD_SUMMARY_TABLE} (
    item_id INT,
    total_properties INT,
    unique_properties INT,
    last_updated DATE,
    property_ids ARRAY<INT>,
    property_values ARRAY<STRING>
)
USING DELTA
LOCATION 's3a://{S3_BUCKET_BATCH}/delta/{GOLD_SUMMARY_TABLE}';

-- Create Gold statistics table
CREATE TABLE IF NOT EXISTS {DATABASE}.{GOLD_STATS_TABLE} (
    property_id INT,
    items_with_property INT,
    total_records INT,
    unique_values INT
)
USING DELTA
LOCATION 's3a://{S3_BUCKET_BATCH}/delta/{GOLD_STATS_TABLE}';
"""
