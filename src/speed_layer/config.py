"""
Databricks Configuration: Speed Layer
Environment setup and table initialization for streaming
"""
# Define database and table names
DATABASE = "default"

# Speed tables
BRONZE_TABLE = "events_bronze"
SILVER_TABLE = "events_silver"
GOLD_METRICS_TABLE = "event_metrics_by_time"
GOLD_VISITOR_TABLE = "visitor_behavior_real_time"
GOLD_POPULARITY_TABLE = "item_popularity_real_time"

# Kinesis configuration
KINESIS_STREAM = "events-stream"
AWS_REGION = "us-east-1"

# Checkpoint locations
CHECKPOINT_BRONZE = "/mnt/dbfs/checkpoints/events_bronze"
CHECKPOINT_SILVER = "/mnt/dbfs/checkpoints/events_silver"
CHECKPOINT_METRICS = "/mnt/dbfs/checkpoints/event_metrics"
CHECKPOINT_VISITOR = "/mnt/dbfs/checkpoints/visitor_behavior"
CHECKPOINT_POPULARITY = "/mnt/dbfs/checkpoints/item_popularity"

# S3 locations for checkpoints
S3_BUCKET = "databricks-streaming-data"
S3_PREFIX_SPEED = "speed/events"

# Initialization SQL
INIT_SQL = f"""
-- Create database if not exists
CREATE DATABASE IF NOT EXISTS {DATABASE};

-- Create Bronze table (Delta, for Kinesis streaming)
CREATE TABLE IF NOT EXISTS {DATABASE}.{BRONZE_TABLE} (
    timestamp STRING,
    visitorid STRING,
    event STRING,
    itemid STRING,
    transactionid STRING,
    _ingestion_id STRING,
    _ingestion_timestamp STRING,
    kinesis_timestamp TIMESTAMP
)
USING DELTA
LOCATION 's3a://{S3_BUCKET}/delta/{BRONZE_TABLE}';

-- Create Silver table (Delta, partitioned by date)
CREATE TABLE IF NOT EXISTS {DATABASE}.{SILVER_TABLE} (
    event_time TIMESTAMP,
    visitor_id STRING,
    event_type STRING,
    item_id INT,
    transaction_id STRING,
    _ingestion_id STRING,
    _ingestion_timestamp STRING,
    processing_time TIMESTAMP,
    event_date DATE,
    event_hour INT,
    ingestion_latency_seconds INT
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3a://{S3_BUCKET}/delta/{SILVER_TABLE}';

-- Create Gold metrics table (5-minute windows)
CREATE TABLE IF NOT EXISTS {DATABASE}.{GOLD_METRICS_TABLE} (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_events LONG,
    unique_visitors LONG,
    unique_items LONG,
    transactions LONG,
    purchases LONG,
    median_latency_seconds DOUBLE
)
USING DELTA
LOCATION 's3a://{S3_BUCKET}/delta/{GOLD_METRICS_TABLE}';

-- Create Gold visitor behavior table
CREATE TABLE IF NOT EXISTS {DATABASE}.{GOLD_VISITOR_TABLE} (
    window_start TIMESTAMP,
    visitor_id STRING,
    event_count LONG,
    items_viewed LONG,
    purchases LONG,
    views LONG,
    avg_latency_ms DOUBLE
)
USING DELTA
LOCATION 's3a://{S3_BUCKET}/delta/{GOLD_VISITOR_TABLE}';

-- Create Gold popularity table
CREATE TABLE IF NOT EXISTS {DATABASE}.{GOLD_POPULARITY_TABLE} (
    window_start TIMESTAMP,
    item_id INT,
    total_interactions LONG,
    total_views LONG,
    total_purchases LONG,
    unique_visitors LONG
)
USING DELTA
LOCATION 's3a://{S3_BUCKET}/delta/{GOLD_POPULARITY_TABLE}';
"""
