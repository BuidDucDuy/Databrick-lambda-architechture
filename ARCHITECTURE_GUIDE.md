# 📐 Architecture Guide

Deep-dive into the technical architecture of the Lambda system.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS LAMBDA ARCHITECTURE                    │
└─────────────────────────────────────────────────────────────────────┘

INPUT SOURCES                  AWS PROCESSING               DATABRICKS LAYER
════════════════════════════════════════════════════════════════════════════

data/batch/                    S3 Bucket                     Autoloader
  item_properties_     ──→    (partitioned)    ────→       ↓
  part2.csv                   YYYY/MM/DD/                BRONZE
                                  ↓                        ↓
                           (auto-detected)           SILVER (cleaned)
                                  ↓                        ↓
                                             GOLD (analytics)
                                            
data/streaming/               S3 Bucket                  Structured Streaming
  events.csv         ──→      (partitioned)    ────→       ↓
                              YYYY/MM/DD/            BRONZE (from Kinesis)
                                  ↓                        ↓
                            Lambda Function          SILVER (transformed)
                            json parse                     ↓
                                  ↓                GOLD (windowed analytics)
                            Kinesis Stream
                            (Sharded by
                             item_id)
```

## Detailed Component Architecture

### 1. Data Ingestion

#### Batch Layer (Item Properties)
```
item_properties_part2.csv (50MB+)
    ↓
[Python: upload_to_s3.py]
    └─ Reads CSV line-by-line
    └─ Uploads to S3 with date partitioning
    └─ Creates: s3://bucket/data/batch/2026/03/17/item_properties_part2.csv
    
        ↓
    S3 Bucket
    └─ Versioning enabled
    └─ Server-side encryption (SSE-S3)
    └─ Lifecycle policy: Archive to GLACIER at 90d, delete at 365d
```

#### Streaming Layer (Events)
```
events.csv (50MB+)
    ↓
[Python: upload_to_s3.py]
    └─ Reads CSV line-by-line
    └─ Uploads to S3 with date partitioning
    └─ Creates: s3://bucket/data/streaming/2026/03/17/events.csv
    
        ↓
    S3 Event Notification
    └─ Triggers on: ObjectCreated:Put, ObjectCreated:Post
    └─ Routes to: Lambda function (s3_streaming_processor)
```

### 2. Lambda Function (s3_streaming_processor.py)

**Trigger:** S3 PutObject event

**Processing:**
```python
def lambda_handler(event, context):
    1. Extract bucket & key from S3 event
    2. Download CSV from S3
    3. Parse CSV into records
    4. For each record:
        a. Extract fields: timestamp, visitorid, event, itemid, transactionid
        b. Validate required fields
        c. Add metadata: _ingestion_id, _ingestion_timestamp, _source_file
        d. Convert to JSON
        e. Put to Kinesis with PartitionKey = itemid
    5. Return summary: {successful: N, failed: M}
```

**Configuration:**
- Memory: 512 MB (configurable)
- Timeout: 60 seconds
- Runtime: Python 3.11
- Permissions:
  - `s3:GetObject` → Streaming bucket
  - `kinesis:PutRecord` → events-stream

### 3. AWS Kinesis Stream

**Name:** `events-stream`

**Configuration:**
- Mode: ON_DEMAND (auto-scaling)
- Retention: 24 hours
- Encryption: Enabled (KMS)
- Shard Count: Auto-scales with on-demand pricing

**Schema:**
```json
{
  "timestamp": 1433221332117,           // milliseconds since epoch
  "visitorid": "user-123",               // unique visitor ID
  "event": "view",                       // event type
  "itemid": "item-456",                  // product/item ID
  "transactionid": "txn-789",            // optional transaction ID
  "_ingestion_id": "uuid4",              // Lambda-generated
  "_ingestion_timestamp": 1433221400000, // Lambda timestamp
  "_source_file": "s3://bucket/..."      // Source file path
}
```

**Partitioning:** By `itemid` (data co-location)

### 4. Databricks Processing Layers

#### Bronze Layer (Raw Data)

**Batch: item_properties_bronze**
```sql
CREATE TABLE item_properties_bronze (
  timestamp BIGINT,
  itemid INT,
  property INT,
  value STRING,
  _ingestion_time TIMESTAMP,
  _source_file STRING
)
USING DELTA
```

**Speed: events_bronze**
```sql
CREATE TABLE events_bronze (
  timestamp BIGINT,
  visitorid STRING,
  event STRING,
  itemid STRING,
  transactionid STRING,
  _ingestion_id STRING,
  _ingestion_timestamp BIGINT,
  _source_file STRING
)
USING DELTA
```

**Ingestion Method:**
- **Batch:** Databricks Autoloader (Cloud Files API)
  - Listens to S3 path
  - Automatically detects new files
  - Processes on schedule or continuously
  
- **Speed:** Structured Streaming from Kinesis
  - Reads from shard (with auto-position)
  - Continuous stream processing
  - Fault-tolerant checkpoints

#### Silver Layer (Cleaned Data)

**Batch: item_properties_silver**
```sql
SELECT
  DATE(FROM_UNIXTIME(timestamp / 1000)) as date,
  itemid as item_id,
  property as property_id,
  value as property_value,
  CURRENT_TIMESTAMP() as _updated_at
FROM item_properties_bronze
WHERE timestamp IS NOT NULL
  AND itemid IS NOT NULL
  AND property IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY itemid, property ORDER BY timestamp DESC) = 1
-- Deduplicates by keeping latest per (itemid, property)
```

**Speed: events_silver**
```sql
SELECT
  timestamp,
  visitorid,
  LOWER(event) as event,  -- Normalize
  itemid,
  transactionid,
  CAST(timestamp / 1000.0 AS TIMESTAMP) as event_time,
  CAST(timestamp / 1000.0 AS DATE) as event_date,
  HOUR(CAST(timestamp / 1000.0 AS TIMESTAMP)) as event_hour,
  (CURRENT_TIMESTAMP() * 1000 - timestamp) as latency_ms,
  _ingestion_id,
  _ingestion_timestamp,
  _source_file
FROM events_bronze
WHERE timestamp IS NOT NULL
  AND visitorid IS NOT NULL
  AND event IN ('view', 'click', 'purchase', 'add_to_cart', 'remove_from_cart')
```

#### Gold Layer (Analytics)

**Batch Gold Tables:**

1. **item_properties_summary**
   - Items ↔ Properties mapping
   - Unique values per property
   - Popular items

2. **property_statistics**
   - Property occurrence counts
   - Item count per property
   - Property popularity rank

**Speed Gold Tables:**

1. **event_metrics_by_time** (5-min windows)
   ```sql
   SELECT
     WINDOW(event_time, '5 minutes') as event_window,
     event,
     COUNT(*) as event_count,
     COUNT(DISTINCT visitorid) as unique_visitors,
     COUNT(DISTINCT transactionid) as transactions
   FROM events_silver
   GROUP BY WINDOW(event_time, '5 minutes'), event
   ```

2. **visitor_behavior_real_time** (10-min windows)
   ```sql
   SELECT
     WINDOW(event_time, '10 minutes') as behavior_window,
     visitorid,
     COUNT(*) as events_count,
     COUNT(DISTINCT event) as event_types,
     MAX(event_time) as last_activity
   FROM events_silver
   GROUP BY WINDOW(event_time, '10 minutes'), visitorid
   ```

3. **item_popularity_real_time** (15-min windows)
   ```sql
   SELECT
     WINDOW(event_time, '15 minutes') as popularity_window,
     itemid,
     COUNT(*) as total_interactions,
     COUNTIF(event = 'purchase') as purchase_count,
     COUNTIF(event = 'view') as view_count
   FROM events_silver
   GROUP BY WINDOW(event_time, '15 minutes'), itemid
   ```

## Data Schemas

### Input CSV Formats

**Batch CSV: item_properties_part2.csv**
```csv
timestamp,itemid,property,value
1620000000,123,1,red
1620000001,123,2,large
1620000002,456,1,blue
```

**Streaming CSV: events.csv**
```csv
timestamp,visitorid,event,itemid,transactionid
1433221332117,user-1,view,item-1,
1433221333118,user-1,click,item-1,
1433221334119,user-1,purchase,item-1,txn-123
```

## Processing Pipelines

### Batch Pipeline

```
1. Upload CSV to S3
   ↓ (partitioned by date)
   
2. Autoloader detects new files
   ↓ (~every 1-2 minutes)
   
3. Ingest to Bronze
   ↓ load_properties_bronze.py (scheduled job)
   
4. Transform to Silver
   ↓ transform_properties_silver.py (deduplicate, clean)
   
5. Aggregate to Gold
   ↓ aggregate_properties_gold.py (analytics tables)
```

**Execution:**
- Manual: Run script directly in Databricks
- Scheduled: Daily at midnight UTC (configurable)
- Triggered: On-demand via API call

### Streaming Pipeline

```
1. Upload CSV to S3
   ↓ (partitioned by date)
   
2. S3 event triggers Lambda
   ↓ (~immediate)
   
3. Lambda processes CSV to Kinesis
   ↓ (1000s records/sec)
   
4. Structured Streaming reads Kinesis
   ↓ ingest_events_bronze.py (continuous job)
   
5. Transform to Silver
   ↓ transform_events_silver.py (continuous)
   
6. Aggregate to Gold
   ↓ aggregate_events_gold.py (windowed aggregations)
```

**Execution:**
- Continuous: Jobs run indefinitely
- Scalable: Auto-scales with Kinesis shards
- Fault-tolerant: Checkpointed state

## Performance & Scaling

### Capacity

| Component | Throughput | Notes |
|-----------|-----------|-------|
| Lambda | ~3000 records/min per 512MB | Adjust memory for speed |
| Kinesis | On-demand | Scales automatically |
| Databricks | Depends on cluster | Scale worker count |
| S3 | Unlimited | Standard throughput |

### Optimization Tips

1. **Lambda:**
   - Increase memory for CPU-bound processing
   - Batch S3 uploads to reduce invocations
   - Monitor execution duration in CloudWatch

2. **Kinesis:**
   - Default on-demand scaling sufficient for most workloads
   - Monitor iterator age for lag detection
   - Adjust partition key for even distribution

3. **Databricks:**
   - Use `OPTIMIZE` on Gold tables regularly
   - Enable auto-compaction for Delta tables
   - Right-size cluster for streaming jobs

4. **S3:**
   - Use S3 Select for large files (cost optimization)
   - Enable CloudFront for frequently accessed data
   - Configure lifecycle policies for cost

## Security Architecture

### IAM Roles

**Databricks EC2 Instance Profile:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::batch-bucket",
        "arn:aws:s3:::batch-bucket/*",
        "arn:aws:s3:::streaming-bucket",
        "arn:aws:s3:::streaming-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:DescribeStream",
        "kinesis:ListStreams"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/events-stream"
    }
  ]
}
```

**Lambda Execution Role:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::streaming-bucket/*"
    },
    {
      "Effect": "Allow",
      "Action": "kinesis:PutRecord",
      "Resource": "arn:aws:kinesis:*:*:stream/events-stream"
    }
  ]
}
```

### Data Encryption

- **S3:** Server-side encryption (SSE-S3)
- **Kinesis:** KMS encryption
- **Databricks:** Workspace encryption + network encryption
- **Transit:** HTTPS for all API calls

### Network Security

- **VPC Endpoint:** S3 Gateway endpoint (for private connectivity)
- **Security Groups:** Restrict Databricks cluster to Lambda
- **IAM:** Minimum privilege access

## Monitoring & Observability

### CloudWatch Metrics

**Lambda:**
```
Metrics:
- Invocations (calls/minute)
- Errors (failed calls)
- Duration (execution time)
- ConcurrentExecutions (parallel runs)

Logs: /aws/lambda/s3-streaming-processor
```

**Kinesis:**
```
Metrics:
- IncomingRecords (records/minute)
- IncomingBytes (MB/minute)
- GetRecords.IteratorAgeMilliseconds (lag)
- ReadProvisionedThroughputExceeded (backpressure)
```

**S3:**
```
Metrics:
- NumberOfObjects (file count)
- BucketSizeBytes (storage size)
- AllRequests (API calls)
```

### Databricks Monitoring

```sql
-- Data freshness
SELECT MAX(_ingestion_timestamp) as latest FROM events_bronze;

-- Ingestion rate
SELECT 
  DATE(_ingestion_timestamp) as date,
  COUNT(*) as records
FROM events_bronze
GROUP BY DATE(_ingestion_timestamp)
ORDER BY date DESC LIMIT 7;

-- Job health
SELECT 
  job_name, 
  status, 
  duration_seconds,
  start_time
FROM system.compute.jobs
ORDER BY start_time DESC LIMIT 10;
```

## Disaster Recovery

### Backup Strategy

- **S3:** Versioning enabled, automatic backups
- **Databricks:** Time travel via Delta Lake (30-day default)
- **Kinesis:** 24-hour retention built-in

### Recovery Procedures

1. **Data Loss:**
   - Restore from S3 versioning
   - Use Delta Lake time travel: `SELECT * FROM table VERSION AS OF timestamp`

2. **Failed Jobs:**
   - Check logs in CloudWatch / Databricks
   - Replay from checkpoint or re-upload data

3. **Cluster Failure:**
   - Databricks auto-recovery or manual restart
   - Streaming jobs resume from checkpoint

---

For implementation details, see [README.md](../README.md) and [QUICK_START.md](../QUICK_START.md).
