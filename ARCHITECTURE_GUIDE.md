# 📐 Architecture Guide

Complete technical breakdown of the Lambda architecture system.

## System Overview

```
┌──────────────────────────────────────────────────────────────┐
│           DATABRICKS LAMBDA ARCHITECTURE                     │
└──────────────────────────────────────────────────────────────┘

INPUT SOURCES          AWS PROCESSING          DATABRICKS
═════════════════════════════════════════════════════════

Batch CSV              S3 (Batch)               Autoloader
  ↓                    Versioned                   ↓
(item_properties)  ──→ Lifecycle Policy ─────→  Bronze
                       30-365 days retention       ↓
                                              Silver
                                                ↓
Streaming CSV          S3 (Streaming)          Gold
  ↓                    Partitioned
(events.csv)       ──→ Event Notifications─→  Lambda ──→ Kinesis
                       Date-based                           ↓
                                              Structured Streaming
                                                   ↓
                                              Bronze ──→
                                                   ↓
                                              Silver ──→
                                                   ↓
                                              Gold (Analytics)
```

## Component Architecture

### 1. Data Ingestion Layer

#### Batch (Item Properties)
```
CSV File (50MB+)
    ↓
upload_to_s3.py
    ├─ Reads CSV line-by-line
    ├─ Partitions by date: YYYY/MM/DD/
    └─ Uploads to S3
         ↓
    S3 Bucket (batch)
    ├─ Versioning: Enabled
    ├─ Lifecycle: Archive @ 90d, Delete @ 365d
    └─ Partitioned: data/batch/2026/03/17/...
```

**Schema:**
```csv
timestamp,itemid,property,value
1620000000,123,1,red
1620000001,123,2,large
```

#### Streaming (Events)
```
CSV File (50MB+)
    ↓
upload_to_s3.py
    ├─ Reads CSV line-by-line
    ├─ Partitions by date: YYYY/MM/DD/
    └─ Uploads to S3
         ↓
    S3 Bucket (streaming)
    ├─ Event Notifications: On ObjectCreated
    ├─ Lifecycle: Delete @ 30d
    ├─ Partitioned: data/streaming/2026/03/17/...
    └─ Triggers Lambda on file upload
         ↓
    Lambda: s3_streaming_processor
```

**Schema:**
```csv
timestamp,visitorid,event,itemid,transactionid
1433221332117,user-1,view,item-1,
1433221333118,user-1,click,item-1,
1433221334119,user-1,purchase,item-1,txn-123
```

### 2. Lambda Processor (s3_streaming_processor.py)

**Trigger:** S3 PutObject event on streaming bucket

**Processing Flow:**
```python
def lambda_handler(event, context):
    1. Extract bucket & key from S3 event
    2. Download CSV from S3
    3. Parse CSV into records
    4. For each record:
       a. Extract: timestamp, visitorid, event, itemid, transactionid
       b. Validate required fields
       c. Add metadata:
          - _ingestion_id (UUID)
          - _ingestion_timestamp (current MS since epoch)
          - _source_file (S3 path)
       d. Convert to JSON
       e. Put to Kinesis with PartitionKey = itemid
    5. Return: {successful: N, failed: M}
```

**Configuration:**
- Runtime: Python 3.11
- Memory: 512 MB (configurable)
- Timeout: 60 seconds
- Environment Variable: `KINESIS_STREAM` (stream name)

**IAM Permissions:**
- `s3:GetObject` → Streaming S3 bucket
- `kinesis:PutRecord`, `kinesis:PutRecords` → Kinesis stream
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` → CloudWatch

### 3. Kinesis Stream

**Configuration:**
- Name: `lambda-arch-events` (customizable)
- Mode: ON_DEMAND (auto-scaling)
- Retention: 24 hours
- Encryption: Enabled (AWS managed)
- Partition Key: `itemid` (ensures data co-location)

**Record Schema (JSON):**
```json
{
  "timestamp": 1433221332117,
  "visitorid": "user-123",
  "event": "view",
  "itemid": "item-456",
  "transactionid": "txn-789",
  "_ingestion_id": "550e8400-e29b-41d4-a716-446655440000",
  "_ingestion_timestamp": 1433221400000,
  "_source_file": "s3://bucket/data/streaming/2026/03/17/events.csv"
}
```

### 4. Databricks Processing

#### Bronze Layer (Raw Data)

**Batch Table: bronze_layer.item_properties**
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

**Speed Table: bronze_layer.events**
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

**Ingestion:**
- **Batch**: Databricks Autoloader reads from S3 (auto-detect new files)
- **Speed**: Structured Streaming reads from Kinesis (continuous)

#### Silver Layer (Cleaned & Transformed)

**Batch: transform_properties_silver.py**
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
QUALIFY ROW_NUMBER() OVER (PARTITION BY itemid, property ORDER BY timestamp DESC) = 1
```

**Speed: transform_events_silver.py**
```sql
SELECT
  timestamp,
  visitorid,
  LOWER(event) as event,
  itemid,
  transactionid,
  CAST(timestamp / 1000.0 AS TIMESTAMP) as event_time,
  DATE(CAST(timestamp / 1000.0 AS TIMESTAMP)) as event_date,
  HOUR(CAST(timestamp / 1000.0 AS TIMESTAMP)) as event_hour,
  _ingestion_id,
  _ingestion_timestamp,
  _source_file
FROM events_bronze
WHERE timestamp IS NOT NULL
  AND visitorid IS NOT NULL
  AND event IN ('view', 'click', 'purchase', 'add_to_cart', 'remove_from_cart')
```

#### Gold Layer (Analytics)

**Batch Analytics:**
- `item_properties_summary`: Items ↔ properties mapping
- `property_statistics`: Property popularity

**Speed Analytics (Windowed):**
```sql
-- 5-minute event metrics
SELECT
  WINDOW(event_time, '5 minutes') as time_window,
  event,
  COUNT(*) as event_count
FROM events_silver
GROUP BY WINDOW(event_time, '5 minutes'), event

-- 15-minute item popularity
SELECT
  WINDOW(event_time, '15 minutes') as time_window,
  itemid,
  COUNT(*) as interactions,
  COUNTIF(event = 'purchase') as purchases
FROM events_silver
GROUP BY WINDOW(event_time, '15 minutes'), itemid
```

## Data Flow Diagram

```
┌─────────────────┐
│ Batch CSV       │
└────────┬────────┘
         │
         ▼
┌──────────────────────────┐         ┌────────────────┐
│ upload_to_s3.py          │         │ S3 (batch)     │
│ ├─ Partitions           │────────▶│ versioned      │
│ └─ Uploads              │         │ lifecycle      │
└──────────────────────────┘         └────────┬───────┘
                                             │
                                             ▼
┌─────────────────┐                ┌──────────────────┐
│ Streaming CSV   │                │ Autoloader       │
└────────┬────────┘                │ (auto-detect)    │
         │                         └────────┬─────────┘
         ▼                                  │
┌──────────────────────────┐               ▼
│ upload_to_s3.py          │         ┌──────────────────┐
│ ├─ Partitions           │         │ BRONZE           │
│ └─ Uploads              │         │ item_properties  │
└──────────────────────────┘         └────────┬─────────┘
         │                                   │
         │ S3 Event                         ▼
         │ (ObjectCreated)           ┌──────────────────┐
         ▼                           │ SILVER           │
┌──────────────────────────┐         │ cleaned data     │
│ Lambda Processor         │         └────────┬─────────┘
│ ├─ Parse CSV            │                 │
│ ├─ Validate fields      │                 ▼
│ ├─ Add metadata         │         ┌──────────────────┐
│ └─ Send to Kinesis      │         │ GOLD             │
└──────────────────────────┘         │ analytics        │
         │                           └──────────────────┘
         │ Records
         ▼
┌──────────────────────────┐
│ Kinesis Stream           │
│ (ON_DEMAND, 24h ret.)   │
└──────────────────────────┘
         │
         │ Structured Streaming
         ▼
┌──────────────────────────┐
│ BRONZE                   │
│ events_bronze            │
└──────────────────────────┘
         │
         ▼
┌──────────────────────────┐
│ SILVER                   │
│ events_silver            │
└──────────────────────────┘
         │
         ▼
┌──────────────────────────┐
│ GOLD (Real-time)         │
│ ├─ 5-min event metrics   │
│ ├─ 10-min visitor behavior
│ └─ 15-min item popularity
└──────────────────────────┘
```

## Deployment Architecture

### Terraform Structure

```
config/terraform/
├── providers.tf          # AWS provider config
├── variables.tf          # Variable definitions
├── s3.tf                 # S3 buckets (batch & streaming)
├── kinesis.tf            # Kinesis stream
├── lambda.tf             # Lambda function & IAM
├── outputs.tf            # Output values
└── terraform.tfvars      # Configuration (customize!)
```

### Key Outputs

| Output | Value | Used For |
|--------|-------|----------|
| `batch_bucket_name` | S3 bucket | Batch data uploads |
| `streaming_bucket_name` | S3 bucket | Streaming data uploads |
| `kinesis_stream_name` | Stream name | Databricks Kinesis connection |
| `lambda_function_name` | Function name | Monitoring & debugging |

## Performance Characteristics

### Throughput

| Component | Capacity | Notes |
|-----------|----------|-------|
| Lambda | ~100 CSV records/sec | 512MB memory |
| Kinesis (ON_DEMAND) | Auto-scales | Unlimited |
| Databricks Autoloader | ~1000 records/sec | Cluster dependent |
| Structured Streaming | ~10000 records/sec | Cluster dependent |

### Latency

| Stage | Latency |
|-------|---------|
| S3 Upload → Lambda Trigger | ~100ms |
| CSV Parse → Kinesis Put | ~50-100ms |
| Kinesis → Databricks Read | ~1-5 seconds |
| Full pipeline E2E | ~10-20 seconds |

## Monitoring & Observability

### CloudWatch Metrics

**Lambda:**
- `Invocations` - CSV files processed
- `Duration` - Execution time
- `Errors` - Failed invocations
- `ConcurrentExecutions` - Parallel runs

**Kinesis:**
- `IncomingRecords` - Records/minute
- `GetRecords.IteratorAge` - Databricks lag
- `WriteProvisionedThroughputExceeded` - Throttling

**S3:**
- `NumberOfObjects` - File count
- `BucketSizeBytes` - Storage size
- `4xxErrors`, `5xxErrors` - Request errors

### Databricks Monitoring

```sql
-- Data freshness
SELECT MAX(_ingestion_timestamp) FROM events_bronze;

-- Records ingested per hour
SELECT 
  CAST(FROM_UNIXTIME(_ingestion_timestamp / 1000 / 3600 * 3600) AS DATE),
  COUNT(*) as records
FROM events_bronze
GROUP BY 1
ORDER BY 1 DESC;
```

## Security

### IAM Permissions

**Lambda Execution Role:**
- `s3:GetObject` on `streaming/` prefix
- `kinesis:PutRecord`, `kinesis:PutRecords`
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

**S3 Bucket Policies:**
- Versioning enabled
- Server-side encryption (default)
- Access logging optional

**Kinesis Stream:**
- Encryption enabled (AWS managed)
- Access via IAM roles only

## Disaster Recovery

### Backup Strategy

- **S3 Versioning**: Keep 365+ days of historical files
- **Kinesis Retention**: 24-hour buffer built-in
- **Delta Lake**: 30-day time travel available

### Recovery Procedures

1. **Lost Records**: Replay from S3 version or Kinesis buffer
2. **Failed Lambda**: Re-trigger manually via CloudWatch
3. **Kinesis Lag**: Increase shard count (ON_DEMAND mode auto-scales)

## Optimization Tips

1. **Lambda Memory**: Increase for CPU-bound processing (~1GB for large files)
2. **Batch Uploading**: Upload multiple files at once to reduce Lambda invocations
3. **Kinesis Partitions**: Ensure even distribution across itemid values
4. **Databricks Cluster**: Use auto-scaling for optimal cost/performance

---

For implementation details, see [QUICK_START.md](./QUICK_START.md).
