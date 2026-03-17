# Databricks Lambda Architecture - Batch + Real-time Streaming



## 📊 Project Overview

Complete **Lambda Architecture** implementation combining:
- **Batch Layer** 📦: Process item properties from S3 via Databricks Autoloader
- **Speed Layer** ⚡: Real-time event streaming via S3 → Lambda → Kinesis → Databricks

**Data Pipeline:**
```
Item Properties  → S3 Autoloader → Databricks (Batch ETL)  → Analytics
User Events      → S3 → Lambda → Kinesis → Databricks (Streaming ETL) → Real-time KPIs
```

## 🎯 Key Features

✨ **Batch Processing**
- Databricks Autoloader for automatic S3 ingestion
- Delta Lake Bronze → Silver → Gold medallion layers
- Deduplication, cleaning, statistical aggregations

⚡ **Real-time Streaming**
- Lambda processes CSV records from S3 to Kinesis
- Structured Streaming for event processing
- 5-15 minute windowed aggregations
- Real-time KPIs: visitor behavior, item popularity, event trends

🔐 **Enterprise-Ready**
- Terraform Infrastructure as Code (AWS + Databricks)
- Delta Lake with ACID transactions & time travel
- Data quality checks & schema validation
- Comprehensive CloudWatch monitoring
- Automated job orchestration

## 📁 Project Structure

```
databricks-demo/
├── data/
│   ├── batch/
│   │   ├── item_properties_part2.csv    # Item catalog (~50MB)
│   │   └── README.md                    # Batch guide
│   └── streaming/
│       ├── events.csv                   # Events (~50MB)
│       └── README.md                    # Streaming guide
│
├── scripts/
│   ├── upload_to_s3.py                  # Unified S3 uploader
│   └── README.md
│
├── lambda/
│   ├── s3_streaming_processor.py         # CSV → Kinesis
│   └── requirements.txt
│
├── src/
│   ├── batch_layer/                     # Batch ETL
│   │   ├── load_properties_bronze.py    # S3 Autoloader → Bronze
│   │   ├── transform_properties_silver.py
│   │   ├── aggregate_properties_gold.py
│   │   ├── config.py                    # DB & table schemas
│   │   └── orchestration.py             # Job definitions
│   │
│   └── speed_layer/                     # Streaming ETL
│       ├── ingest_events_bronze.py      # Kinesis → Bronze
│       ├── transform_events_silver.py
│       ├── aggregate_events_gold.py
│       ├── config.py
│       └── orchestration.py
│
├── terraform/                           # Infrastructure as Code
│   ├── providers.tf
│   ├── aws.tf                           # S3, Kinesis, Lambda
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example
│   └── README.md
│
├── QUICK_START.md                       # 5-minute setup
├── ARCHITECTURE_GUIDE.md                # Technical deep-dive
└── README.md                            # This file
```

## 🏗️ Architecture

### Data Layers & Flow

```
┌─────────────────────────────────────────┬─────────────────────────────────┐
│ BATCH LAYER (Item Properties)           │ SPEED LAYER (Events)            │
└─────────────────────────────────────────┴─────────────────────────────────┘

item_properties_part2.csv                 events.csv
          ↓                                       ↓
[upload_to_s3.py]                        [upload_to_s3.py]
          ↓                                       ↓
  S3 Bucket (Batch)                      S3 Bucket (Streaming)
  data/batch/YYYY/MM/DD/...              data/streaming/YYYY/MM/DD/...
          ↓                                       ↓
Databricks Autoloader                    Lambda: s3_streaming_processor
(Auto-detect new files)                  (S3 → Kinesis converter)
          ↓                                       ↓
BRONZE LAYER:                            Kinesis Stream (events-stream)
  item_properties_bronze                           ↓
  (Raw CSVs)                             BRONZE LAYER:
          ↓                                events_bronze
SILVER LAYER:                            (Structured Streaming)
  item_properties_silver                          ↓
  (Cleaned, deduplicated,                SILVER LAYER:
   partitioned by date)                   events_silver
          ↓                               (Validated, enriched,
GOLD LAYER:                               partitioned by event_date)
  • item_properties_summary                       ↓
  • property_statistics               GOLD LAYER (Real-time):
  (Aggregations & analytics)           • event_metrics_by_time (5-min)
                                       • visitor_behavior (10-min)
                                       • item_popularity (15-min)
```

### Data Processing Pipeline
```
INPUT                    PROCESSING              OUTPUT
──────────────────────────────────────────────────────────
                         Batch:
Properties CSV ──→ S3 ──→ Autoloader ──→ Databricks ──→ Analytics
                                             ↓
                                         Bronze → Silver → Gold
                         
                         Speed:
Events CSV ────→ S3 ────→ Lambda ──→ Kinesis ──→ Databricks ──→ Real-time KPIs
                         Processor                    ↓
                                                 Bronze → Silver → Gold
```

## 🚀 Quick Start (5 minutes)

### 1️⃣ Prerequisites
- AWS account with credentials
- Databricks workspace with PAT token
- Python 3.9+, Terraform 1.0+

### 2️⃣ Deploy AWS Infrastructure
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your credentials

terraform init
terraform apply

# Get bucket names:
terraform output batch_data_bucket
terraform output streaming_data_bucket
```

### 3️⃣ Upload Data to S3
```bash
export BATCH_BUCKET=$(terraform output -raw batch_data_bucket)
export STREAMING_BUCKET=$(terraform output -raw streaming_data_bucket)

# Upload batch (one-time) → Auto-ingested by Databricks
python scripts/upload_to_s3.py data/batch/item_properties_part2.csv \
  --bucket $BATCH_BUCKET

# Upload streaming (triggers Lambda) → Events → Kinesis → Databricks
python scripts/upload_to_s3.py data/streaming/events.csv \
  --bucket $STREAMING_BUCKET
```

### 4️⃣ Initialize Databricks Tables
Create a notebook in Databricks workspace and run:
```python
import sys
sys.path.append('/Workspace/src')

from batch_layer.config import INIT_SQL as BATCH_INIT
from speed_layer.config import INIT_SQL as SPEED_INIT

spark.sql(BATCH_INIT)
spark.sql(SPEED_INIT)
print("✅ Tables initialized")
```

### 5️⃣ Start Processing Jobs
Deploy and run jobs in Databricks from:
- `src/batch_layer/orchestration.py` - Batch jobs
- `src/speed_layer/orchestration.py` - Streaming jobs

### 6️⃣ Verify Data
```sql
-- Batch layer
SELECT COUNT(*) FROM item_properties_bronze;
SELECT COUNT(*) FROM item_properties_silver;

-- Speed layer
SELECT COUNT(*) FROM events_bronze;
SELECT * FROM event_metrics_by_time ORDER BY event_at DESC LIMIT 5;
```

**→ Full setup guide: [QUICK_START.md](QUICK_START.md)**

## 📊 Data Sources & Tables

### Batch Layer (Item Properties)

| Stage | Table | Format | Purpose |
|-------|-------|--------|---------|
| **Bronze** | `item_properties_bronze` | Delta | Raw CSV ingestion |
| **Silver** | `item_properties_silver` | Delta | Cleaned, deduplicated |
| **Gold** | `item_properties_summary` | Delta | Item-property mapping |
| **Gold** | `property_statistics` | Delta | Property popularity analysis |

**Sample Query:**
```sql
SELECT itemid, COUNT(*) as property_count
FROM item_properties_silver
GROUP BY itemid
ORDER BY property_count DESC;
```

### Speed Layer (Events)

| Stage | Table | Format | Purpose |
|-------|-------|--------|---------|
| **Bronze** | `events_bronze` | Delta | Raw Kinesis events |
| **Silver** | `events_silver` | Delta | Validated, enriched |
| **Gold** | `event_metrics_by_time` | Delta | 5-min event aggregations |
| **Gold** | `visitor_behavior_real_time` | Delta | 10-min visitor KPIs |
| **Gold** | `item_popularity_real_time` | Delta | 15-min trending items |

**Sample Queries:**
```sql
-- Real-time event volume
SELECT event, COUNT(*) as count
FROM event_metrics_by_time
WHERE event_at > current_timestamp - INTERVAL 1 HOUR
GROUP BY event;

-- Trending items
SELECT itemid, total_interactions
FROM item_popularity_real_time
ORDER BY total_interactions DESC LIMIT 20;
```

## 🔄 Data Flow Details

### **Batch Layer Flow**

```
1. Upload CSV to S3
   python scripts/upload_to_s3.py data/batch/item_properties_part2.csv --bucket $BUCKET
   ↓ Uploaded to: s3://bucket/data/batch/YYYY/MM/DD/item_properties_part2.csv

2. Databricks Autoloader detects new files
   ↓ Automatically ingests

3. Bronze Table (raw data)
   item_properties_bronze
   - Columns: timestamp, itemid, property, value, _ingestion_time, _source_file
   ↓ Job: load_properties_bronze.py

4. Silver Table (cleaned)
   item_properties_silver
   - Deduplicates by (itemid, property)
   - Converts datatypes, renames columns
   - Partitioned by: date
   ↓ Job: transform_properties_silver.py

5. Gold Tables (analytics)
   item_properties_summary → Item-property mapping
   property_statistics → Property popularity metrics
   ↓ Job: aggregate_properties_gold.py
```

### **Speed Layer Flow**

```
1. Upload CSV to S3
   python scripts/upload_to_s3.py data/streaming/events.csv --bucket $BUCKET
   ↓ Uploaded to: s3://bucket/data/streaming/YYYY/MM/DD/events.csv

2. S3 PutObject triggers AWS Lambda
   ↓ Lambda: s3_streaming_processor.py

3. Lambda Processing:
   - Reads CSV file from S3
   - Parses each record
   - Adds metadata (_ingestion_id, _ingestion_timestamp, _source_file)
   - Validates required fields (timestamp, visitorid, event, itemid)
   - Puts each record to Kinesis stream
   ↓ Partitioned by: item_id

4. Kinesis Stream (events-stream)
   - On-demand scaling, 24-hour retention
   ↓ Consumed by Databricks Structured Streaming

5. Bronze Table (streaming ingestion)
   events_bronze
   - Columns: timestamp, visitorid, event, itemid, transactionid,
              _ingestion_id, _ingestion_timestamp, _source_file
   ↓ Job: ingest_events_bronze.py (continuous)

6. Silver Table (cleaned events)
   events_silver
   - Validates event types
   - Converts timestamps, calculates latency
   - Extracts event_date, event_hour
   - Partitioned by: event_date
   ↓ Job: transform_events_silver.py (continuous)

7. Gold Tables (real-time analytics)
   event_metrics_by_time → Event counts & volume (5-min windows)
   visitor_behavior_real_time → Visitor engagement (10-min windows)
   item_popularity_real_time → Trending items (15-min windows)
   ↓ Job: aggregate_events_gold.py (continuous)
```

## 🛠️ Technology Stack

### Cloud Services
- **AWS S3**: Data storage (versioning, encryption, lifecycle policies)
- **AWS Kinesis**: Real-time event streaming (on-demand scaling)
- **AWS Lambda**: CSV processing and Kinesis integration
- **AWS IAM**: Access control and security

### Data Platform
- **Databricks**: Data warehousing + analytics
- **Delta Lake**: ACID transactions, MVCC, time travel
- **Apache Spark**: Distributed data processing
- **Structured Streaming**: Event stream processing

### Infrastructure
- **Terraform**: Infrastructure as Code (AWS + Databricks)
- **HCL**: Configuration language

### Languages & Tools
- **Python 3.9+**: Lambda functions, Databricks notebooks
- **SQL**: Data transformations and analytics
- **Bash/PowerShell**: Deployment scripts

## 📈 Monitoring & Operations

### Observability

**CloudWatch Metrics:**
```bash
# Lambda invocations
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --start-time 2026-03-17T00:00:00Z \
  --end-time 2026-03-17T01:00:00Z \
  --period 300 \
  --statistics Sum

# Kinesis put-record rate
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name IncomingRecords \
  --stream-name events-stream \
  --period 300 \
  --statistics Sum
```

**Databricks Monitoring (in workspace):**
```sql
-- Check data freshness
SELECT MAX(_ingestion_timestamp) FROM events_bronze;

-- Monitor ingestion rate
SELECT 
  COUNT(*) as record_count,
  DATE('_ingestion_timestamp') as ingestion_date
FROM events_bronze
GROUP BY DATE('_ingestion_timestamp')
ORDER BY ingestion_date DESC LIMIT 7;

-- Check for processing errors
SELECT COUNT(*) as errors
FROM events_silver
WHERE validation_status = 'failed';
```

### Common Operations

**Re-process data:**
```bash
# Re-upload batch (appends to existing)
python scripts/upload_to_s3.py data/batch/item_properties_part2.csv --bucket $BATCH_BUCKET

# Re-upload streaming (triggers Lambda again)
python scripts/upload_to_s3.py data/streaming/events.csv --bucket $STREAMING_BUCKET
```

**Check job status:**
```bash
databricks jobs list
databricks jobs get-run --run-id 123
```

**Scale up:**
```hcl
# In terraform/variables.tf
variable "kinesis_shard_count" {
  default = 2  # Increase shards for higher throughput
}

variable "lambda_memory_mb" {
  default = 1024  # Increase memory for CPU-bound processing
}
```

## 🔐 Security Best Practices

✅ **AWS IAM**
- Databricks EC2 instance profile with minimal permissions
- Lambda execution role scoped to S3 and Kinesis
- S3 bucket policies for data access control

✅ **Data Protection**
- S3 encryption enabled (SSE-S3)
- Kinesis encryption enabled
- Databricks workspace encryption at rest
- VPC isolation (if deployed in VPC)

✅ **Monitoring & Auditing**
- CloudWatch Logs for Lambda execution
- Databricks audit logs for data access
- S3 access logging enabled
- CloudTrail for API tracking

## 📚 Documentation

- **[QUICK_START.md](QUICK_START.md)** - 5-minute setup guide
- **[ARCHITECTURE_GUIDE.md](ARCHITECTURE_GUIDE.md)** - Detailed technical docs
- **[terraform/README.md](terraform/README.md)** - Infrastructure details
- **[scripts/README.md](scripts/README.md)** - Upload script guide
- **[data/batch/README.md](data/batch/README.md)** - Batch data info
- **[data/streaming/README.md](data/streaming/README.md)** - Streaming data info

## ❓ Troubleshooting

### Issue: Lambda not processing S3 uploads
**Solution:**
- Check S3 event notifications: AWS Console → S3 → Properties → Event notifications
- Verify Lambda has S3:GetObject permission
- Check CloudWatch logs: `/aws/lambda/s3-streaming-processor`

### Issue: Data not in Databricks tables
**Solution:**
- Verify Kinesis stream has data: `aws kinesis describe-stream --stream-name events-stream`
- Check cluster is running and has right IAM permissions
- Review job logs in Databricks workspace

### Issue: Autoloader not picking up batch data
**Solution:**
- Verify S3 path matches config: `s3://bucket/data/batch/...`
- Check Bronze table schema matches CSV headers
- Ensure Databricks cluster can access S3 bucket

### Issue: High Lambda costs
**Solution:**
- Reduce batch size in upload script
- Increase Kinesis shard count to handle more events
- Monitor Lambda execution duration in CloudWatch

## 🚀 Scaling Considerations

| Component | Current Capacity | Scale Up |
|-----------|------------------|----------|
| **S3** | Unlimited | Automatic (AWS managed) |
| **Kinesis** | On-demand | Increase shard count in Terraform |
| **Lambda** | 512 MB / 60s | Increase memory & timeout |
| **Databricks** | Auto-scaling cluster | Increase worker count |
| **Delta Tables** | Unlimited | Optimize with `OPTIMIZE` command |

## 📄 License

Provided as-is for educational & training purposes.

## 🤝 Support

- Check **troubleshooting** section above
- Review documentation files for detailed info
- Check job logs in Databricks & CloudWatch
- Verify AWS & Databricks permissions

---

**Status:** ✅ Production Ready | **Last Updated:** March 2026 | **Version:** 1.0
