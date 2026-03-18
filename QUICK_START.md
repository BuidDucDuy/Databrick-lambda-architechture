# ⚡ Quick Start (5 Minutes)

Get the entire system running in 5 minutes.

## Prerequisites (30 seconds)

✅ Have these ready:
- AWS account with credentials (`aws configure`)
- Python 3.9+ installed
- Terraform 1.0+ installed

## Step 1: Deploy Infrastructure (1 minute)

```bash
cd config/terraform
terraform init
terraform apply

# Note the outputs:
# - batch_bucket_name
# - streaming_bucket_name
# - kinesis_stream_name
# - lambda_function_name
```

Save these values - you'll need them next.

## Step 2: Upload Sample Data (1 minute)

From the **project root** directory:

```bash
# Get bucket names from Terraform outputs
export BATCH_BUCKET=$(cd config/terraform && terraform output -raw batch_bucket_name)
export STREAMING_BUCKET=$(cd config/terraform && terraform output -raw streaming_bucket_name)

# Upload batch data (processed by Autoloader)
python scripts/upload_to_s3.py data/batch/item_properties_part2.csv --bucket $BATCH_BUCKET

# Upload streaming data (triggers Lambda → Kinesis)
python scripts/upload_to_s3.py data/streaming/events.csv --bucket $STREAMING_BUCKET
```

## Step 3: Create Databricks Tables (2 minutes)

In your **Databricks workspace**, create a new notebook and run:

```python
# Create Bronze tables
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_layer.item_properties (
  timestamp BIGINT,
  itemid INT,
  property INT,
  value STRING,
  _ingestion_time TIMESTAMP,
  _source_file STRING
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_layer.events (
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
""")

print("✅ Bronze tables created!")
```

## Step 4: Query Your Data (1 minute)

```sql
-- Count batch records
SELECT COUNT(*) as batch_records FROM bronze_layer.item_properties;

-- Count event records  
SELECT COUNT(*) as event_records FROM bronze_layer.events;

-- Sample data
SELECT * FROM bronze_layer.events LIMIT 10;
```

## ✅ You're Done!

Your pipeline is now running:
- ✅ S3 buckets created
- ✅ Kinesis stream active
- ✅ Lambda function deployed
- ✅ Data flowing into Databricks

## 🎯 Next Steps

1. **Create Silver tables** - Add deduplication & validation logic
2. **Create Gold tables** - Build analytics aggregations
3. **Set up jobs** - Schedule batch and streaming pipelines
4. **Monitor** - Check CloudWatch for Lambda execution

## 📚 Learn More

- [ARCHITECTURE_GUIDE.md](./ARCHITECTURE_GUIDE.md) - Deep technical details
- [AWS Terraform](./config/terraform/) - Infrastructure code

## 🐛 Troubleshooting

**Lambda not running?**
- Check S3 event notifications in AWS console
- Verify Lambda has S3 & Kinesis permissions

**Data not appearing in Databricks?**
- Ensure tables are created correctly
- Check Autoloader configuration
- Verify Databricks cluster is running

**S3 upload failing?**
- `aws configure` with correct credentials
- Verify bucket name is correct
- Check IAM permissions

---

**Questions?** See [ARCHITECTURE_GUIDE.md](./ARCHITECTURE_GUIDE.md) for detailed explanations.
