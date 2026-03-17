# ⚡ Quick Start (5 Minutes)

Get the entire system running in 5 minutes.

## Prerequisites (1 min)

✅ Have these ready:
- AWS account credentials (`aws configure`)
- Databricks workspace URL & PAT token
- Python 3.9+ installed
- Terraform 1.0+ installed

## Step 1: Clone & Configure (1 min)

```bash
git clone <repo> && cd databricks-demo

cd terraform
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars
# Set: aws_region, databricks_host, databricks_token
```

## Step 2: Deploy Infrastructure (2 min)

```bash
terraform init
terraform apply

# Save outputs for next steps
terraform output > outputs.txt
```

**What gets created:**
- ✅ S3 buckets (batch & streaming data)
- ✅ Kinesis stream (events-stream)
- ✅ Lambda function (CSV → Kinesis processor)
- ✅ IAM roles & policies

## Step 3: Upload Data to S3 (1 min)

```bash
# Get bucket names
export BATCH_BUCKET=$(terraform output -raw batch_data_bucket)
export STREAMING_BUCKET=$(terraform output -raw streaming_data_bucket)

# Upload batch data (one-time) → Autoloader ingests
python ../scripts/upload_to_s3.py ../data/batch/item_properties_part2.csv \
  --bucket $BATCH_BUCKET

# Upload streaming (triggers Lambda) → Kinesis → Databricks
python ../scripts/upload_to_s3.py ../data/streaming/events.csv \
  --bucket $STREAMING_BUCKET
```

## Step 4: Initialize Databricks Tables (1 min)

In your Databricks workspace, create a new notebook and run:

```python
import sys
sys.path.append('/Workspace/src')

from batch_layer.config import INIT_SQL as B
from speed_layer.config import INIT_SQL as S

spark.sql(B); spark.sql(S)
print("✅ Ready to go!")
```

---

## ✅ You're Done!

Check data in Databricks:
```sql
SELECT COUNT(*) FROM item_properties_bronze;  -- Batch data
SELECT COUNT(*) FROM events_bronze;            -- Events from Kinesis
```

## Next Steps

1. **Deploy Jobs** in Databricks:
   - Use `src/batch_layer/orchestration.py`
   - Use `src/speed_layer/orchestration.py`

2. **Monitor** in CloudWatch:
   - Lambda invocations
   - Kinesis put-record rate
   - S3 upload activity

3. **Review docs**:
   - [README.md](../README.md) - Full project info
   - [ARCHITECTURE_GUIDE.md](../ARCHITECTURE_GUIDE.md) - Detailed architecture

## Troubleshooting

**Lambda not working?**
- Check S3 event notifications
- Verify Lambda permissions in AWS console

**Data not in Databricks?**
- Ensure Kinesis stream exists
- Check cluster is running

**Autoloader not picking up files?**
- Verify S3 path: `s3://bucket/data/batch/YYYY/MM/DD/...`
- Check table schema matches CSV

---

**Ready?** Let's go! 🚀
