# Batch & Streaming Data Upload to S3

**Purpose:** Universal script to upload both batch and streaming data to S3

## Files

- `upload_to_s3.py` - Unified uploader for batch and streaming data

## How to use

### **Option 1: Batch Data (Item Properties)**

```bash
python scripts/upload_to_s3.py data/batch/item_properties_part2.csv \
  --bucket <batch-bucket-name> \
  --type batch
```

**Flow:**
- File uploaded to: `s3://<bucket>/data/batch/YYYY/MM/DD/item_properties_part2.csv`
- Databricks Autoloader detects and ingests automatically
- Bronze → Silver → Gold tables populated

### **Option 2: Streaming Data (Events)**

```bash
python scripts/upload_to_s3.py data/streaming/events.csv \
  --bucket <streaming-bucket-name> \
  --type streaming
```

**Flow:**
- File uploaded to: `s3://<bucket>/data/streaming/YYYY/MM/DD/events.csv`
- Lambda function `s3_streaming_processor` triggered automatically
- Lambda reads CSV, processes each record, puts to Kinesis
- Databricks Structured Streaming ingests from Kinesis
- Bronze → Silver → Gold tables populated

### **Option 3: Auto-detect (Recommended)**

```bash
# Auto-detects from file path
python scripts/upload_to_s3.py data/batch/item_properties_part2.csv --bucket <bucket>
python scripts/upload_to_s3.py data/streaming/events.csv --bucket <bucket>
```

## Parameters

- `file_path` - Path to CSV file
- `--bucket` (required) - S3 bucket name
- `--type` - `batch`, `streaming`, or `auto` (default: auto)
- `--prefix` - S3 prefix path (default: data)

## Notes

- **Get bucket names:** `terraform output batch_data_bucket` and `terraform output streaming_data_bucket`
- **Batch upload:** One-time or scheduled operation
- **Streaming upload:** Triggers Lambda automatically to process events to Kinesis
- **Partitioning:** Files organized by upload date (YYYY/MM/DD)
