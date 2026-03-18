# Databricks Asset Bundle (DAB) Setup

This project uses **Databricks Asset Bundle** to manage and deploy ETL jobs for the Lambda Architecture.

## Directory Structure

```
databricks.yml              # Main DAB configuration
resources/
├── jobs/
│   ├── batch.yml          # Batch layer job definitions
│   └── speed.yml          # Speed layer job definitions
└── clusters/
    └── clusters.yml       # Cluster configurations
```

## Prerequisites

1. Install **Databricks CLI** (v0.214.0 or higher)
   ```bash
   pip install databricks-cli
   ```

2. Set up Databricks credentials
   ```bash
   databricks configure --host https://your-workspace.cloud.databricks.com --token your-token
   ```

3. Set environment variables
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-token"
   export BATCH_S3_BUCKET="your-batch-bucket"
   export KINESIS_STREAM_NAME="lambda-arch-events"
   export INSTANCE_POOL_ID="your-instance-pool-id"  # Optional
   ```

## Deploy

### Validate Configuration
```bash
databricks bundle validate
```

### Deploy to Workspace
```bash
databricks bundle deploy
```

### Run Batch Job
```bash
databricks bundle run batch_etl
```

### Run Speed Job
```bash
databricks bundle run speed_etl
```

## Configuration Files

### `databricks.yml`
Main DAB configuration file defining environments and overall structure.

### `resources/jobs/batch.yml`
Batch ETL job with 3 stages:
1. **Ingest Bronze** - Load from S3 using Autoloader
2. **Transform Silver** - Clean and deduplicate
3. **Aggregate Gold** - Create analytics tables

Schedule: Daily at 2 AM UTC

### `resources/jobs/speed.yml`
Speed ETL job with 3 continuous stages:
1. **Ingest Bronze** - Stream from Kinesis
2. **Transform Silver** - Validate and transform
3. **Aggregate Gold** - Windowed aggregations

### `resources/clusters/clusters.yml`
Cluster configurations for batch and speed processing.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Databricks workspace URL |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `BATCH_S3_BUCKET` | S3 bucket for batch data |
| `KINESIS_STREAM_NAME` | Kinesis stream name |
| `INSTANCE_POOL_ID` | (Optional) Instance pool ID |

## DAB Commands

```bash
# Validate bundle
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy

# Run job
databricks bundle run batch_etl

# Show job details
databricks bundle describe batch_etl

# Destroy resources
databricks bundle destroy
```

## Job Monitoring

After deployment, monitor jobs in Databricks workspace:
1. Go to **Jobs** section
2. Find "Lambda Architecture - Batch ETL" or "Lambda Architecture - Speed ETL"
3. View runs, logs, and metrics

## Troubleshooting

**Error: "Package not found"**
- Ensure Python package is properly built and installed
- Check `setup.py` configuration

**Error: "Cluster not available"**
- Verify instance pool exists or remove `instance_pool_id` to use on-demand pricing

**Error: "S3 bucket not accessible"**
- Check S3 bucket name in environment variable
- Verify Databricks cluster has S3 permissions

For more info: [Databricks Asset Bundle Docs](https://docs.databricks.com/en/dev-tools/bundles/index.html)
