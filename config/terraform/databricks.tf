# ============================================================================
# Databricks Workspace Setup
# ============================================================================

data "databricks_current_user" "me" {}

data "databricks_spark_version" "latest" {
  long_term_support = true
}

# ============================================================================
# Databricks Directory for Jobs
# ============================================================================

resource "databricks_directory" "jobs" {
  path = "/Shared/lambda-architecture-jobs"
}

# ============================================================================
# Databricks Cluster Policy (for cost control)
# ============================================================================

resource "databricks_cluster_policy" "etl_policy" {
  name = "${var.project_name}-etl-policy"
  definition = jsonencode({
    "spark_conf.databricks.cluster.profile" : {
      "type" : "fixed",
      "value" : "singleNode"
    },
    "node_type_id" : {
      "type" : "fixed",
      "value" : "i3.xlarge"
    },
    "num_workers" : {
      "type" : "fixed",
      "value" : 0
    }
  })
}

# ============================================================================
# Databricks Job for Batch ETL (Upload from S3)
# ============================================================================

resource "databricks_job" "batch_etl" {
  name = "${var.project_name}-batch-etl-job"

  task {
    task_key = "ingest-and-process"

    notebook_task {
      notebook_path = "${databricks_directory.jobs.path}/batch_etl"
      base_parameters = {
        s3_bucket        = aws_s3_bucket.batch_uploads.bucket
        s3_region        = var.aws_region
        output_table     = "default.batch_processed"
      }
    }

    existing_cluster_id = databricks_cluster.etl_cluster.cluster_id
  }

  max_concurrent_runs = 1
}

# ============================================================================
# Databricks Cluster for ETL Jobs
# ============================================================================

resource "databricks_cluster" "etl_cluster" {
  cluster_name            = "${var.project_name}-etl-cluster"
  spark_version           = data.databricks_spark_version.latest.id
  node_type_id            = "i3.xlarge"
  num_workers             = 0  # Single node cluster for cost optimization
  instance_profile_arn    = databricks_instance_profile.etl_profile.arn
  autotermination_minutes = 20
  
  spark_conf = {
    "spark.databricks.cluster.profile" : "singleNode"
  }

  tags = {
    Purpose = "ETL processing for batch and streaming data"
  }
}

# ============================================================================
# Databricks Instance Profile (for AWS access)
# ============================================================================

resource "databricks_instance_profile" "etl_profile" {
  instance_profile_arn = aws_iam_instance_profile.etl_profile.arn
}

resource "aws_iam_role" "etl_role" {
  name = "${var.project_name}-databricks-etl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "414351767826" # Databricks account ID for AWS integration
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "s3_access" {
  name = "${var.project_name}-s3-access"
  role = aws_iam_role.etl_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ]
        Resource = [
          aws_s3_bucket.batch_uploads.arn,
          "${aws_s3_bucket.batch_uploads.arn}/*",
          aws_s3_bucket.streaming_data.arn,
          "${aws_s3_bucket.streaming_data.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "etl_profile" {
  name = "${var.project_name}-etl-profile"
  role = aws_iam_role.etl_role.name
}

# ============================================================================
# Databricks Workspace Variable (connection string for S3)
# ============================================================================

resource "databricks_workspace_variable" "s3_bucket_batch" {
  key          = "S3_BATCH_BUCKET"
  value        = aws_s3_bucket.batch_uploads.bucket
  description  = "S3 bucket for batch uploads"
}

resource "databricks_workspace_variable" "s3_bucket_streaming" {
  key          = "S3_STREAMING_BUCKET"
  value        = aws_s3_bucket.streaming_data.bucket
  description  = "S3 bucket for streaming data"
}
