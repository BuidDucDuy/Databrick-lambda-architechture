terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}


# AWS Infrastructure - Lambda Architecture
# S3, Kinesis, Lambda 
data "aws_caller_identity" "current" {}


# S3 Bucket - Batch Data (Item Properties)
resource "aws_s3_bucket" "batch_data" {
  bucket = "databricks-batch-data-demo"

  tags = {
    Name        = "${var.project_name}-batch"
    Environment = var.environment
    Layer       = "Batch"
  }
}

resource "aws_s3_bucket_versioning" "batch_data" {
  bucket = aws_s3_bucket.batch_data.id

  versioning_configuration {
    status = "Enabled"
  }
}


# S3 Bucket - Streaming Data (Events)
resource "aws_s3_bucket" "streaming_data" {
  bucket = "databricks-streaming-data-demo"

  tags = {
    Name        = "${var.project_name}-streaming"
    Environment = var.environment
    Layer       = "Speed"
  }
}



# Kinesis Stream - Real-time Events
resource "aws_kinesis_stream" "events" {
  name             = "${var.project_name}-events"
  retention_period = 24

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  tags = {
    Name        = "${var.project_name}-events"
    Environment = var.environment
    Purpose     = "Real-time events ingestion"
  }
}


# IAM Role - Lambda Execution
resource "aws_iam_role" "lambda_execution" {
  name = "${var.project_name}-lambda-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}


# IAM Policy - Lambda (S3 & Kinesis Access)
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.streaming_data.arn,
          "${aws_s3_bucket.streaming_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords"
        ]
        Resource = aws_kinesis_stream.events.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}


# S3 Event Notification - Lambda Permission

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_processor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.streaming_data.arn
}

# ============================================================================
# Lambda Function Package
# ============================================================================
data "archive_file" "lambda_code" {
  type        = "zip"
  source_file = "${path.module}/../../lambda/s3_streaming_processor.py"
  output_path = "${path.module}/lambda_function.zip"
}

# ============================================================================
# Lambda Function - S3 to Kinesis Processor
# ============================================================================
resource "aws_lambda_function" "s3_processor" {
  filename            = data.archive_file.lambda_code.output_path
  function_name       = "${var.project_name}-s3-processor"
  role                = aws_iam_role.lambda_execution.arn
  handler             = "s3_streaming_processor.lambda_handler"
  runtime             = "python3.11"
  timeout             = 60
  memory_size         = 512
  source_code_hash    = data.archive_file.lambda_code.output_base64sha256

  environment {
    variables = {
      KINESIS_STREAM = aws_kinesis_stream.events.name
    }
  }

  depends_on = [aws_iam_role_policy.lambda_policy]

  tags = {
    Name        = "${var.project_name}-s3-processor"
    Environment = var.environment
  }
}

# ============================================================================
# S3 Event Notification - Trigger Lambda on file upload
# ============================================================================
resource "aws_s3_bucket_notification" "streaming_notification" {
  bucket      = aws_s3_bucket.streaming_data.id
  depends_on  = [aws_lambda_permission.allow_s3]

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "data/"
  }
}
