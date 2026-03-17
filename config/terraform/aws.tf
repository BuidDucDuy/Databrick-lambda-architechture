# ============================================================================
# AWS Infrastructure for Databricks Lambda Architecture
# Data Ingestion: Kinesis for Streaming + S3 for Batch
# ============================================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ============================================================================
# S3 Buckets for Batch Layer
# ============================================================================

resource "aws_s3_bucket" "batch_data" {
  bucket = "${var.project_name}-batch-data-${data.aws_caller_identity.current.account_id}"

  tags = {
    Purpose = "Item properties batch uploads for Databricks"
    Layer   = "Batch"
  }
}

resource "aws_s3_bucket_versioning" "batch_data" {
  bucket = aws_s3_bucket.batch_data.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "batch_data" {
  bucket = aws_s3_bucket.batch_data.id

  rule {
    id     = "archive-old-batches"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# ============================================================================
# S3 Bucket for Speed Layer / Checkpoints
# ============================================================================

resource "aws_s3_bucket" "streaming_data" {
  bucket = "${var.project_name}-streaming-data-${data.aws_caller_identity.current.account_id}"

  tags = {
    Purpose = "Streaming events and checkpoints"
    Layer   = "Speed"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "streaming_data" {
  bucket = aws_s3_bucket.streaming_data.id

  rule {
    id     = "cleanup-old-checkpoints"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    expiration {
      days = 30
    }
  }
}

# ============================================================================
# Kinesis Stream for Real-time Events
# ============================================================================

resource "aws_kinesis_stream" "events" {
  name             = "${var.project_name}-events-stream"
  retention_period = 24  # 24 hours
  
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  tags = {
    Purpose = "Real-time events ingestion"
    Source  = "API Lambda"
  }
}

# ============================================================================
# IAM Role for Lambda Functions
# ============================================================================

resource "aws_iam_role" "lambda_execution" {
  name = "${var.project_name}-lambda-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Principal"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# ============================================================================
# Lambda Execution Policy - Kinesis Events
# ============================================================================

resource "aws_iam_role_policy" "lambda_kinesis" {
  name = "${var.project_name}-lambda-kinesis-policy"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListStreams"
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

# ============================================================================
# Lambda Layer - Dependencies
# ============================================================================

data "aws_lambda_layer_version" "python_deps" {
  layer_name = "python-dependencies"
  compatible_runtimes = ["python3.11"]
}

# ============================================================================
# Lambda Function - Kinesis Events Ingest
# ============================================================================

resource "aws_lambda_function" "kinesis_ingest" {
  filename      = "${path.module}/../lambda/kinesis_ingest.py.zip"
  function_name = "${var.project_name}-kinesis-ingest"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "kinesis_ingest.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60

  environment {
    variables = {
      KINESIS_STREAM = aws_kinesis_stream.events.name
    }
  }

  depends_on = [aws_iam_role_policy.lambda_kinesis]

  tags = {
    Purpose = "Ingest events into Kinesis stream"
    Type    = "API Handler"
  }
}

# ============================================================================
# API Gateway - HTTP API for Events Ingestion
# ============================================================================

resource "aws_apigatewayv2_api" "events_api" {
  name          = "${var.project_name}-events-api"
  protocol_type = "HTTP"
  
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["POST", "GET", "OPTIONS"]
    allow_headers = ["Content-Type", "Authorization"]
    max_age       = 300
  }

  tags = {
    Purpose = "Events ingestion API"
  }
}

# ============================================================================
# API Gateway Integration - Kinesis
# ============================================================================

resource "aws_apigatewayv2_integration" "kinesis_integration" {
  api_id             = aws_apigatewayv2_api.events_api.id
  integration_type   = "AWS_PROXY"
  integration_method = "POST"
  payload_format_version = "2.0"
  
  target = aws_lambda_function.kinesis_ingest.arn
}

resource "aws_apigatewayv2_route" "events_ingest" {
  api_id    = aws_apigatewayv2_api.events_api.id
  route_key = "POST /ingest"
  
  target = "integrations/${aws_apigatewayv2_integration.kinesis_integration.id}"
}

# ============================================================================
# API Gateway Stage
# ============================================================================

resource "aws_apigatewayv2_stage" "events_api_prod" {
  api_id      = aws_apigatewayv2_api.events_api.id
  name        = "prod"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_logs.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      ip             = "$context.identity.sourceIp"
      requestTime    = "$context.requestTime"
      httpMethod     = "$context.httpMethod"
      resourcePath   = "$context.resourcePath"
      status         = "$context.status"
      protocol       = "$context.protocol"
      responseLength = "$context.responseLength"
      integrationLatency = "$context.integration.latency"
      error          = "$context.error.message"
    })
  }
}

# ============================================================================
# CloudWatch Log Group for API Gateway
# ============================================================================

resource "aws_cloudwatch_log_group" "api_logs" {
  name              = "/aws/apigateway/${var.project_name}-events-api"
  retention_in_days = 7

  tags = {
    Purpose = "API Gateway logs"
  }
}

# ============================================================================
# Lambda Permissions
# ============================================================================

resource "aws_lambda_permission" "api_kinesis_invoke" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.kinesis_ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.events_api.execution_arn}/*"
}

# ============================================================================
# IAM Role for Databricks EC2 Instances (for S3 access)
# ============================================================================

resource "aws_iam_role" "databricks_ec2_role" {
  name = "${var.project_name}-databricks-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Principal"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "databricks_s3_access" {
  name = "${var.project_name}-databricks-s3-policy"
  role = aws_iam_role.databricks_ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.batch_data.arn,
          "${aws_s3_bucket.batch_data.arn}/*",
          aws_s3_bucket.streaming_data.arn,
          "${aws_s3_bucket.streaming_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListStreams",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.events.arn
      }
    ]
  })
}

resource "aws_iam_instance_profile" "databricks_profile" {
  name = "${var.project_name}-databricks-profile"
  role = aws_iam_role.databricks_ec2_role.name
}
  timeout       = var.lambda_timeout

  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.streaming_data.bucket
    }
  }

  depends_on = [data.archive_file.lambda_zip]
}


# ============================================================================
# API Gateway V2 (HTTP API)
# ============================================================================

resource "aws_apigatewayv2_api" "streaming_api" {
  name          = "${var.project_name}-streaming-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_credentials = false
    allow_headers     = ["content-type", "authorization"]
    allow_methods     = ["POST", "OPTIONS", "GET"]
    allow_origins     = ["*"]
    max_age           = 300
  }

  tags = {
    Purpose = "Streaming data ingestion API"
  }
}

resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id           = aws_apigatewayv2_api.streaming_api.id
  integration_type = "AWS_PROXY"
  integration_method = "POST"
  payload_format_version = "2.0"
  target = "arn:aws:apigatewayv2:${var.aws_region}:lambda:path/2015-03-31/functions/${aws_lambda_function.streaming_handler.arn}/invocations"
}

resource "aws_apigatewayv2_route" "post_route" {
  api_id    = aws_apigatewayv2_api.streaming_api.id
  route_key = "POST /ingest"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

resource "aws_apigatewayv2_route" "options_route" {
  api_id    = aws_apigatewayv2_api.streaming_api.id
  route_key = "OPTIONS /ingest"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

resource "aws_apigatewayv2_stage" "streaming" {
  api_id      = aws_apigatewayv2_api.streaming_api.id
  name        = var.environment
  auto_deploy = true

  default_route_settings {
    throttle_settings {
      burst_limit = 100
      rate_limit  = 50
    }
  }
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.streaming_handler.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.streaming_api.execution_arn}/*/*"
}

# ============================================================================
# Data Sources
# ============================================================================

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
