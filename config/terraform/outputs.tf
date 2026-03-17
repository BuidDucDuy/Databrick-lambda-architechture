output "kinesis_stream_name" {
  description = "Kinesis stream name for events"
  value       = aws_kinesis_stream.events.name
}

output "kinesis_stream_arn" {
  description = "Kinesis stream ARN"
  value       = aws_kinesis_stream.events.arn
}

output "batch_data_bucket" {
  description = "S3 bucket for batch data"
  value       = aws_s3_bucket.batch_data.id
}

output "streaming_data_bucket" {
  description = "S3 bucket for streaming data and checkpoints"
  value       = aws_s3_bucket.streaming_data.id
}

output "events_api_endpoint" {
  description = "API Gateway endpoint for events ingestion"
  value       = aws_apigatewayv2_stage.events_api_prod.invoke_url
}

output "kinesis_ingest_lambda_name" {
  description = "Lambda function name for Kinesis ingestion"
  value       = aws_lambda_function.kinesis_ingest.function_name
}

output "databricks_instance_profile_arn" {
  description = "ARN of Databricks EC2 instance profile for S3 and Kinesis access"
  value       = aws_iam_instance_profile.databricks_profile.arn
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
  value       = data.databricks_current_user.me.workspace_id
}

output "databricks_jobs_folder" {
  description = "Databricks folder for ETL jobs"
  value       = databricks_directory.jobs.path
}
